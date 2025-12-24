package com.smee.binlog.collector;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.otter.canal.connector.core.consumer.CommonMessage;
import com.alibaba.otter.canal.connector.core.util.MessageUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.sink.AbstractCanalEventSink;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.model.SqlWithPos;
import com.utils.Utils; // 推测的工具类引用
import lombok.SneakyThrows;
import lombok.var;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

@Component
public class DefaultCanalEventSink extends AbstractCanalEventSink<List<Entry>> {
    private final static Logger LOG = LoggerFactory.getLogger(DefaultCanalEventSink.class);
    private final ApplicationConfig config;
    private final DefaultBinlogPositionManager positionManager;
    private final CanalKafkaProducer producer;
    private final ConcurrentLinkedQueue<Tuple2<List<Entry>, EntryPosition>> eventQueue = new ConcurrentLinkedQueue<>();
    private BinlogWatermark consumedBinlogWatermark;
    private final SinkBinlogEntry sinkEntry = new SinkBinlogEntry();
    private boolean canSend = false;

    private long curSizeBytes = 2;
    private EntryPosition lastAddedPosition = null;
    private static final int SQL_COUNT_THRESHOLD = 512;
    private EntryPosition lastSentPosition = null;
    private static final long LINGER_MS = 200;
    private volatile long lastAppendAtMs = System.currentTimeMillis();

    public DefaultCanalEventSink(ApplicationConfig config, DefaultBinlogPositionManager positionManager) {
        this.config = config;
        this.producer = new CanalKafkaProducer(config.getKafka());
        this.positionManager = positionManager;
        this.sinkEntry.setSqlList(new ArrayList<>());
        resetBinlogWatermark();
    }

    @Override
    public boolean sink(List<Entry> events, InetSocketAddress remoteAddress, String destination) throws CanalSinkException {
        int curTotalEvents = events.size();
        int idx = 0;
        final int REDUCE_BATCH_TRIGGER = 512;
        final List<Entry> bucket = new ArrayList<>(REDUCE_BATCH_TRIGGER);
        String currentFile = null;

        events = Mono.just(events)
                .map(this::filterHeartBeat)
                .map(this::filterTransaction)
                .block();

        var eventsSize = events.size();
        if (eventsSize == 0) {
            return true;
        }

        synchronized (this) {
            for (Entry e : events) {
                String file = e.getHeader().getLogfileName();
                if (currentFile == null) {
                    currentFile = file;
                }
                if (!file.equals(currentFile)) {
                    if (!bucket.isEmpty()) {
                        handleBucketEvents(bucket);
                    }
                    currentFile = file;
                }

                double t1 = System.nanoTime();
                var currBW = BinlogWatermark.parse(String.format("%s|%s", e.getHeader().getLogfileName(), e.getHeader().getLogfileOffset()));
                double t2 = System.nanoTime();
                PerfMonitorContext.setParseCostTime(PerfMonitorContext.getParseCostTime() + (t2 - t1) / 1_000_000.0);

                if (!validateBinlogWatermark(consumedBinlogWatermark, currBW)) {
                    continue;
                }
                bucket.add(e);

                if (bucket.size() >= REDUCE_BATCH_TRIGGER || idx == curTotalEvents - 1) {
                    handleBucketEvents(bucket);
                }
                idx++;
            }
            if (!bucket.isEmpty()) {
                handleBucketEvents(bucket);
            }
        }
        return true;
    }

    private void handleBucketEvents(List<Entry> bucket) {
        Entry last = bucket.get(bucket.size() - 1);
        var h = last.getHeader();
        EntryPosition lasPos = new EntryPosition(h.getLogfileName(), h.getLogfileOffset());
        reduceEvents(new ArrayList<>(bucket), lasPos);
        bucket.clear();
    }

    private void resetBinlogWatermark() {
        var pos = positionManager.getLatestIndexByInDb(config.getEquipment());
        if (pos == null) {
            var bw = new BinlogWatermark();
            bw.setFileId(0);
            bw.setPosition(0);
            consumedBinlogWatermark = bw;
        } else {
            consumedBinlogWatermark = position2BinlogWatermark(pos.getPosition());
        }
    }

    private BinlogWatermark position2BinlogWatermark(EntryPosition pos) {
        return BinlogWatermark.parse(String.format("%s|%s", pos.getJournalName(), pos.getPosition()));
    }

    @Override
    public void interrupt() {
        resetBinlogWatermark();
    }

    private boolean validateBinlogWatermark(BinlogWatermark target, BinlogWatermark current) {
        if (current.getFileId() == target.getFileId()) {
            return current.getPosition() > target.getPosition();
        }
        return current.getFileId() > target.getFileId();
    }

    private List<Entry> filterHeartBeat(List<Entry> events) {
        return events.stream()
                .filter(e -> !e.getEntryType().equals(EntryType.HEARTBEAT))
                .collect(Collectors.toList());
    }

    private List<Entry> filterTransaction(List<Entry> events) {
        return events.stream()
                .filter(e -> !e.getEntryType().equals(EntryType.TRANSACTIONBEGIN)
                        && !e.getEntryType().equals(EntryType.TRANSACTIONEND))
                .collect(Collectors.toList());
    }

    @SneakyThrows
    private void reduceEvents(List<Entry> events, EntryPosition lastPosition) {
        if (events.size() > 0) {
            synchronized (eventQueue) {
                eventQueue.offer(Tuples.of(events, lastPosition));
            }
        }

        var queueLength = eventQueue.size();
        if (queueLength >= config.getBinlogPositionPersistStep()) {
            EntryPosition pos = null;
            while (queueLength-- > 0) {
                var t = eventQueue.poll();
                if (t == null) {
                    break;
                }
                var e = t.getT1().stream()
                        .filter(entry -> {
                            var type = entry.getEntryType();
                            return type != EntryType.TRANSACTIONBEGIN && type != EntryType.TRANSACTIONEND;
                        })
                        .collect(Collectors.toList());
                pos = t.getT2();

                if (e.size() > 0) { // 这里开始批量转化为sql语句的时间
                    double startConvert = System.nanoTime();
                    convertCanalEntry(e, pos);
                    double endConvert = System.nanoTime();
                    double costConvert = (endConvert - startConvert) / 1_000_000.0;
                    PerfMonitorContext.setConvertCostTime(PerfMonitorContext.getConvertCostTime() + costConvert);
                    flushIfNeeded(false);
                }
            }
        }
        flushByLingerIfNeeded(); // 超过200ms没填满512条sql直接发送
    }

    private void resetSinkEntry() {
        if (sinkEntry.getSqlList() == null) {
            sinkEntry.setSqlList(new ArrayList<>());
        } else {
            sinkEntry.getSqlList().clear();
        }
        sinkEntry.setHeader(null);
        curSizeBytes = 2;
        lastAddedPosition = null;
        canSend = false;
    }

    private void flushIfNeeded(boolean force) {
        if (config.isDryRun()) {
            if (force) {
                resetSinkEntry();
            }
            return;
        }

        if (sinkEntry.getSqlList() == null || sinkEntry.getSqlList().isEmpty()) {
            canSend = false;
            return;
        }

        if (!force && !canSend) {
            return;
        }

        if (sinkEntry.getHeader() == null) {
            sinkEntry.setHeader(lastAddedPosition);
        }

        SinkBinlogEntry snapshot = new SinkBinlogEntry();
        snapshot.setHeader(sinkEntry.getHeader());
        snapshot.setSqlList(new ArrayList<>(sinkEntry.getSqlList()));
        double startSend = System.nanoTime();

        LOG.info("flushIfNeeded: force={}, canSend={}, sendSqlCount={}, header={}",
                force, canSend, snapshot.getSqlList().size(),
                snapshot.getHeader() == null ? "null" : Utils.EntryPosition2String(snapshot.getHeader()));

        try {
            producer.send(snapshot);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        lastSentPosition = snapshot.getHeader();
        double endSend = System.nanoTime();
        double costSend = (endSend - startSend) / 1_000_000.0;
        PerfMonitorContext.setSendCostTime(PerfMonitorContext.getSendCostTime() + costSend);

        if (lastSentPosition != null) {
            positionManager.persistLogPositionReal(config.getEquipment(), lastSentPosition);
            consumedBinlogWatermark = position2BinlogWatermark(lastSentPosition);
            LOG.info("当前批次成功发送的末尾sql位点：" + Utils.EntryPosition2String(lastSentPosition));
            recordTimeLog(lastSentPosition);
        }
        resetSinkEntry();
    }

    private void recordTimeLog(EntryPosition pos) {
        double total = PerfMonitorContext.getTotalCostTime();

        // 百分比计算
        double parseRatio = total > 0 ? (PerfMonitorContext.getParseCostTime() * 100.0 / total) : 0;
        double convertRatio = total > 0 ? (PerfMonitorContext.getConvertCostTime() * 100.0 / total) : 0;
        double sendRatio = total > 0 ? (PerfMonitorContext.getSendCostTime() * 100.0 / total) : 0;

        LOG.info("====================================================================");
        LOG.info("* 文件[{}]处理明细:", PerfMonitorContext.lastFileName);
        LOG.info("* 开始时间: {}", new java.util.Date(PerfMonitorContext.getBeginWallTime()));
        LOG.info("* 结束时间: {}", new java.util.Date(PerfMonitorContext.getEndWallTime()));
        LOG.info("* 三阶段总耗时: {} ms", total);
        LOG.info("--------------------------------------------------------------------");
        LOG.info("* 文件[{}]各个阶段耗时明细:", PerfMonitorContext.lastFileName);
        LOG.info("* 解析阶段(parseCostTime):   {} ms   占比{}%", PerfMonitorContext.getParseCostTime(), parseRatio);
        LOG.info("* 转换阶段(convertCostTime): {} ms   占比{}%", PerfMonitorContext.getConvertCostTime(), convertRatio);
        LOG.info("* 发送阶段(sendCostTime):    {} ms   占比{}%", PerfMonitorContext.getSendCostTime(), sendRatio);
        LOG.info("--------------------------------------------------------------------");
        LOG.info("远程耗时占比验证: {}%", parseRatio + convertRatio + sendRatio);
        LOG.info("====================================================================");
        PerfMonitorContext.reset();
        PerfMonitorContext.lastFileName = currentFile;
    }

    private void convertCanalEntry(List<Entry> events, EntryPosition position) {
        long sendSizeThresholdBytes = config.getKafka().getSendSizeThresholdBytes();
        List<CommonMessage> commonMessageList = MessageUtil.convert(new Message(-1, events));
        for (CommonMessage commonMessage : commonMessageList) {
            EntryPosition curLastPosition = commonMessage.getEntryPosition();
            List<SingleDml> singleDmls = message2SingleDmls(commonMessage);
            for (SingleDml singleDml : singleDmls) {
                var type = singleDml.getType();
                if (type == null) {
                    throw new RuntimeException("Cannot resolve binlog type: NULL.");
                }

                var enumType = CanalEntry.EventType.valueOf(type.toUpperCase());
                String sql;
                try {
                    switch (enumType) {
                        case INSERT:
                            sql = insert(singleDml);
                            break;
                        case UPDATE:
                            sql = update(singleDml);
                            break;
                        case DELETE:
                            sql = delete(singleDml);
                            break;
                        case ALTER:
                        case CREATE:
                        case QUERY:
                        case ERASE:
                        case CINDEX:
                        case DINDEX:
                        case TRUNCATE:
                        case RENAME:
                            sql = pass(singleDml);
                            break;
                        default:
                            throw new RuntimeException(String.format("Cannot resolve binlog type: %s.", type));
                    }

                    if (sql == null) {
                        continue;
                    }

                    SqlWithPos sqlWithPos = new SqlWithPos();
                    sqlWithPos.setPosition(singleDml.getPosition());
                    sqlWithPos.setSql(sql);
                    byte[] bytes = JSON.toJSONBytes(sqlWithPos, JSONWriter.Feature.LargeObject);
                    long length = bytes.length;

                    boolean sizeOk = (curSizeBytes + length + 1 < sendSizeThresholdBytes);
                    boolean countOk = (sinkEntry.getSqlList().size() < SQL_COUNT_THRESHOLD);

                    if (sizeOk && countOk) { // 如果当前包的sql可以加入缓冲中
                        sinkEntry.getSqlList().add(sqlWithPos);
                        curSizeBytes += length + 1;
                        lastAddedPosition = curLastPosition;
                        sinkEntry.setHeader(lastAddedPosition);
                        lastAppendAtMs = System.currentTimeMillis();

                        if (sinkEntry.getSqlList().size() >= SQL_COUNT_THRESHOLD) { // 加入sql之前如果达到阈值
                            LOG.info("消息数量达到当前上限: {}, 进行发送", SQL_COUNT_THRESHOLD);
                            canSend = true;
                            flushIfNeeded(false);
                        }
                    } else { //表示当前这条sql不可加入消息中，容量不够，将之前的消息进行发送
                        if (!sinkEntry.getSqlList().isEmpty()) {
                            sinkEntry.setHeader(lastAddedPosition != null ? lastAddedPosition : curLastPosition);
                            canSend = true;
                            flushIfNeeded(false);
                        }

                        sizeOk = (curSizeBytes + length + 1 < sendSizeThresholdBytes);
                        countOk = (sinkEntry.getSqlList().size() < SQL_COUNT_THRESHOLD);

                        if (sizeOk && countOk) {
                            sinkEntry.getSqlList().add(sqlWithPos);
                            curSizeBytes += length + 1;
                            lastAddedPosition = curLastPosition;
                            sinkEntry.setHeader(lastAddedPosition);
                            lastAppendAtMs = System.currentTimeMillis();
                        } else {
                            long singleSize = 2 + length + 1;
                            LOG.error("单条SQL序列化大小={} 已超过阈值={}，无法发送，需特殊处理 pos={}",
                                    singleSize, sendSizeThresholdBytes, Utils.EntryPosition2String(curLastPosition));
                        }
                    }

                } catch (RuntimeException e) {
                    if (!sinkEntry.getSqlList().isEmpty()) {
                        sinkEntry.setHeader(lastAddedPosition != null ? lastAddedPosition : curLastPosition);
                        LOG.info("出现异常，提前发送！");
                        canSend = true;
                        flushIfNeeded(false);
                    }
                    LOG.info("sql解析异常, singleDml: {}, curLastPosition: {}, exception: {}", singleDml, curLastPosition, e.getMessage());
                    continue;
                }

                // 再次检查header
                if (sinkEntry.getHeader() == null) {
                    if (sinkEntry.getSqlList() != null && !sinkEntry.getSqlList().isEmpty() && lastAddedPosition != null) {
                        sinkEntry.setHeader(lastAddedPosition);
                    } else {
                        sinkEntry.setHeader(position);
                    }
                }
            }
        }
    }

    private void flushByLingerIfNeeded() {
        if (sinkEntry.getHeader() == null || sinkEntry.getSqlList().isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastAppendAtMs > LINGER_MS) {
            flushIfNeeded(true);
        }
    }

    private static List<SingleDml> message2SingleDmls(CommonMessage dml) {
        // MySQL 5.7
        if (dml.getData() != null) {
            int size = dml.getData().size();
            var singleDmls = new ArrayList<SingleDml>(size);

            for (int i = 0; i < size; i++) {
                var singleDml = new SingleDml();
                singleDml.setDatabase(dml.getDatabase());
                singleDml.setTable(dml.getTable());
                singleDml.setType(dml.getType());
                singleDml.setPkNames(dml.getPkNames());
                singleDml.setSql(dml.getSql());
                singleDml.setPosition(dml.getEntryPosition());
                Map<String, Object> data = dml.getData().get(i);
                singleDml.setData(data);
                if (dml.getOld() != null) {
                    Map<String, Object> oldData = dml.getOld().get(i);
                    singleDml.setOld(oldData);
                }
                singleDmls.add(singleDml);
            }
            return singleDmls;
        }
        // MySQL 5.1
        else {
            var singleDml = new SingleDml() {{
                setDatabase(dml.getDatabase());
                setTable(dml.getTable());
                setType(dml.getType());
                setPkNames(dml.getPkNames());
                setSql(dml.getSql());
                setPosition(dml.getEntryPosition());
            }};
            return Collections.singletonList(singleDml);
        }
    }

    private String insert(SingleDml dml) {
        var data = dml.getData();
        if (data == null || data.isEmpty()) {
            // MySQL 5.1
            return dml.getSql();
        }

        var columnSet = data.keySet();
        var sql = new StringBuilder();
        sql.append("INSERT INTO ")
                .append(dml.getTable())
                .append(" (");

        columnSet.forEach(k -> sql.append(k).append(","));

        int len = sql.length();
        sql.delete(len - 1, len).append(") VALUES (");

        for (var columnKey : columnSet) {
            var v = getColumnValue(data.get(columnKey));
            sql.append(String.format("%s,", v));
        }
        len = sql.length();
        sql.delete(len - 1, len).append(")");

        return sql.toString();
    }

    private String update(SingleDml dml) {
        var data = dml.getData();
        var old = dml.getOld();
        if (data == null || data.isEmpty()) {
            // MySQL 5.1
            return dml.getSql();
        }
        if (old == null || old.isEmpty()) {
            return null;
        }

        var sql = new StringBuilder();
        sql.append("UPDATE ")
                .append(dml.getTable())
                .append(" SET ");

        for (String srcColumnName : old.keySet()) {
            var v = getColumnValue(data.get(srcColumnName));
            sql.append(srcColumnName).append("=").append(String.format("%s, ", v));
        }
        int len = sql.length();
        sql.delete(len - 2, len);
        appendCondition(sql, dml.getPkNames(), data);

        return sql.toString();
    }

    private String delete(SingleDml dml) {
        var data = dml.getData();
        if (data == null || data.isEmpty()) {
            // MySQL 5.1
            return dml.getSql();
        }

        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ")
                .append(dml.getTable());

        appendCondition(sql, dml.getPkNames(), data);

        return sql.toString();
    }

    private String pass(SingleDml msg) {
        return msg.getSql();
    }

    private void appendCondition(StringBuilder sql, List<String> pkNames, Map<String, Object> data) {
        sql.append(" WHERE ");

        Iterable<String> columns;
        if (pkNames.isEmpty()) {
            columns = data.keySet();
        } else {
            columns = pkNames;
        }

        for (var column : columns) {
            var v = getColumnValue(data.get(column));
            sql.append(String.format("%s=%s AND ", column, v));
        }
        int len = sql.length();
        sql.delete(len - 5, len); // 图片中是 len-4 还是 len-5，通常 " AND " 是5个字符
    }

    @SneakyThrows
    private String getColumnValue(Object source) {
        if (source == null) {
            return "NULL";
        }
        if (source instanceof String) {
            return String.format("'%s'", escapeStringForMySQL((String) source));
        }
        if (source instanceof Integer
                || source instanceof BigInteger
                || source instanceof Long
                || source instanceof Byte
                || source instanceof Double) {
            return source.toString();
        }
        if (source instanceof byte[]) {
            return Flux.fromArray(ArrayUtils.toObject((byte[]) source))
                    .reduce(new StringBuilder("x'"), (sb, it) -> {
                        sb.append(String.format("%02x", it).toUpperCase());
                        return sb;
                    })
                    .block().append("'")
                    .toString();
        }
        if (source instanceof Timestamp) {
            return String.format("'%s'", source.toString());
        }

        throw new Exception(String.format("Cannot convert %s to SQL.", source.getClass().getSimpleName()));
    }

    private String escapeStringForMySQL(String s) {
        return s.replace("\b", "\\b")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t")
                .replace("\u001A", "\\Z")
                .replace("\u0000", "\\0")
                .replace("\\", "\\\\")
                .replace("'", "''")
                .replace("\"", "\\\"");
    }
}