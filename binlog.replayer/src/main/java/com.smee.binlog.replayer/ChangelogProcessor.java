package com.smee.binlog.replayer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.model.SqlWithPos;
import com.smee.binlog.replayer.longhorn.CreateSnapshotRequest;
import com.smee.binlog.replayer.longhorn.LonghornClient;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.util.List;
import lombok.SneakyThrows;
import lombok.var;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Component
public class ChangelogProcessor {

    private final Logger LOG = LoggerFactory.getLogger(ChangelogProcessor.class);
    private final ApplicationConfig config;
    private final ApplicationConfig.KafkaConfig kafkaConfig;
    private final String topic;
    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean closeRequested = new AtomicBoolean(false);
    private final SqlExecutor targetDb;
    private final ObjectMapper objectMapper;
    private final PositionManager positionManager;
    private final LonghornClient longhornClient;
    private final Retry longhornRetry;
    private final ChangelogExceptionHandler changelogExceptionHandler;
    private BinlogWatermark prevWatermark;
    private boolean batchMode = false;

    private final AtomicLong updateMetadataDbCounter = new AtomicLong(0);
    private final AtomicLong updateLonghornCounter = new AtomicLong(0);

    public ChangelogProcessor(ApplicationConfig config, PositionManager positionManager, LonghornClient longhornClient, ChangelogExceptionHandler changelogExceptionHandler) {
        this.config = config;
        this.kafkaConfig = config.getKafka();
        topic = config.getKafka().getTopic();
        this.positionManager = positionManager;
        this.longhornClient = longhornClient;
        this.changelogExceptionHandler = changelogExceptionHandler;

        longhornRetry = Retry.of("longhorn", () -> RetryConfig.custom()
                .maxAttempts(config.getMetadata().getLonghorn().getMaxRetryCount())
                .waitDuration(Duration.ofSeconds(5))
                .retryOnException(e -> true)
                .failAfterMaxAttempts(true)
                .build());

        var kafkaProperties = new Properties();
        kafkaProperties.putAll(config.getKafka().getProperties());
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        kafkaProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProperties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.MAX_VALUE);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "binlog-replayer-10001"); //todo group-id
        kafkaProperties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "master");

        consumer = new KafkaConsumer<>(kafkaProperties);
        targetDb = new SqlExecutor(config.getTargetDatabase());
        objectMapper = new ObjectMapper();
    }

    @SneakyThrows
    public void run() {
        positionManager.start();

        prevWatermark = positionManager.getLatestWatermark();

        var conn = targetDb.getDataSource().getConnection();
        var catalog = conn.getCatalog();
        conn.close();
        targetDb.execute(String.format("use %s;", catalog));

        var partition = new TopicPartition(topic, 0);
        consumer.assign(Collections.singletonList(partition));

        var restoreOffset = positionManager.getLatestKafkaOffset();
        if (restoreOffset == null) {
            if (kafkaConfig.getInitialOffset() == null) {
                throw new Exception("kafkaConfig.initialOffset must not be empty.");
            }
            restoreOffset = kafkaConfig.getInitialOffset();
        }
        consumer.seek(partition, restoreOffset);

        try {
            while (!closeRequested.get()) {
                var message = consumer.poll(Duration.ofMillis(kafkaConfig.getPollTimeout()));
                if (message.isEmpty()) {
                    continue;
                }
                var record = StreamSupport.stream(message.records(topic).spliterator(), false)
                        .collect(Collectors.toList())
                        .get(0);
                if (!batchMode) {
                    reduceMessages(record, false); // 启动首消息：保持旧逻辑
                    batchMode = true;
                } else {
                    reduceMessagesBatch(record);   // 后续消息：批量
                }
            }
        } catch (Exception e) {
            if (!closeRequested.get()) {
                throw e;
            }
        }
    }

    public void stop() {
        closeRequested.set(true);
    }

    private Boolean validateBinlogWatermark(BinlogWatermark curr) {
        if (curr.getFileId() == prevWatermark.getFileId()) {
            return curr.getPosition() > prevWatermark.getPosition();
        }
        return curr.getFileId() > prevWatermark.getFileId();
    }

    @SneakyThrows
    private void reduceMessages(ConsumerRecord<String, String> record,boolean forceMeta) {
        LOG.info(String.format("%s offset: %s", record.key(), record.offset()));
        List<SqlWithPos> sqlWithPosList = objectMapper.readValue(record.value(),
                new TypeReference<List<SqlWithPos>>() {
                });
        
        for (SqlWithPos sqlWithPos : sqlWithPosList) {
            var currWatermark = BinlogWatermark.parse(sqlWithPos.getPosition());
            if (!validateBinlogWatermark(currWatermark)) {
                continue;
            }
            try {
                targetDb.execute(sqlWithPos.getSql());
                LOG.info("kafka offset {} position {}", record.offset(), sqlWithPos.getPosition());
                if (forceMeta) {
                    positionManager.savePosition(currWatermark, record.offset()); // 写PG :contentReference[oaicite:5]{index=5}
                    updateMetadataDbCounter.set(0);
                    updateLonghornSnapshot(currWatermark, record.offset());
                } else {
                    updateMetadata(currWatermark, record.offset());        // 旧逻辑(带门控)
                }

                prevWatermark = currWatermark;
            } catch (SQLException e) {
                if (changelogExceptionHandler.handle(sqlWithPos.getSql(), e)) {
                    updateMetadata(currWatermark, Long.valueOf(record.offset()));
                    prevWatermark = currWatermark;
                    continue;
                }
                LOG.error(String.format("Error occurred when executing SQL:\n%s", sqlWithPos.getSql()));
                throw e;
            }
        }
        consumer.commitSync();                               // commit kafka offset :contentReference[oaicite:6]{index=6}
    }
    private void reduceMessagesBatch(ConsumerRecord<String, String> record) throws Exception {
        LOG.info(String.format("[BATCH] %s offset: %s", record.key(), record.offset()));
        List<SqlWithPos> list = objectMapper.readValue(record.value(), new TypeReference<List<SqlWithPos>>() {});

        // 1) 过滤掉 <= prevWatermark 的
        List<SqlWithPos> toExec = new java.util.ArrayList<>();
        for (SqlWithPos s : list) {
            var wm = BinlogWatermark.parse(s.getPosition());
            if (validateBinlogWatermark(wm)) {
                toExec.add(s);
            }
        }
        if (toExec.isEmpty()) return;

        // 2) 批量执行（单事务）
        List<String> sqls = toExec.stream().map(SqlWithPos::getSql).collect(java.util.stream.Collectors.toList());
        try {
            targetDb.executeBatch(sqls);

            // 3) 批量成功：只写最后一个点位（你要的行为）
            SqlWithPos last = toExec.get(toExec.size() - 1);
            BinlogWatermark lastWm = BinlogWatermark.parse(last.getPosition());
            savePositionAndCommit(lastWm, record.offset());
            prevWatermark = lastWm;

            LOG.info("[BATCH] success, offset {} lastPos {}", record.offset(), last.getPosition());
        } catch (SQLException batchEx) {
            LOG.error("[BATCH] failed, will rollback and degrade to single. offset {}", record.offset(), batchEx);

            // 4) 降级单条：强制每条成功都落PG点位；遇到不可处理错误仍会 throw 终止（符合你的方案）
            reduceMessages(record, true);

            // 如果 reduceMessages 全部成功，说明 batch 失败可能是 JDBC batch/驱动层问题；
            // 这时你可以选择继续 batch 或继续单条。按你方案可继续 batch（这里不额外处理）。
        }
    }

    private void savePositionAndCommit(BinlogWatermark currWatermark, long offset) throws Exception {
        positionManager.savePosition(currWatermark, offset); // 写PG :contentReference[oaicite:5]{index=5}
        consumer.commitSync();                               // commit kafka offset :contentReference[oaicite:6]{index=6}
        updateMetadataDbCounter.set(0);                      // 避免门控状态干扰
        updateLonghornSnapshot(currWatermark, offset);
    }


    private void reduceMessages(ConsumerRecord<String, String> record) {
        reduceMessages(record, false);
    }
    @SneakyThrows
    private void updateMetadata(BinlogWatermark currWatermark, long offset) {
        updateMetadataDb(currWatermark, offset);
        updateLonghornSnapshot(currWatermark, offset);
    }

    @SneakyThrows
    private void updateMetadataDb(BinlogWatermark currWatermark, long offset) {
        if (prevWatermark.getFileId() != currWatermark.getFileId()) {
            updateMetadataDbCounter.set(0);
        } else if (updateMetadataDbCounter.addAndGet(1) < config.getMetadata().getUpdateBatch()) {
            return;
        }

        positionManager.savePosition(currWatermark, offset);
        consumer.commitSync();

        updateMetadataDbCounter.set(0);

        // LOG.info("Save current position watermark finished.");
    }

    @SneakyThrows
    private void updateLonghornSnapshot(BinlogWatermark currWatermark, long offset) {
        var longhornConfig = config.getMetadata().getLonghorn();

        if (!longhornConfig.isEnabled()) {
            return;
        }

        if (prevWatermark.getFileId() != currWatermark.getFileId()) {
            updateLonghornCounter.set(0);
        } else if (updateLonghornCounter.addAndGet(1) < longhornConfig.getUpdateBatch()) {
            return;
        }

        Thread.sleep(10 * 1000);

        var snapshotNameOri = String.format("%s-%s", currWatermark.getRaw(), offset);
        // escape snapshot name for longhorn CDR
        var snapshotName = snapshotNameOri.replace('|', '-');

        var body = new CreateSnapshotRequest();
        body.setName(snapshotName);

        var volumeName = config.getMetadata().getLonghorn().getVolume();
        var createSnapshot = Retry.decorateCheckedRunnable(longhornRetry, () -> {
            var snapshotList = longhornClient.snapshotList(volumeName).execute();
            if (snapshotList.body().getData().stream().anyMatch(x -> snapshotName.equals(x.getName()))) {
                return;
            }

            var pvSnapshot = longhornClient.createSnapshot(volumeName, body)
                    .execute();
            if (!pvSnapshot.isSuccessful()) {
                throw new Exception(String.format("Create Longhorn snapshot failed, message: %s", pvSnapshot.message()));
            }
        });

        try {
            createSnapshot.run();
        } catch (Throwable ex) {
            LOG.error(String.format("Create Longhorn snapshot %s failed", snapshotName));
            throw ex;
        }

        updateLonghornCounter.set(0);

        LOG.info(String.format("Created Longhorn snapshot %s", snapshotName));
    }
}
