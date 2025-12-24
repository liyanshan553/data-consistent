package com.smee.binlog.collector;

import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.AbstractMysqlEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.LocalBinlogEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.TableMetaCache;
import lombok.var;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class DefaultLocalBinlogEventParser extends LocalBinlogEventParser {
    private final ApplicationContext context;
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLocalBinlogEventParser.class);

    public DefaultLocalBinlogEventParser(ApplicationContext context) {
        this.context = context;
    }

    @Override
    protected ErosaConnection buildErosaConnection() {
        var connection = context.getBean(LocalBinlogConnection.class);
        
        connection.setBufferSize(this.bufferSize);
        connection.setDirectory(this.directory);
        return connection;
    }

    @Override
    protected void preDump(ErosaConnection connection) {
        if (!PerfMonitorContext.hasRecordedBegin) {
            PerfMonitorContext.hasRecordedBegin = true;
            PerfMonitorContext.setBeginTime(System.nanoTime());
            PerfMonitorContext.setBeginWallTime(System.currentTimeMillis());
            LOG.info(">>> 【单个文件首次preDump】记录开始时间：{}", new java.util.Date(PerfMonitorContext.getBeginWallTime()));
        }

        metaConnection = new MysqlConnection(runningInfo.getAddress(),
                runningInfo.getUsername(),
                runningInfo.getPassword(),
                connectionCharsetNumber,
                runningInfo.getDefaultDatabaseName());

        if (tableMetaTSDB != null && tableMetaTSDB instanceof HyperTableMetaTSDB) {
            tableMetaTSDB.init(destination);
        }

        tableMetaCache = new TableMetaCache(metaConnection, tableMetaTSDB);
        ((LogEventConvert) binlogParser).setTableMetaCache(tableMetaCache);
    }

    @Override
    protected void afterDump(ErosaConnection connection) {
    }

    @Override
    public void stop() {
        if (tableMetaCache != null) {
            tableMetaCache.clearTableMeta();
        }
        ((AbstractMysqlEventParser) this).stop();
    }
}