package com.smee.binlog.collector;

import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.MemoryTableMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.TableMetaTSDB;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import lombok.var;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class HyperTableMetaTSDB implements TableMetaTSDB {
    private static final Logger LOG = LoggerFactory.getLogger(HyperTableMetaTSDB.class);
    private static final String TABLE_NAME = "public.binlog_collector_ddl";

    private final String destination;
    private final ApplicationConfig config;
    private final DefaultBinlogPositionManager positionManager;
    private final MemoryTableMeta memoryTableMeta = new MemoryTableMeta();
    private final AtomicBoolean hasInited = new AtomicBoolean(false);

    public HyperTableMetaTSDB(String destination, ApplicationConfig config, DefaultBinlogPositionManager positionManager) {
        this.destination = destination;
        this.config = config;
        this.positionManager = positionManager;
    }

    @Override
    public boolean init(String destination) {
        synchronized (hasInited) {
            if (hasInited.get() == true) {
                return true;
            }

            var ddl = positionManager.getDdlFromDb();
            memoryTableMeta.apply(null, config.getSyncDatabase(), ddl, null);

            hasInited.set(true);
        }
        return true;
    }

    @Override
    public void destory() {
    }

    @Override
    public TableMeta find(String schema, String table) {
        return memoryTableMeta.find(schema, table);
    }

    @Override
    public boolean apply(EntryPosition position, String schema, String ddl, String extra) {
        memoryTableMeta.apply(position, schema, ddl, extra);
        var newDdl = String.join("\n", snapshot().values());
        positionManager.setUpdatedDdlDefer(newDdl);
        return true;
    }

    @Override
    public boolean rollback(EntryPosition position) {
        return true;
    }

    @Override
    public Map<String, String> snapshot() {
        return memoryTableMeta.snapshot();
    }
}