package com.smee.binlog.replayer;

import lombok.SneakyThrows;
import lombok.var;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Arrays;

@Component
public class PositionManager {

    private static final BinlogWatermark BINLOG_WATERMARK_EMPTY = BinlogWatermark.parse("mysql-bin.0|0");
    private static final String TABLE_NAME = "public.binlog_replayer_position";
    private static final String COLUMN_EQUIPMENT = "equipment";
    private static final String COLUMN_BINLOG = "binlog";
    private static final String COLUMN_KAFKA_OFFSET = "kafka_offset";
    private static final String COLUMN_CREATION_TIME = "creation_time";
    private final ApplicationConfig config;
    private final SqlExecutor metadataStore;

    public PositionManager(ApplicationConfig config) {
        this.config = config;
        this.metadataStore = new SqlExecutor(config.getMetadata().getDatabase());
    }

    @SneakyThrows
    public void start() {
        var sql = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "\n" +
                "(\n" +
                "    id         serial  NOT NULL primary key,\n" +
                String.format("  %s         varchar NOT NULL,\n", COLUMN_EQUIPMENT) +
                String.format("  %s         int8            ,\n", COLUMN_KAFKA_OFFSET) +
                String.format("  %s         varchar         ,\n", COLUMN_BINLOG) +
                "    creation_time int8                  \n" +
                ");\n" +
                "\n" +
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_equipment ON " + TABLE_NAME + " USING btree (equipment);\n";
        metadataStore.execute(sql);

        sql = String.format(
                "SELECT id from %s WHERE %s = '%s'",
                TABLE_NAME,
                COLUMN_EQUIPMENT,
                config.getEquipment());
        var rs = metadataStore.executeQuery(sql);
        if (rs.size() != 0) {
            return;
        }

        sql = String.format("INSERT INTO %s (%s, creation_time) VALUES (?, ?)", TABLE_NAME, COLUMN_EQUIPMENT);
        var values = Arrays.asList(config.getEquipment(), Instant.now().toEpochMilli());
        metadataStore.execute(sql, values);
    }

    @SneakyThrows
    public BinlogWatermark getLatestWatermark() {
        var sql = String.format(
                "SELECT %s from %s WHERE %s = '%s'",
                COLUMN_BINLOG,
                TABLE_NAME,
                COLUMN_EQUIPMENT,
                config.getEquipment());
        var rs = metadataStore.executeQuery(sql);
        if (rs.size() != 0) {
            var payload = rs.get(0).get(COLUMN_BINLOG);
            if (payload == null) {
                return BINLOG_WATERMARK_EMPTY;
            }
            return BinlogWatermark.parse((String) payload);
        }

        return BINLOG_WATERMARK_EMPTY;
    }

    @SneakyThrows
    public Long getLatestKafkaOffset() {
        var sql = String.format(
                "SELECT %s from %s WHERE %s = '%s'",
                COLUMN_KAFKA_OFFSET,
                TABLE_NAME,
                COLUMN_EQUIPMENT,
                config.getEquipment());
        var rs = metadataStore.executeQuery(sql);
        if (rs.size() != 0) {
            var payload = rs.get(0).get(COLUMN_KAFKA_OFFSET);
            if (payload == null) {
                return null;
            }
            return (Long) payload;
        }

        return null;
    }

    @SneakyThrows
    public void savePosition(BinlogWatermark watermark, long offset) {
        var sql = String.format("UPDATE %s SET %s=?, %s=?, %s=? WHERE %s=?", TABLE_NAME, COLUMN_BINLOG, COLUMN_KAFKA_OFFSET, COLUMN_CREATION_TIME, COLUMN_EQUIPMENT);
        var values = Arrays.asList(watermark.getRaw(), offset, Instant.now().toEpochMilli(), config.getEquipment());
        metadataStore.execute(sql, values);
    }
}