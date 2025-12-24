package com.smee.binlog.collector;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.mysql.LocalBinlogEventParser;
import com.alibaba.otter.canal.parse.index.AbstractLogPositionManager;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import lombok.SneakyThrows;
import lombok.var;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class DefaultBinlogPositionManager extends AbstractLogPositionManager {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultBinlogPositionManager.class);
    private final String TABLE_NAME = "public.binlog_collector_position";
    private final ApplicationConfig config;
    private final SqlExecutor sqlExecutor;
    private final LocalBinlogEventParser localBinlogEventParser;
    private final AtomicReference<String> updatedDdlDefer = new AtomicReference<>(null);

    public DefaultBinlogPositionManager(ApplicationConfig config, LocalBinlogEventParser localBinlogEventParser) throws SQLException {
        this.config = config;
        this.localBinlogEventParser = localBinlogEventParser;
        this.sqlExecutor = new SqlExecutor(config.getMetadata());

        seedDatabaseIfNecessary();
    }

    @SneakyThrows
    private void seedDatabaseIfNecessary() {
        var sql = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "\n" +
                "(\n" +
                "    id          serial NOT NULL primary key,\n" +
                "    equipment   varchar NOT NULL,\n" +
                "    last_position varchar,\n" +
                "    ddl         varchar,\n" +
                "    creation_time int8\n" +
                ");\n" +
                "\n" +
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_equipment ON " + TABLE_NAME + " USING btree (equipment);\n";
        sqlExecutor.execute(sql);

        sql = String.format(
                "SELECT id from %s WHERE equipment = '%s'",
                TABLE_NAME,
                config.getEquipment());
        var rs = sqlExecutor.executeQuery(sql);
        if (rs.size() != 0) {
            return;
        }

        var initialDdlFile = config.getInitialDdl();
        String ddl;
        if (initialDdlFile == null || initialDdlFile.isEmpty()) {
            ddl = "";
        } else {
            ddl = new String(Files.readAllBytes(Paths.get(config.getInitialDdl())));
        }
        sql = String.format("INSERT INTO %s (equipment, ddl, creation_time) VALUES (?, ?, ?)", TABLE_NAME);
        var values = Arrays.asList(config.getEquipment(), ddl, Instant.now().toEpochMilli());
        sqlExecutor.execute(sql, values);
    }

    @SneakyThrows
    protected LogPosition getLatestIndexByInDb(String destination) {
        var sql = String.format(
                "SELECT last_position from %s WHERE equipment = '%s'",
                TABLE_NAME,
                destination);

        try {
            var rs = sqlExecutor.executeQuery(sql);
            if (rs.size() == 0) {
                return null;
            }
            var lastPosition = rs.get(0).get("last_position");
            if (lastPosition == null || lastPosition.toString().isEmpty()) {
                return null;
            }

            var rowValue = lastPosition.toString().split("\\|");
            var currPosition = new EntryPosition(rowValue[0], Long.parseLong(rowValue[1]), 1L);

            return new LogPosition() {{
                setPostion(currPosition);
            }};
        } catch (SQLException e) {
            LOG.error("", e);
            localBinlogEventParser.stop();
            throw e;
        }
    }

    @Override
    @SneakyThrows
    public LogPosition getLatestIndexBy(String destination) {
        var dbPos = getLatestIndexByInDb(destination);
        if (config.getQuirks().isSeekLatestBinlogFromZero()) {
            var quirkPosition = new EntryPosition(dbPos.getPostion().getJournalName(), 4L, 1L);
            return new LogPosition() {{
                setPostion(quirkPosition);
            }};
        } else {
            return dbPos;
        }
    }

    @Override
    @Deprecated
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
    }

    public void persistLogPositionReal(String destination, EntryPosition logPosition) throws CanalParseException {
        if (config.isDryRun()) {
            return;
        }

        var posValue = Utils.EntryPosition2String(logPosition);
        var sql = String.format(
                "UPDATE %s SET last_position = '%s', creation_time = %s WHERE equipment = '%s'",
                TABLE_NAME,
                posValue,
                Instant.now().toEpochMilli(),
                destination
        );

        try {
            sqlExecutor.execute(sql);
        } catch (PSQLException e) {
            if (e.getSQLState().equals("23505")) {
                return;
            }
            LOG.error("", e);
            localBinlogEventParser.stop();
            throw new CanalParseException(e);
        } catch (SQLException e) {
            LOG.error("", e);
            localBinlogEventParser.stop();
            throw new CanalParseException(e);
        }

        updatedDdlDefer.getAndUpdate(v -> {
            if (!Objects.equals(v, null)) {
                updateDdl(v);
            }
            return null;
        });
    }

    public String getDdlFromDb() {
        var sql = String.format(
                "SELECT ddl from %s WHERE equipment = '%s'",
                TABLE_NAME,
                config.getEquipment());
        try {
            var rs = sqlExecutor.executeQuery(sql);
            return rs.get(0).get("ddl").toString();
        } catch (SQLException e) {
            System.exit(1);
            throw new RuntimeException(e);
        }
    }

    private void updateDdl(String ddl) {
        if (config.isDryRun()) {
            return;
        }

        var sql = String.format(
                "UPDATE %s SET ddl = ?, creation_time = ? WHERE equipment = ?",
                TABLE_NAME
        );

        var values = Arrays.asList(ddl, Instant.now().toEpochMilli(), config.getEquipment());
        try {
            sqlExecutor.execute(sql, values);
        } catch (SQLException e) {
            LOG.error("", e);
            localBinlogEventParser.stop();
            throw new CanalParseException(e);
        }
    }

    @Override
    public void stop() {
        sqlExecutor = null;
        super.stop();
    }

    public void setUpdatedDdlDefer(String ddl) {
        this.updatedDdlDefer.set(ddl);
    }
}