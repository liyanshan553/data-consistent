package com.smee.binlog.replayer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import lombok.SneakyThrows;
import lombok.var;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SqlExecutor {
    private final Logger LOG = LoggerFactory.getLogger(SqlExecutor.class);
    private static final RetryRegistry RETRY_REGISTRY;
    private static final String STMT_RETRY_POLICY_NAME = "stmt";

    static {
        RETRY_REGISTRY = RetryRegistry.custom().build();
        var stmtRetryConfig = RetryConfig.custom()
                .maxAttempts(2)
                .waitDuration(Duration.ofMillis(50))
                .retryOnException(e -> true)
                .failAfterMaxAttempts(true)
                .build();
        RETRY_REGISTRY.addConfiguration(STMT_RETRY_POLICY_NAME, stmtRetryConfig);
    }

    private final MapListHandler mapListHandler = new MapListHandler();
    private final HikariDataSource db;

    @SneakyThrows
    public SqlExecutor(Properties prop) {
        if (!org.postgresql.Driver.isRegistered()) {
            org.postgresql.Driver.register();
        }

        HikariConfig hikariConfig = new HikariConfig(prop);
        this.db = new HikariDataSource(hikariConfig);
        this.db.getConnection().close();
    }

    public List<Map<String, Object>> executeQuery(String sql) throws SQLException {
        var retryRun = Retry.decorateCheckedSupplier(RETRY_REGISTRY.retry(STMT_RETRY_POLICY_NAME),
                () -> {
                    try (var conn = db.getConnection()) {
                        try (var stmt = conn.createStatement()) {
                            var rs = mapListHandler.handle(stmt.executeQuery(sql));
                            return rs;
                        }
                    }
                });

        try {
            return retryRun.apply();
        } catch (SQLException e) {
            throw e;
        } catch (Throwable e) {
            throw new SQLException(e);
        }
    }
    public void executeBatch(List<String> sqlList) throws SQLException {
        java.sql.Connection conn = null;
        try {
            conn = db.getConnection();
            conn.setAutoCommit(false);

            try (var stmt = conn.createStatement()) {
                for (String sql : sqlList) {
                    stmt.addBatch(sql);
                }
                stmt.executeBatch();
            }

            conn.commit();
        } catch (SQLException e) {
            if (conn != null) {
                try { conn.rollback(); } catch (SQLException ignored) {}
            }
            throw e;
        } finally {
            if (conn != null) {
                try { conn.close(); } catch (SQLException ignored) {}
            }
        }
    }

    public boolean execute(String sql, List<Object> values) throws SQLException {
        var retryRun = Retry.decorateCheckedSupplier(RETRY_REGISTRY.retry(STMT_RETRY_POLICY_NAME),
                () -> {
                    try (var conn = db.getConnection()) {
                        try (var stmt = conn.prepareStatement(sql)) {
                            setStatementValues(stmt, values);
                            return stmt.execute();
                        }
                    }
                });

        try {
            return retryRun.apply();
        } catch (SQLException e) {
            throw e;
        } catch (Throwable e) {
            throw new SQLException(e);
        }
    }

    private void setStatementValues(PreparedStatement stmt, List<Object> values) {
        Flux.fromIterable(values)
                .index()
                .doOnNext(t -> {
                    try {
                        var idx = (int) (t.getT1() + 1);
                        var v = t.getT2();
                        if (v instanceof String) {
                            stmt.setString(idx, (String) v);
                        } else if (v instanceof Long) {
                            stmt.setLong(idx, (Long) v);
                        }
                    } catch (SQLException e) {

                    }
                })
                .blockLast();
    }

    public boolean execute(String sql) throws SQLException {
        return execute(sql, Collections.emptyList());
    }

    public DataSource getDataSource() {
        return db;
    }
}