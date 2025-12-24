package com.smee.binlog.collector;

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
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Component // 从上下文推断可能有这个注解，虽然图7只显示了 import
public class SqlExecutor {
    private final Logger LOG = LoggerFactory.getLogger(SqlExecutor.class);
    private static final RetryRegistry RETRY_REGISTRY;
    private static final String STMT_RETRY_POLICY_NAME = "stmt";

    static {
        RETRY_REGISTRY = RetryRegistry.custom().build();
        var stmtRetryConfig = RetryConfig.custom()
                .maxAttempts(3)
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
            LOG.error("", e);
            throw e;
        } catch (Throwable e) {
            LOG.error("", e);
            System.exit(1);
            throw new SQLException(e);
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
            LOG.error("", e);
            throw e;
        } catch (Throwable e) {
            LOG.error("", e);
            System.exit(1);
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
                        // 异常处理逻辑在 lambda 中可能需要特殊处理，图上看起来是空的或只有结尾大括号
                    }
                })
                .blockLast();
    }

    public boolean execute(String sql) throws SQLException {
        return execute(sql, Collections.emptyList());
    }
}