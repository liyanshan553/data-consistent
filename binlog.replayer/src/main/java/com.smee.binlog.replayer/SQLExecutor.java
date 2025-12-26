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
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
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
    /**
     * 批量执行SQL，支持重复键容错。
     * 需要JDBC URL配置: continueBatchOnError=true&rewriteBatchedStatements=true
     *
     * @param sqlList SQL列表
     * @return 执行结果：skippedCount（跳过的重复键数量）
     * @throws SQLException 遇到不可跳过的错误时抛出
     */
    public BatchResult executeBatch(List<String> sqlList) throws SQLException {
        java.sql.Connection conn = null;
        int skippedCount = 0;
        List<String> skippedSqls = new ArrayList<>();

        try {
            conn = db.getConnection();
            conn.setAutoCommit(false);

            try (var stmt = conn.createStatement()) {
                for (String sql : sqlList) {
                    stmt.addBatch(sql);
                }

                try {
                    stmt.executeBatch();
                } catch (BatchUpdateException bue) {
                    // continueBatchOnError=true 时，批量执行会继续，这里处理结果
                    int[] updateCounts = bue.getUpdateCounts();
                    SQLException currentEx = bue;

                    for (int i = 0; i < updateCounts.length; i++) {
                        if (updateCounts[i] == Statement.EXECUTE_FAILED) {
                            String failedSql = i < sqlList.size() ? sqlList.get(i) : "unknown";

                            // 检查是否为可跳过的异常（重复键、表已存在等）
                            if (isSkippableException(currentEx)) {
                                skippedCount++;
                                skippedSqls.add(failedSql);
                                LOG.warn("[BATCH] Skipped duplicate/constraint error at index {}: {}",
                                        i, truncateSql(failedSql));
                            } else {
                                // 不可跳过的错误，回滚并抛出
                                LOG.error("[BATCH] Non-skippable error at index {}: {}", i, failedSql);
                                throw bue;
                            }
                        }
                    }

                    // 如果还有链式异常，检查是否都是可跳过的
                    while ((currentEx = currentEx.getNextException()) != null) {
                        if (!isSkippableException(currentEx)) {
                            throw bue;
                        }
                    }
                }
            }

            conn.commit();

            if (skippedCount > 0) {
                LOG.info("[BATCH] Completed with {} skipped duplicates out of {} total",
                        skippedCount, sqlList.size());
            }

            return new BatchResult(sqlList.size(), skippedCount, skippedSqls);

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

    /**
     * 判断是否为可跳过的异常（重复键、约束冲突、表已存在等）
     */
    private boolean isSkippableException(SQLException ex) {
        if (ex instanceof SQLIntegrityConstraintViolationException) {
            return true;
        }
        int errorCode = ex.getErrorCode();
        // MySQL error codes:
        // 1062: Duplicate entry (ER_DUP_ENTRY)
        // 1007: Database already exists (ER_DB_CREATE_EXISTS)
        // 1050: Table already exists (ER_TABLE_EXISTS_ERROR)
        // 1517: Duplicate partition name (ER_SAME_NAME_PARTITION)
        return errorCode == 1062 || errorCode == 1007 || errorCode == 1050 || errorCode == 1517;
    }

    private String truncateSql(String sql) {
        return sql.length() > 200 ? sql.substring(0, 200) + "..." : sql;
    }

    /**
     * 批量执行结果
     */
    public static class BatchResult {
        private final int totalCount;
        private final int skippedCount;
        private final List<String> skippedSqls;

        public BatchResult(int totalCount, int skippedCount, List<String> skippedSqls) {
            this.totalCount = totalCount;
            this.skippedCount = skippedCount;
            this.skippedSqls = skippedSqls;
        }

        public int getTotalCount() { return totalCount; }
        public int getSkippedCount() { return skippedCount; }
        public List<String> getSkippedSqls() { return skippedSqls; }
        public int getSuccessCount() { return totalCount - skippedCount; }
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