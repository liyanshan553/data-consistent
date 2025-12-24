package com.smee.binlog.replayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

@Component
public class ChangelogExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ChangelogExceptionHandler.class);

    public boolean handle(String query, SQLException ex) {
        if (ex instanceof SQLIntegrityConstraintViolationException) {
            LOG.warn(String.format("ConstraintViolationException occurred when executing SQL: \n%s", query));
            return true;
        }

        if (ex.getErrorCode() == 1007) {
            LOG.warn(String.format("%s: %s", ex.getMessage(), query));
            return true;
        }

        if (ex.getErrorCode() == 1517) {
            LOG.warn(ex.getMessage());
            return true;
        }
        // MySQL: table already exists (CREATE TABLE xxx)
        if (ex.getErrorCode() == 1050 /* ER_TABLE_EXISTS_ERROR */
                || "42S01".equals(ex.getSQLState())) {
            LOG.warn(String.format("%s: %s", ex.getMessage(), query));
            return true;
        }

        return false;
    }
}