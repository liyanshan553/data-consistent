package com.smee.binlog.replayer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Properties;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApplicationConfig {
    private boolean exitOnError = true;
    private String equipment;
    private String syncDatabase;
    private Properties targetDatabase;
    private MetadataConfig metadata;
    private KafkaConfig kafka;

    @Data
    @NoArgsConstructor
    public static class MetadataConfig {
        private Properties database;
        private long updateBatch;
        private LonghornConfig longhorn;
    }

    @Data
    @NoArgsConstructor
    public static class LonghornConfig {
        private boolean enabled = true;
        private String endpoint;
        private String volume;
        private long apiTimeout = 60;
        private int maxRetryCount = 3;
        private long updateBatch;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class KafkaConfig {
        private String topic;
        private long initialOffset;
        private long pollTimeout;
        private Properties properties;
    }
}