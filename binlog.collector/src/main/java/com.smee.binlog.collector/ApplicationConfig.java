package com.smee.binlog.collector;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Map;
import java.util.Properties;

@Data
public class ApplicationConfig {
    private boolean dryRun = false;
    private long restartDelay = 10000;
    private String equipment;
    private String syncDatabase;
    private Properties metadata;
    private String binlogLocation;
    private String initialDdl;
    private InitialEntryPosition initialEntryPosition;
    private int binlogPositionPersistStep;
    private int binlogFileDelayCount = 0;
    private KafkaConfig kafka;
    private QuirksConfig quirks = new QuirksConfig();

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class InitialEntryPosition {
        public String fileName;
        public Long position;
    }

    @Data
    @NoArgsConstructor
    public static class KafkaConfig {
        private long sendSizeThresholdBytes;
        private KafkaTopicConfig topic;
        private Properties properties;
    }

    @Data
    @NoArgsConstructor
    public static class KafkaTopicConfig {
        private String name;
        private short replicationFactor;
        private Map<String, String> properties;
    }

    @Data
    @NoArgsConstructor
    public static class QuirksConfig {
        private boolean seekLatestBinlogFromZero = false;
    }
}