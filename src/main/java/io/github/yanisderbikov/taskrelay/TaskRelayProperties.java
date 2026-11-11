package io.github.yanisderbikov.taskrelay;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "taskrelay")
public class TaskRelayProperties {

    private boolean enabled = true;
    private String tableName = "taskrelay_task";
    private PayloadFormat payloadFormat = PayloadFormat.JACKSON;
    private Duration pollInterval = Duration.ofSeconds(5);
    private Duration initialDelay = Duration.ofSeconds(10);
    private int batchSize = 50;
    private Duration lockTimeout = Duration.ofMinutes(15);
    private int threads = 1;
    private Duration shutdownTimeout = Duration.ofSeconds(30);
    private Duration retention;
    private final Worker worker = new Worker();
    private final Schema schema = new Schema();
    private Map<String, TypeSettings> types = new LinkedHashMap<>();

    public enum PayloadFormat {
        JACKSON,
        GSON
    }

    public static class Worker {

        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class Schema {

        private boolean autoCreate = true;

        public boolean isAutoCreate() {
            return autoCreate;
        }

        public void setAutoCreate(boolean autoCreate) {
            this.autoCreate = autoCreate;
        }
    }

    public static class TypeSettings {

        private Integer batchSize;
        private Duration pollInterval;
        private Duration lockTimeout;
        private Boolean enabled;

        public Integer getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
        }

        public Duration getPollInterval() {
            return pollInterval;
        }

        public void setPollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
        }

        public Duration getLockTimeout() {
            return lockTimeout;
        }

        public void setLockTimeout(Duration lockTimeout) {
            this.lockTimeout = lockTimeout;
        }

        public Boolean getEnabled() {
            return enabled;
        }

        public void setEnabled(Boolean enabled) {
            this.enabled = enabled;
        }
    }

    TypeSettings settingsFor(String type) {
        String wanted = TaskTypes.normalize(type);
        for (Map.Entry<String, TypeSettings> entry : types.entrySet()) {
            if (TaskTypes.normalize(entry.getKey()).equals(wanted)) {
                return entry.getValue();
            }
        }
        return new TypeSettings();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public PayloadFormat getPayloadFormat() {
        return payloadFormat;
    }

    public void setPayloadFormat(PayloadFormat payloadFormat) {
        this.payloadFormat = payloadFormat;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }

    public Duration getInitialDelay() {
        return initialDelay;
    }

    public void setInitialDelay(Duration initialDelay) {
        this.initialDelay = initialDelay;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public Duration getLockTimeout() {
        return lockTimeout;
    }

    public void setLockTimeout(Duration lockTimeout) {
        this.lockTimeout = lockTimeout;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    public void setShutdownTimeout(Duration shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }

    public Duration getRetention() {
        return retention;
    }

    public void setRetention(Duration retention) {
        this.retention = retention;
    }

    public Worker getWorker() {
        return worker;
    }

    public Schema getSchema() {
        return schema;
    }

    public Map<String, TypeSettings> getTypes() {
        return types;
    }

    public void setTypes(Map<String, TypeSettings> types) {
        this.types = types;
    }
}
