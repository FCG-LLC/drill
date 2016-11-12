package org.apache.drill.exec.store.cslogs;

import org.apache.drill.common.logical.StoragePluginConfigBase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(CSLogsStoragePluginConfig.NAME)
public class CSLogsStoragePluginConfig extends StoragePluginConfigBase {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CSLogsStoragePluginConfig.class);

    public static final String NAME = "cslogs";

    private final String masterAddresses;
    private final long operationTimeoutMs;

    @JsonCreator
    public CSLogsStoragePluginConfig(@JsonProperty("masterAddresses") String masterAddresses, @JsonProperty("operationTimeoutMs") long operationTimoutMs) {
        this.masterAddresses = masterAddresses;
        this.operationTimeoutMs = operationTimoutMs;
    }

    public String getMasterAddresses() {
        return masterAddresses;
    }

    public long getOperationTimeoutMs() { return operationTimeoutMs; }

    @Override
    public int hashCode() {
        int result = masterAddresses.hashCode();
        result = 31 * result + (int) (operationTimeoutMs ^ (operationTimeoutMs >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CSLogsStoragePluginConfig that = (CSLogsStoragePluginConfig) o;

        if (operationTimeoutMs != that.operationTimeoutMs) {
            return false;
        }

        return masterAddresses != null ? masterAddresses.equals(that.masterAddresses) : that.masterAddresses == null;
    }

}
