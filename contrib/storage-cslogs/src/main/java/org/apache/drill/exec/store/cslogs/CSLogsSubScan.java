package org.apache.drill.exec.store.cslogs;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

@JsonTypeName("cslogs-tablet-scan")
public class CSLogsSubScan extends AbstractBase implements SubScan {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CSLogsSubScan.class);

    @JsonProperty
    public final CSLogsStoragePluginConfig storage;


    private final CSLogsStoragePlugin storagePlugin;
    private final List<CSLogsSubScanSpec> tabletScanSpecList;
    private final List<SchemaPath> columns;

    @JsonCreator
    public CSLogsSubScan(@JacksonInject StoragePluginRegistry registry,
                       @JsonProperty("storage") StoragePluginConfig storage,
                       @JsonProperty("tabletScanSpecList") LinkedList<CSLogsSubScanSpec> tabletScanSpecList,
                       @JsonProperty("columns") List<SchemaPath> columns) throws ExecutionSetupException {
        super((String) null);
        storagePlugin = (CSLogsStoragePlugin) registry.getPlugin(storage);
        this.tabletScanSpecList = tabletScanSpecList;
        this.storage = (CSLogsStoragePluginConfig) storage;
        this.columns = columns;
    }

    public CSLogsSubScan(CSLogsStoragePlugin plugin, CSLogsStoragePluginConfig config,
                       List<CSLogsSubScanSpec> tabletInfoList, List<SchemaPath> columns) {
        super((String) null);
        storagePlugin = plugin;
        storage = config;
        this.tabletScanSpecList = tabletInfoList;
        this.columns = columns;
    }

    public List<CSLogsSubScanSpec> getTabletScanSpecList() {
        return tabletScanSpecList;
    }

    @JsonIgnore
    public CSLogsStoragePluginConfig getStorageConfig() {
        return storage;
    }

    public List<SchemaPath> getColumns() {
        return columns;
    }

    @Override
    public boolean isExecutable() {
        return false;
    }

    @JsonIgnore
    public CSLogsStoragePlugin getStorageEngine(){
        return storagePlugin;
    }

    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
        return physicalVisitor.visitSubScan(this, value);
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
        Preconditions.checkArgument(children.isEmpty());
        return new CSLogsSubScan(storagePlugin, storage, tabletScanSpecList, columns);
    }

    @Override
    public Iterator<PhysicalOperator> iterator() {
        return Iterators.emptyIterator();
    }

    public static class CSLogsSubScanSpec {

        private final byte[] serializedInvertedIndexToken;
        private final byte[] serializedLogToken;

        @JsonCreator
        public CSLogsSubScanSpec(
                @JsonProperty("invertedIndexToken") byte[] serializedInvertedIndexToken,
                @JsonProperty("serializedLogToken") byte[] serializedLogToken
        ) {
            this.serializedInvertedIndexToken = serializedInvertedIndexToken;
            this.serializedLogToken = serializedLogToken;
        }

        public byte[] getSerializedInvertedIndexToken() {
            return serializedInvertedIndexToken;
        }

        public byte[] getSerializedLogToken() {
            return serializedLogToken;
        }

        public KuduScanner deserializeIntoInvertedIndexScanner(KuduClient client) throws IOException {
            return KuduScanToken.deserializeIntoScanner(serializedInvertedIndexToken, client);
        }

        public KuduScanner deserializeIntoLogScanner(KuduClient client) throws IOException {
            return KuduScanToken.deserializeIntoScanner(serializedLogToken, client);
        }

        public String toString(KuduClient client) throws IOException {
            return String.format("CSLogsSubScanSpec: {} / {} ", KuduScanToken.stringifySerializedToken(getSerializedInvertedIndexToken(), client), KuduScanToken.stringifySerializedToken(getSerializedLogToken(), client));
        }
    }

    @Override
    public int getOperatorType() {
        return UserBitShared.CoreOperatorType.HBASE_SUB_SCAN_VALUE;
    }

}
