package org.apache.drill.exec.store.cslogs;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.CompleteWork;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.LocatedTablet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@JsonTypeName("cslogs-scan")
public class CSLogsGroupScan extends AbstractGroupScan {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CSLogsGroupScan.class);
    private static final long DEFAULT_TABLET_SIZE = 1000;

    private CSLogsStoragePluginConfig storagePluginConfig;
    private List<SchemaPath> columns;
    private CSLogsScanSpec scanSpec;
    private CSLogsStoragePlugin storagePlugin;
    private boolean filterPushedDown = false;
    private List<CSLogsWork> workList = Lists.newArrayList();
    private ListMultimap<Integer,CSLogsWork> assignments;
    private List<EndpointAffinity> affinities;

    private KuduClient client;
//    private KuduTable table;
//    private Schema tableSchema;

    @JsonCreator
    public CSLogsGroupScan(@JsonProperty("scanSpec") CSLogsScanSpec scanSpec,
                         @JsonProperty("storage") CSLogsStoragePluginConfig storagePluginConfig,
                         @JsonProperty("columns") List<SchemaPath> columns,
                         @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
        this((CSLogsStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), scanSpec, columns);
    }

    public CSLogsGroupScan(CSLogsStoragePlugin storagePlugin, CSLogsScanSpec scanSpec,
                         List<SchemaPath> columns) {
        super((String) null);
        this.storagePlugin = storagePlugin;
        this.storagePluginConfig = storagePlugin.getConfig();
        this.scanSpec = scanSpec;
        this.columns = columns == null || columns.size() == 0? ALL_COLUMNS : columns;

        init();
    }

    private List<KuduScanToken> initScanTokens() throws KuduException {
//        ArrayList<KuduScanToken> allScanTokens = new ArrayList<>();
//
//        KuduScanSpecOptimizer scanSpecOptimizer = new KuduScanSpecOptimizer(kuduScanSpec, tableSchema);
//
//        // We want to get rid of items that would be inefficient in the scan
//        List<List<KuduPredicate>> predicatePermutationSets = scanSpecOptimizer.optimizeScanSpec(kuduScanSpec);
//        kuduScanSpec = scanSpecOptimizer.rebuildScanSpec(predicatePermutationSets);
//
//        for (List<KuduPredicate> predicateSet : predicatePermutationSets) {
//            KuduScanToken.KuduScanTokenBuilder scanTokenBuilder = client.newScanTokenBuilder(table);
//
//            if (!AbstractRecordReader.isStarQuery(columns)) {
//                List<String> colNames = Lists.newArrayList();
//                for (SchemaPath p : this.getColumns()) {
//                    colNames.add(p.getAsUnescapedPath());
//                }
//
//                // We must set projected columns in order, otherwise nasty things
//                // related to primary (composite) key columns might happen
//                Collections.sort(colNames, new Comparator<String>() {
//                    @Override
//                    public int compare(String o1, String o2) {
//                        return table.getSchema().getColumnIndex(o1) - table.getSchema().getColumnIndex(o2);
//                    }
//                });
//
//                scanTokenBuilder.setProjectedColumnNames(colNames);
//            }
//
//            KuduScanSpec pseudoScanSpec = new KuduScanSpec(getTableName(), predicateSet);
//            logger.info("Generated scan spec: {}", pseudoScanSpec.toString());
//
//            // Remove it
//            System.out.println("Generated scan spec: " + pseudoScanSpec.toString());
//
//            for (KuduPredicate pred : predicateSet) {
//                scanTokenBuilder.addPredicate(pred);
//            }
//
//            allScanTokens.addAll(scanTokenBuilder.build());
//        }
//
//        return allScanTokens;
        return Lists.newArrayList();
    }

    private void initFields() {
//        this.client = storagePlugin.getClient();
//        try {
//            this.table = client.openTable("kudu.logs");
//            this.tableSchema = this.table.getSchema();
//        } catch (KuduException ke) {
//            throw new RuntimeException(ke);
//        }
    }

    private void init() {
        initFields();

        Collection<CoordinationProtos.DrillbitEndpoint> endpoints = storagePlugin.getContext().getBits();
        Map<String,CoordinationProtos.DrillbitEndpoint> endpointMap = Maps.newHashMap();
        for (CoordinationProtos.DrillbitEndpoint endpoint : endpoints) {
            endpointMap.put(endpoint.getAddress(), endpoint);
        }

        try {
            final List<KuduScanToken> scanTokens = initScanTokens();

            for (KuduScanToken scanToken : scanTokens) {
                CSLogsWork work = new CSLogsWork(scanToken.serialize());

                for (LocatedTablet.Replica replica : scanToken.getTablet().getReplicas()) {
                    String host = replica.getRpcHost();
                    CoordinationProtos.DrillbitEndpoint ep = endpointMap.get(host);
                    if (ep != null) {
                        work.getByteMap().add(ep, DEFAULT_TABLET_SIZE);
                    }
                }
                workList.add(work);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

//    @JsonIgnore
//    public Schema getTableSchema() { return this.tableSchema; }



    private static class CSLogsWork implements CompleteWork {
        private EndpointByteMapImpl byteMap = new EndpointByteMapImpl();
        private byte[] serializedScanToken;

        public CSLogsWork(byte[] serializedScanToken) {
            this.serializedScanToken = serializedScanToken;
        }

        @Override
        public long getTotalBytes() {
            return DEFAULT_TABLET_SIZE;
        }

        @Override
        public EndpointByteMap getByteMap() {
            return byteMap;
        }

        @Override
        public int compareTo(CompleteWork o) {
            return 0;
        }

        public byte[] getSerializedScanToken() {
            return serializedScanToken;
        }
    }

    /**
     * Private constructor, used for cloning.
     * @param that The KuduGroupScan to clone
     */
    private CSLogsGroupScan(CSLogsGroupScan that) {
        super(that);
        this.columns = that.columns;
        this.scanSpec = that.scanSpec;
        this.storagePlugin = that.storagePlugin;
        this.storagePluginConfig = that.storagePluginConfig;
        this.filterPushedDown = that.filterPushedDown;
        this.workList = that.workList;
        this.assignments = that.assignments;
//        this.table = that.table;
//        this.tableSchema = that.tableSchema;
    }

    @Override
    public GroupScan clone(List<SchemaPath> columns) {
        CSLogsGroupScan newScan = new CSLogsGroupScan(this);
        newScan.columns = columns;
        return newScan;
    }

    @Override
    public List<EndpointAffinity> getOperatorAffinity() {
        if (affinities == null) {
            affinities = AffinityCreator.getAffinityMap(workList);
        }
        return affinities;
    }


    @Override
    public int getMaxParallelizationWidth() {
        return workList.size();
    }


    /**
     *
     * @param incomingEndpoints
     */
    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> incomingEndpoints) {
        assignments = AssignmentCreator.getMappings(incomingEndpoints, workList);
    }

    @Override
    public CSLogsSubScan getSpecificScan(int minorFragmentId) {
        List<CSLogsWork> workList = assignments.get(minorFragmentId);

        List<CSLogsSubScan.CSLogsSubScanSpec> scanSpecList = Lists.newArrayList();

        logger.info("Specific scan: {}", scanSpec.toString());

        for (CSLogsWork work : workList) {
            scanSpecList.add(new CSLogsSubScan.CSLogsSubScanSpec(getTableName(), work.getSerializedScanToken()));
        }

        return new CSLogsSubScan(storagePlugin, storagePluginConfig, scanSpecList, this.columns);
    }

    @Override
    public ScanStats getScanStats() {
        // Very naive - we just assume the more constraints the better...
        int constraintsDenominator = scanSpec.getParamsInvertedPredicates().size() + 1;
        long recordCount = (100000 / constraintsDenominator);

        //int columnsNominator = AbstractRecordReader.isStarQuery(columns) ? this.getTableSchema().getColumns().size() : this.getColumns().size();
        int columnsNominator = AbstractRecordReader.isStarQuery(columns) ? 1000 : this.getColumns().size();

        return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, recordCount, columnsNominator/((float) constraintsDenominator), columnsNominator * recordCount);
    }

    @Override
    @JsonIgnore
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
        Preconditions.checkArgument(children.isEmpty());
        return new CSLogsGroupScan(this);
    }

    @JsonIgnore
    public CSLogsStoragePlugin getStoragePlugin() {
        return storagePlugin;
    }

    @JsonIgnore
    public String getTableName() {
        return CSLogsStoragePlugin.LOG_TABLE_NAME;
    }

    @Override
    public String getDigest() {
        return toString();
    }

    @Override
    public String toString() {
        return "KuduGroupScan [KuduScanSpec="
                + scanSpec + ", columns="
                + columns + "]";
    }

    @JsonProperty("storage")
    public CSLogsStoragePluginConfig getStorageConfig() {
        return this.storagePluginConfig;
    }

    @JsonProperty
    public List<SchemaPath> getColumns() {
        return columns;
    }

    @JsonProperty
    public CSLogsScanSpec getScanSpec() {
        return scanSpec;
    }

    @Override
    @JsonIgnore
    public boolean canPushdownProjects(List<SchemaPath> columns) {
        return true;
    }

    @JsonIgnore
    public void setFilterPushedDown(boolean b) {
        this.filterPushedDown = b;
    }

    @JsonIgnore
    public boolean isFilterPushedDown() {
        return filterPushedDown;
    }

    /**
     * Empty constructor, do not use, only for testing.
     */
    @VisibleForTesting
    public CSLogsGroupScan() {
        super((String)null);
    }

    /**
     * Do not use, only for testing.
     */
    @VisibleForTesting
    public void setCSLogsScanSpec(CSLogsScanSpec kuduScanSpec) {
        this.scanSpec = kuduScanSpec;
    }

}
