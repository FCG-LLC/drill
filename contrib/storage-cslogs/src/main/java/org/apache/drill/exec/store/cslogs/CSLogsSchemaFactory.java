package org.apache.drill.exec.store.cslogs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.graph.GraphVisitor;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.Writer;
import org.apache.drill.exec.planner.logical.CreateTableEntry;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class CSLogsSchemaFactory implements SchemaFactory {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CSLogsSchemaFactory.class);

    final String schemaName;
    final CSLogsStoragePlugin plugin;

    public CSLogsSchemaFactory(CSLogsStoragePlugin plugin, String name) throws IOException {
        this.plugin = plugin;
        this.schemaName = name;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        CSLogsTables schema = new CSLogsTables(schemaName);
        SchemaPlus hPlus = parent.add(schemaName, schema);
        schema.setHolder(hPlus);
    }

    class CSLogsTables extends AbstractSchema {

        public CSLogsTables(String name) {
            super(ImmutableList.<String>of(), name);
        }

        public void setHolder(SchemaPlus plusOfThis) {
        }

        @Override
        public AbstractSchema getSubSchema(String name) {
            return null;
        }

        @Override
        public Set<String> getSubSchemaNames() {
            return Collections.emptySet();
        }

        @Override
        public Table getTable(String name) {
            try {
                return new DrillCSLogsTable(schemaName, plugin, new CSLogsScanSpec());
            } catch (Exception e) {
                logger.warn("Failure while retrieving kudu table {}", name, e);
                return null;
            }

        }

        @Override
        public Set<String> getTableNames() {
            return Sets.newHashSet(CSLogsStoragePlugin.LOG_TABLE_NAME);
        }

        @Override
        public CreateTableEntry createNewTable(final String tableName, List<String> partitionColumns) {
            return new CreateTableEntry(){

                @Override
                public Writer getWriter(PhysicalOperator child) throws IOException {
                    return new Writer() {
                        @Override
                        public boolean isExecutable() {
                            return false;
                        }

                        @Override
                        public BatchSchema.SelectionVectorMode getSVMode() {
                            return null;
                        }

                        @Override
                        public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
                            return null;
                        }

                        @Override
                        public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
                            return null;
                        }

                        @Override
                        public long getInitialAllocation() {
                            return 0;
                        }

                        @Override
                        public long getMaxAllocation() {
                            return 0;
                        }

                        @Override
                        public int getOperatorId() {
                            return 0;
                        }

                        @Override
                        public void setOperatorId(int id) {

                        }

                        @Override
                        public void setCost(double cost) {

                        }

                        @Override
                        public double getCost() {
                            return 0;
                        }

                        @Override
                        public String getUserName() {
                            return null;
                        }

                        @Override
                        public int getOperatorType() {
                            return 0;
                        }

                        @Override
                        public void accept(GraphVisitor<PhysicalOperator> visitor) {

                        }

                        @Override
                        public Iterator<PhysicalOperator> iterator() {
                            return null;
                        }
                    };
                }

                @Override
                public List<String> getPartitionColumns() {
                    return Collections.emptyList();
                }

            };
        }

        @Override
        public void dropTable(String tableName) {
            throw UserException.dataWriteError()
                    .message("Tables in CS Logs plugin cannot be dropped")
                    .addContext("plugin", name)
                    .build(logger);
        }

        @Override
        public boolean isMutable() {
            return true;
        }

        @Override
        public String getTypeName() {
            return CSLogsStoragePluginConfig.NAME;
        }

    }

}
