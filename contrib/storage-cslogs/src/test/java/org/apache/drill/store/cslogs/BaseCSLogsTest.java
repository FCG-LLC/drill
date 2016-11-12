package org.apache.drill.store.cslogs;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.cslogs.CSLogsStoragePlugin;
import org.apache.drill.exec.store.cslogs.CSLogsStoragePluginConfig;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.ListTablesResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BaseCSLogsTest extends BaseTestQuery {
    private static final String CSLOGS_STORAGE_PLUGIN_NAME = "cslogs";

    protected static CSLogsStoragePlugin storagePlugin;
    protected static CSLogsStoragePluginConfig storagePluginConfig;

    @BeforeClass
    public static void setupDefaultTestCluster() throws Exception {
        BaseTestQuery.setupDefaultTestCluster();

        final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
        storagePlugin = (CSLogsStoragePlugin) pluginRegistry.getPlugin(CSLOGS_STORAGE_PLUGIN_NAME);
        storagePluginConfig = storagePlugin.getConfig();
        storagePluginConfig.setEnabled(true);
        pluginRegistry.createOrUpdate(CSLOGS_STORAGE_PLUGIN_NAME, storagePluginConfig, true);

        createTestTables();
    }

    private final static String[][] SAMPLE_LOG_PARAMS = new String[][] {
        new String[] {
            "foo", " ", "bar"
        },
        new String[] {
            "10.12.3.4", "1500"
        },
        new String[] {
            "10.12.3.4", "3000"
        },
        new String[] {
            "10.9.9.9", "5000"
        }
    };

    private final static int[][] SAMPLE_LOG_TIMESTAMP_PATTERN_UUID = new int[][] {
            new int[] { 1000, 3, 101 },
            new int[] { 1000, 9, 102 },
            new int[] { 1000, 9, 103 },
            new int[] { 1000, 9, 104 }
    };

    private static void createTrieNodesTable(KuduClient client) throws Exception {
        // FIXME
    }

    private static KuduTable recreateTable(KuduClient client, String name, List<ColumnSchema> columns, List<String> hashPartitions) throws Exception {
        ListTablesResponse tables = client.getTablesList(name);
        if (!tables.getTablesList().isEmpty()) {
            client.deleteTable(name);
        }

        Schema schema = new Schema(columns);
        CreateTableOptions builder = new CreateTableOptions();
        builder.setNumReplicas(1);
        builder.addHashPartitions(hashPartitions, 5);

        client.createTable(name, schema, builder);

        return client.openTable(name);
    }

    private static void createParamsTable(KuduClient client) throws Exception {
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("time_stamp_bucket", Type.INT32).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("template_id", Type.INT32).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("uuid", Type.INT64).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("param_no",  Type.INT16).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("param_value",  Type.STRING).nullable(true).build());

        KuduTable paramsTable = recreateTable(client, "log_params", columns, Arrays.asList("time_stamp_bucket"));

        KuduSession session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

        for (int i = 0; i < SAMPLE_LOG_PARAMS.length; i++) {
            String[] params = SAMPLE_LOG_PARAMS[i];
            int[] triple  = SAMPLE_LOG_TIMESTAMP_PATTERN_UUID[i];

            for (int j = 0; j < params.length; j++) {
                Insert insert = paramsTable.newInsert();
                PartialRow row = insert.getRow();
                row.addInt(0, triple[0]);
                row.addInt(1, triple[1]);
                row.addLong(2, triple[2]);

                row.addShort(3, (short) j);
                row.addString(4, params[j]);

                session.apply(insert);
            }
        }
    }

    private static void createInvertedParamsTable(KuduClient client) throws Exception {
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("param_value",  Type.STRING).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("template_id", Type.INT32).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("param_no",  Type.INT16).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("time_stamp_bucket", Type.INT32).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("uuid", Type.INT64).key(true).build());

        KuduTable paramsTable = recreateTable(client, "log_params_inverted", columns, Arrays.asList("time_stamp_bucket"));

        KuduSession session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

        for (int i = 0; i < SAMPLE_LOG_PARAMS.length; i++) {
            String[] params = SAMPLE_LOG_PARAMS[i];
            int[] triple  = SAMPLE_LOG_TIMESTAMP_PATTERN_UUID[i];

            Insert insert = paramsTable.newInsert();
            PartialRow row = insert.getRow();


            for (int j = 0; j < params.length; j++) {
                row.addString(0, params[j]);
                row.addInt(1, triple[1]);
                row.addShort(2, (short) j);
                row.addInt(3, triple[0]);
                row.addLong(4, triple[2]);
            }
            session.apply(insert);
        }
    }

    private static void createLogsTable(KuduClient client) throws Exception {
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("time_stamp_bucket", Type.INT32).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("uuid", Type.INT64).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("pattern_id", Type.INT32).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("server_ip1", Type.INT64).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("server_ip2", Type.INT64).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("time_stamp_remainder_ms",  Type.INT32).key(true).build());

        KuduTable logsTable = recreateTable(client, "logs", columns, Arrays.asList("time_stamp_bucket"));

        KuduSession session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

        for (int i = 0; i < SAMPLE_LOG_TIMESTAMP_PATTERN_UUID.length; i++) {
            int[] triple  = SAMPLE_LOG_TIMESTAMP_PATTERN_UUID[i];

            Insert insert = logsTable.newInsert();
            PartialRow row = insert.getRow();

            row.addInt(0, triple[0]);
            row.addLong(1, triple[2]);
            row.addInt(2, triple[1]);
            row.addLong(3, i);
            row.addLong(4, i);
            row.addInt(5, i*1000000);
            session.apply(insert);
        }
    }

    private static void createTestTables() throws Exception {
        try (KuduClient client = storagePlugin.getClient()) {
            createParamsTable(client);
            createInvertedParamsTable(client);
            createLogsTable(client);
        }
    }

    protected String getPlanText(String planFile, String tableName) throws IOException {
        return Files.toString(FileUtils.getResourceAsFile(planFile), Charsets.UTF_8)
                .replace("[TABLE_NAME]", "cslogs."+tableName);
    }

    protected void runCSLogsPhysicalVerifyCount(String planFile, String tableName, int expectedRowCount) throws Exception{
        String physicalPlan = getPlanText(planFile, tableName);
        List<QueryDataBatch> results = testPhysicalWithResults(physicalPlan);
        printResultAndVerifyRowCount(results, expectedRowCount);
    }

    protected List<QueryDataBatch> runCSLogsSQLlWithResults(String sql) throws Exception {
        sql = canonizeCSLogsSQL(sql);
        System.out.println("Running query:\n" + sql);
        return testSqlWithResults(sql);
    }

    protected void runCSLogsSQLVerifyCount(String sql, int expectedRowCount) throws Exception{
        List<QueryDataBatch> results = runCSLogsSQLlWithResults(sql);
        printResultAndVerifyRowCount(results, expectedRowCount);
    }

    private void printResultAndVerifyRowCount(List<QueryDataBatch> results, int expectedRowCount) throws SchemaChangeException {
        int rowCount = printResult(results);
        if (expectedRowCount != -1) {
            Assert.assertEquals(expectedRowCount, rowCount);
        }
    }

    protected String canonizeCSLogsSQL(String sql) {
        return sql.replace("[TABLE_NAME]", "cslogs.logs");
    }

}
