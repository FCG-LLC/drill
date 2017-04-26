package org.apache.drill.store.kudu;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.kudu.KuduStoragePlugin;
import org.apache.drill.exec.store.kudu.KuduStoragePluginConfig;
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

public class BaseKuduTest extends BaseTestQuery {
    public static final String KUDU_STORAGE_PLUGIN_NAME = "kudu";
    private static final String TEST_TABLE_1 = "test_foo";

    protected static KuduStoragePlugin storagePlugin;
    protected static KuduStoragePluginConfig storagePluginConfig;

    private final static int OPTIMIZER_MAX_NON_PRIMARY_KEY_ALTERNATIVES = 1;

    @BeforeClass
    public static void setupDefaultTestCluster() throws Exception {
        BaseTestQuery.setupDefaultTestCluster();

        final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
        storagePlugin = (KuduStoragePlugin) pluginRegistry.getPlugin(KUDU_STORAGE_PLUGIN_NAME);
        storagePluginConfig = storagePlugin.getConfig();
        storagePluginConfig = new KuduStoragePluginConfig(
                storagePluginConfig.getMasterAddresses(),
                storagePluginConfig.getOperationTimeoutMs(),
                OPTIMIZER_MAX_NON_PRIMARY_KEY_ALTERNATIVES,
                true,
                true,
                true
        );
        storagePluginConfig.setEnabled(true);
        pluginRegistry.createOrUpdate(KUDU_STORAGE_PLUGIN_NAME, storagePluginConfig, true);

        createKuduTestTables();
    }

    private static void createKuduTestTables() throws Exception {
        try (KuduClient client = storagePlugin.getClient()) {

            ListTablesResponse tables = client.getTablesList(TEST_TABLE_1);
            if (!tables.getTablesList().isEmpty()) {
                client.deleteTable(TEST_TABLE_1);
            }

            List<ColumnSchema> columns = new ArrayList<>(4);
            columns.add(new ColumnSchema.ColumnSchemaBuilder("key1", Type.INT32).key(true).build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("key2", Type.STRING).key(true).build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("key3", Type.INT8).key(true).build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("x", Type.INT16).key(false).build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("str",  Type.STRING).nullable(true).build());

            Schema schema = new Schema(columns);
            CreateTableOptions builder = new CreateTableOptions();
            builder.setNumReplicas(1);
            builder.addHashPartitions(Arrays.asList("key3"), 5);

            client.createTable(TEST_TABLE_1, schema, builder);

            KuduTable table = client.openTable(TEST_TABLE_1);

            KuduSession session = client.newSession();
            session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

            int[] key1_values =    { 1, 0, Integer.MAX_VALUE, (int) 3000000000L, (int) 4200000000L };
            String[] key2_values = {  "a",  "b",  "b",  "b",  "d" };
            byte[] key3_values =   {  101,  102,  103,  104,  (byte) 205 };
            short[] x_values =     {    1,    0,  Short.MAX_VALUE, (short) 40500, (short) 65535 };
            String[] str_values =  { "xx", "yy", "zz", "uu", "vv" };

            for (int i = 0; i < key1_values.length; i++) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addInt(0, key1_values[i]);
                row.addString(1, key2_values[i]);
                row.addByte(2, key3_values[i]);
                row.addShort( 3, x_values[i]);
                row.addString(4, str_values[i]);
                session.apply(insert);
            }
        }
    }

    protected String getPlanText(String planFile, String tableName) throws IOException {
        return Files.toString(FileUtils.getResourceAsFile(planFile), Charsets.UTF_8)
                .replace("[TABLE_NAME]", "kudu."+tableName);
    }

    protected void runKuduPhysicalVerifyCount(String planFile, String tableName, int expectedRowCount) throws Exception{
        String physicalPlan = getPlanText(planFile, tableName);
        List<QueryDataBatch> results = testPhysicalWithResults(physicalPlan);
        printResultAndVerifyRowCount(results, expectedRowCount);
    }

    protected List<QueryDataBatch> runKuduSQLlWithResults(String sql) throws Exception {
        sql = canonizeKuduSQL(sql);
        System.out.println("Running query:\n" + sql);
        return testSqlWithResults(sql);
    }

    protected void runKuduSQLVerifyCount(String sql, int expectedRowCount) throws Exception{
        List<QueryDataBatch> results = runKuduSQLlWithResults(sql);
        printResultAndVerifyRowCount(results, expectedRowCount);
    }

    private void printResultAndVerifyRowCount(List<QueryDataBatch> results, int expectedRowCount) throws SchemaChangeException {
        int rowCount = printResult(results);
        if (expectedRowCount != -1) {
            Assert.assertEquals(expectedRowCount, rowCount);
        }
    }

    protected String canonizeKuduSQL(String sql) {
        return sql.replace("[TABLE_NAME]", "kudu."+TEST_TABLE_1);
    }

}
