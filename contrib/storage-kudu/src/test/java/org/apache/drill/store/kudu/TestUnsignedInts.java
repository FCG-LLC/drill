package org.apache.drill.store.kudu;

import com.google.common.collect.ImmutableMap;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.kudu.KuduStoragePlugin;
import org.apache.drill.exec.store.kudu.KuduStoragePluginConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class TestUnsignedInts extends BaseKuduTest {

    private Map<String,Integer> conditionToCount = ImmutableMap.<String,Integer>builder()
            .put("key3 > 200", 1)
            .put("key3 > 100", 5)
            .put("key3 > 102", 3)
            .put("key3 > 205", 0)
            .put("key3 < 100", 0)
            .put("key3 >= 205", 1)
            .put("key3 < 255", 5)
            .put("key3 > 1000", 0)
            .put("key3 > 1100", 0)
            .put("key3 > 1200", 0)
            .put("key3 <= 205", 5)
            .put("key3 < 205", 4)
            .put("key3 < 0", 0)
            .put("key3 < -1", 0)
            .put("x >= 0", 5)
            .put("x > 0", 4)
            .put("x > 10000", 3)
            .put("x > 40000", 2)
            .put("x < 41000", 4)
            .put("x > 41000", 1)
            .put("x >= 32767", 3)
            .put("x <= 32767", 3)
            .put("x < 32767", 2)
            .put("x >= 32768", 2)
            .put("x <= 32768", 3)
            .put("x < 32768", 3)
            .put("x >= 65535", 1)
            .put("x > 65535", 0)
            .put("x <= 65535", 5)
            .build();

    @Before
    public void preparePlugin() throws Exception {
        final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
        storagePlugin = (KuduStoragePlugin) pluginRegistry.getPlugin(BaseKuduTest.KUDU_STORAGE_PLUGIN_NAME);
        storagePluginConfig = storagePlugin.getConfig();
        storagePluginConfig = new KuduStoragePluginConfig(
                storagePluginConfig.getMasterAddresses(),
                storagePluginConfig.getOperationTimeoutMs(),
                100,
                true,
                true);
        storagePluginConfig.setEnabled(true);
        pluginRegistry.createOrUpdate(KUDU_STORAGE_PLUGIN_NAME, storagePluginConfig, true);
    }

    @Test
    public void testColumnSelects() throws Exception {

        setColumnWidths(new int[] {8, 38, 38});
        final String baseSql = "SELECT\n"
                + "  key1, key3, x\n"
                + "FROM\n"
                + "  [TABLE_NAME]\n"
                + "WHERE ";

        for (String k : conditionToCount.keySet()) {
            int v = conditionToCount.get(k);
            try {
                String sql = baseSql + k;
                runKuduSQLVerifyCount(sql, v);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }


    @Test
    public void testColumnSelectNotEq() throws Exception {
        setColumnWidths(new int[] {8, 38, 38});
        setColumnWidths(new int[] {8, 38, 38});

        final String sql = "SELECT\n"
                + "  key1, key3, x\n"
                + "FROM\n"
                + "  [TABLE_NAME]\n"
                + "WHERE\n"
                + "  key2 <> 'a'";

        runKuduSQLVerifyCount(sql, 4);
    }

    @Test
    public void testColumnTwoEqualsNotPresent() throws Exception {
        setColumnWidths(new int[] {8, 38, 38});
        setColumnWidths(new int[] {8, 38, 38});

        final String sql = "SELECT\n"
                + "  key1, key3, x\n"
                + "FROM\n"
                + "  [TABLE_NAME]\n"
                + "WHERE\n"
                + "  (key3 = 99 AND x = 100)\n"
                + "ORDER BY key3";

        runKuduSQLVerifyCount(sql, 0);
    }
}
