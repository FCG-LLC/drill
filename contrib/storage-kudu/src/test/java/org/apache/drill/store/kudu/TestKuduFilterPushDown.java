package org.apache.drill.store.kudu;

import org.apache.drill.PlanTestBase;
import org.junit.Test;

public class TestKuduFilterPushDown extends BaseKuduTest {
    @Test
    public void testFilterPushDownRowKeyEqual() throws Exception {
        setColumnWidths(new int[] {8, 38, 38});
        final String sql = "SELECT\n"
                + "  key1\n"
                + "FROM\n"
                + "  [TABLE_NAME]\n"
                + "WHERE\n"
                + "  key2 = 'b'";

        runKuduSQLVerifyCount(sql, 3);

        final String[] expectedPlan = {".*Predicates on table test_foo: `key2` = \"b\".*"};
        final String[] excludedPlan ={};
        final String sqlKudu = canonizeKuduSQL(sql);
        PlanTestBase.testPlanMatchingPatterns(sqlKudu, expectedPlan, excludedPlan);
    }

    @Test
    public void testFilterPushDownRowKeyIN() throws Exception {
        setColumnWidths(new int[] {8, 38, 38});
        final String sql = "SELECT\n"
            + "  key1\n"
            + "FROM\n"
            + "  [TABLE_NAME]\n"
            + "WHERE\n"
            + "  key1 > 0 AND key2 IN ('a','b')";

        runKuduSQLVerifyCount(sql, 4);

        final String[] expectedPlan = {".*Predicates on table test_foo: `key1` >= 1\\ AND \\{`key2` = \"a\" OR `key2` = \"b\"\\}.*"};
        final String[] excludedPlan ={};
        final String sqlKudu = canonizeKuduSQL(sql);
        PlanTestBase.testPlanMatchingPatterns(sqlKudu, expectedPlan, excludedPlan);
    }

    @Test
    public void testFilterPushDownRowKeyMutualOrAnd() throws Exception {
        setColumnWidths(new int[] {8, 38, 38});
        final String sql = "SELECT\n"
                + "  key1\n"
                + "FROM\n"
                + "  [TABLE_NAME]\n"
                + "WHERE\n"
                + "  (key1 >= 1 AND key2 = 'a') OR (key1 = 3 AND key2 = 'b')";

        runKuduSQLVerifyCount(sql, 2);

        final String[] expectedPlan = {".*Predicates on table test_foo\\: \\{`key1` >= 1 AND `key2` = \"a\"\\} OR \\{`key1` = 3 AND `key2` = \"b\"\\}.*"};
        final String[] excludedPlan ={};
        final String sqlKudu = canonizeKuduSQL(sql);
        PlanTestBase.testPlanMatchingPatterns(sqlKudu, expectedPlan, excludedPlan);
    }

    @Test
    public void testFilterPushDownRowKeyMutualAndOr() throws Exception {
        setColumnWidths(new int[] {8, 38, 38});
        final String sql = "SELECT\n"
                + "  key1\n"
                + "FROM\n"
                + "  [TABLE_NAME]\n"
                + "WHERE\n"
                + "  (key1 <= 1 OR key1 = 3) AND (key2 = 'a' OR key2 = 'b')";

        runKuduSQLVerifyCount(sql, 2);

        final String[] expectedPlan = {".*Predicates on table test_foo\\: \\{`key1` < 2 OR `key1` = 3\\} AND \\{`key2` = \"a\" OR `key2` = \"b\"\\}.*"};
        final String[] excludedPlan ={};
        final String sqlKudu = canonizeKuduSQL(sql);
        PlanTestBase.testPlanMatchingPatterns(sqlKudu, expectedPlan, excludedPlan);
    }

    @Test
    public void testFilterPushDownRowKeyMutualOrAndIn() throws Exception {
        setColumnWidths(new int[] {8, 38, 38});
        final String sql = "SELECT\n"
                + "  key1\n"
                + "FROM\n"
                + "  [TABLE_NAME]\n"
                + "WHERE\n"
                + "  (key1 >= 1 AND key2 = 'a') OR (key1 = 3 AND key2 IN ('a','b'))";

        runKuduSQLVerifyCount(sql, 2);

        final String[] expectedPlan = {".*Predicates on table test_foo\\: \\{`key1` >= 1 AND `key2` = \"a\"\\} OR \\{`key1` = 3 AND \\{`key2` = \"a\" OR `key2` = \"b\"\\}\\}.*"};
        final String[] excludedPlan ={};
        final String sqlKudu = canonizeKuduSQL(sql);
        PlanTestBase.testPlanMatchingPatterns(sqlKudu, expectedPlan, excludedPlan);
    }

    @Test
    public void testGapFilterPushDownRowKeyMutualOrAndIn() throws Exception {
        setColumnWidths(new int[] {8, 38, 38});
        final String sql = "SELECT\n"
                + "  key1\n"
                + "FROM\n"
                + "  [TABLE_NAME]\n"
                + "WHERE\n"
                + "  (key3 = 8) OR (key3 = 104 AND key2 IN ('a','b'))";

        runKuduSQLVerifyCount(sql, 1);

        final String[] expectedPlan = {".*Predicates on table test_foo\\: ,.*"};
        final String[] excludedPlan ={};
        final String sqlKudu = canonizeKuduSQL(sql);
        PlanTestBase.testPlanMatchingPatterns(sqlKudu, expectedPlan, excludedPlan);
    }

    @Test
    public void testGapFilterPushDownRowKeyRetainFirst() throws Exception {
        setColumnWidths(new int[] {8, 38, 38});
        final String sql = "SELECT\n"
                + "  key1\n"
                + "FROM\n"
                + "  [TABLE_NAME]\n"
                + "WHERE\n"
                + "  (key1 < 5) AND (key3 >= 104 OR key3 = 101)";

        runKuduSQLVerifyCount(sql, 3);

        final String[] expectedPlan = {".*Predicates on table test_foo\\: `key1` < 5,.*"};
        final String[] excludedPlan ={};
        final String sqlKudu = canonizeKuduSQL(sql);
        PlanTestBase.testPlanMatchingPatterns(sqlKudu, expectedPlan, excludedPlan);
    }

    @Test
    public void testGapFilterPushDownRowKeyRetainSecond() throws Exception {
        setColumnWidths(new int[] {8, 38, 38});
        final String sql = "SELECT\n"
                + "  key1\n"
                + "FROM\n"
                + "  [TABLE_NAME]\n"
                + "WHERE\n"
                + "  (key2 >= 'a') AND (key3 >= 104 OR key3 = 101)";

        runKuduSQLVerifyCount(sql, 3);

        final String[] expectedPlan = {".*Predicates on table test_foo\\: `key2` >= \"a\",.*"};
        final String[] excludedPlan ={};
        final String sqlKudu = canonizeKuduSQL(sql);
        PlanTestBase.testPlanMatchingPatterns(sqlKudu, expectedPlan, excludedPlan);
    }
}
