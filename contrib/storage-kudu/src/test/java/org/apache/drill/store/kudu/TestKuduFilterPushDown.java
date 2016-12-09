package org.apache.drill.store.kudu;

import org.apache.drill.PlanTestBase;
import org.junit.Ignore;
import org.junit.Test;

//@Ignore("requires a remote kudu server to run.")
public class TestKuduFilterPushDown extends BaseKuduTest {
    @Test
    public void testColumnSelect() throws Exception {
        setColumnWidths(new int[] {8, 38, 38});
        final String sql = "SELECT\n"
                + "  key1\n"
                + "FROM\n"
                + "  [TABLE_NAME]\n";

        runKuduSQLVerifyCount(sql, 5);

        final String[] expectedPlan = {".*columns=\\[`key1`\\].*"};
        final String[] excludedPlan = {};
        final String sqlKudu = canonizeKuduSQL(sql);
        PlanTestBase.testPlanMatchingPatterns(sqlKudu, expectedPlan, excludedPlan);
    }

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
        final String[] excludedPlan = {};
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

        final String[] expectedPlan = {".*Predicates on table test_foo: \\{`key1` >= 1 AND `key2` = \"a\"\\} OR \\{`key1` >= 1 AND `key2` = \"b\"\\}.*"};
        final String[] excludedPlan = {};
        final String sqlKudu = canonizeKuduSQL(sql);
        PlanTestBase.testPlanMatchingPatterns(sqlKudu, expectedPlan, excludedPlan);
    }

    @Test
    public void testFilterPushDownRowKeyINNonPrimaryKey() throws Exception {
        setColumnWidths(new int[] {8, 38, 38});
        final String sql = "SELECT\n"
                + "  key1\n"
                + "FROM\n"
                + "  [TABLE_NAME]\n"
                + "WHERE\n"
                + "  key1 > 0 AND str IN ('a','b','uu')";

        runKuduSQLVerifyCount(sql, 1);

        final String[] expectedPlan = {".*Predicates on table test_foo: `key1` >= 1.*"};
        final String[] excludedPlan = {};
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
        final String[] excludedPlan = {};
        final String sqlKudu = canonizeKuduSQL(sql);
        PlanTestBase.testPlanMatchingPatterns(sqlKudu, expectedPlan, excludedPlan);
    }

//    @Test
    @Ignore
    public void testFilterPushDownRowKeyMutualOrAndIncludingNonPrimaryKey() throws Exception {
        setColumnWidths(new int[] {8, 38, 38});
        final String sql = "SELECT\n"
                + "  key1\n"
                + "FROM\n"
                + "  [TABLE_NAME]\n"
                + "WHERE\n"
                + "  (key1 >= 1 AND key2 = 'a' AND str = 'xx') OR (key1 = 3 AND key2 = 'b' AND str = 'uu') OR (key1 = 3 AND key2 = 'b' AND str = 'uuuuuuu')";

        runKuduSQLVerifyCount(sql, 2);

        // If following fails then a probable cause is different order within each permutation set
        final String[] expectedPlan = {
                ".*\\{`key2` =\\s+\"a\" AND `key1` >= 1 AND `str` =\\s+\"xx\"\\}.*",
                ".*\\{`key2` =\\s+\"b\" AND `key1` = 3\\}.*",
                ".*\\} OR \\{.*"
        };
        final String[] excludedPlan = {};
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

        final String[] expectedPlan = {".*Predicates on table test_foo\\: \\{`key1` < 2 AND `key2` = \"a\"\\} OR \\{`key1` < 2 AND `key2` = \"b\"\\} OR \\{`key1` = 3 AND `key2` = \"a\"\\} OR \\{`key1` = 3 AND `key2` = \"b\"\\}.*"};
        final String[] excludedPlan = {};
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

        final String[] expectedPlan = {".*Predicates on table test_foo\\: \\{`key1` >= 1 AND `key2` = \"a\"\\} OR \\{`key1` = 3 AND `key2` = \"a\"\\} OR \\{`key1` = 3 AND `key2` = \"b\"\\}.*"};
        final String[] excludedPlan = {};
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
        final String[] excludedPlan = {};
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
        final String[] excludedPlan = {};
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
        final String[] excludedPlan = {};
        final String sqlKudu = canonizeKuduSQL(sql);
        PlanTestBase.testPlanMatchingPatterns(sqlKudu, expectedPlan, excludedPlan);
    }

    @Test
    public void testConflictingFilterPushDown() throws Exception {
        setColumnWidths(new int[] {8, 38, 38});
        final String sql = "SELECT\n"
                + "  key1\n"
                + "FROM\n"
                + "  [TABLE_NAME]\n"
                + "WHERE\n"
                + "  (key2 = 'a' AND key2 = 'b')";

        runKuduSQLVerifyCount(sql, 0);

        final String[] expectedPlan = {
                ".*`key2` = \"a\".*",
                ".*AND.*",
                ".*`key2` = \"b\".*"};
        final String[] excludedPlan = {};
        final String sqlKudu = canonizeKuduSQL(sql);
        PlanTestBase.testPlanMatchingPatterns(sqlKudu, expectedPlan, excludedPlan);
    }

    @Test
    public void testNoResultsQuery() throws Exception {
        setColumnWidths(new int[] {8, 38, 38});
        final String sql = "SELECT\n"
                + "  key1\n"
                + "FROM\n"
                + "  [TABLE_NAME]\n"
                + "WHERE\n"
                + "  key2 = 'such value is not present'";

        runKuduSQLVerifyCount(sql, 0);
    }
}