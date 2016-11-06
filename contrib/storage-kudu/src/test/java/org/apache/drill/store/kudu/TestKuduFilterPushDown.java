package org.apache.drill.store.kudu;

import org.apache.drill.PlanTestBase;
import org.apache.drill.QueryTestUtil;
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
//@Test
//public void testFilterPushDownRowKeyIN() throws Exception {
//    setColumnWidths(new int[] {8, 38, 38});
//    final String sql = "SELECT\n"
//            + "  key1\n"
//            + "FROM\n"
//            + "  [TABLE_NAME]\n"
//            + "WHERE\n"
//            + "  key2 IN ('a','b')";
//
//    runKuduSQLVerifyCount(sql, 4);
//
//    final String[] expectedPlan = {".*Predicates on table test_foo: `key2` = \"b\".*"};
//    final String[] excludedPlan ={};
//    final String sqlKudu = canonizeKuduSQL(sql);
//    PlanTestBase.testPlanMatchingPatterns(sqlKudu, expectedPlan, excludedPlan);
}

}
