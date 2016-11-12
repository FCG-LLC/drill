package org.apache.drill.store.cslogs;

import org.apache.drill.PlanTestBase;
import org.junit.Ignore;
import org.junit.Test;

//@Ignore("requires a remote kudu server to run.")

// WARNING: THIS TESTS WILL MESS YOUR DB
public class TestCSLogsQueries extends BaseCSLogsTest {
    @Test
    public void testColumnSelect() throws Exception {
        setColumnWidths(new int[] {8, 38, 38});
        final String sql = "SELECT\n"
                + "  param1, param2\n"
                + "FROM\n"
                + "  [TABLE_NAME]\n";

        runCSLogsSQLVerifyCount(sql, 4);

        final String[] expectedPlan = {".*columns=\\[`key1`\\].*"};
        final String[] excludedPlan ={};
        final String sqlKudu = canonizeCSLogsSQL(sql);
        PlanTestBase.testPlanMatchingPatterns(sqlKudu, expectedPlan, excludedPlan);
    }
}