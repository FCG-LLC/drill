package org.apache.drill.store.kudu;

import org.apache.drill.exec.store.kudu.KuduScanSpecOptimizer;
import org.apache.drill.exec.store.kudu.KuduUtils;
import org.apache.drill.exec.store.kudu.NonPrimaryKeyPermutationPruner;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Integration test of {@link NonPrimaryKeyPermutationPruner#reduceNonePredicates()}
 * integrated with {@link KuduUtils}
 * NOTE: It's named Unit test because it can be run without external dependencies
 */
public class ReduceNonePredicatesUnitTest {

    private NonPrimaryKeyPermutationPruner pruner;
    private KuduScanSpecOptimizer kuduOptimizer;
    private Schema testSchema;

    // copied from KuduPredicate internals:
    private static long maxIntValue(ColumnSchema column) {
        switch (column.getType()) {
            case INT8:
                return Byte.MAX_VALUE;
            case INT16:
                return Short.MAX_VALUE;
            case INT32:
                return Integer.MAX_VALUE;
            case UNIXTIME_MICROS:
            case INT64:
                return Long.MAX_VALUE;
            default:
                throw new IllegalArgumentException("type must be an integer type");
        }
    }

    // copied from KuduPredicate internals:
    private static long minIntValue(ColumnSchema column) {
        switch (column.getType()) {
            case INT8:
                return Byte.MIN_VALUE;
            case INT16:
                return Short.MIN_VALUE;
            case INT32:
                return Integer.MIN_VALUE;
            case UNIXTIME_MICROS:
            case INT64:
                return Long.MIN_VALUE;
            default:
                throw new IllegalArgumentException("type must be an integer type");
        }
    }

    private Schema createTestSchema() {
        List<ColumnSchema> columns = new ArrayList<>(4);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key1", Type.INT32).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("x8", Type.INT8).key(false).nullable(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("x16", Type.INT16).key(false).nullable(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("x32", Type.INT32).key(false).nullable(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("x64", Type.INT64).key(false).nullable(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("y8", Type.INT8).key(false).nullable(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("y16", Type.INT16).key(false).nullable(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("y32", Type.INT32).key(false).nullable(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("y64", Type.INT64).key(false).nullable(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("str", Type.STRING).nullable(true).build());
        Schema schema = new Schema(columns);
        return schema;
    }

    // In KuduScanSpecOptimizer a List<KuduPredicate> is basically a conjunction
    private List<KuduPredicate> and(KuduPredicate... preds) {
        List<KuduPredicate> result = new ArrayList<>(preds.length);
        result.addAll(Arrays.asList(preds));
        return result;
    }

    // In KuduScanSpecOptimizer a List<List<KuduPredicate>> is basically
    // an alternative of conjunctions
    private List<List<KuduPredicate>> or(List<KuduPredicate>... andConditions) {
        List<List<KuduPredicate>> or = new ArrayList<>();
        or.addAll(Arrays.asList(andConditions));
        return or;
    }

    private NonPrimaryKeyPermutationPruner createPruner(List<List<KuduPredicate>> permutationSet) {
        return new NonPrimaryKeyPermutationPruner(new KuduUtils(), kuduOptimizer, permutationSet);
    }

    @Before
    public void setUp() {
        kuduOptimizer = mock(KuduScanSpecOptimizer.class);
        testSchema = createTestSchema();
    }

    @Test
    public void allReducesToNone() {

        ColumnSchema x = testSchema.getColumn("x8");
        ColumnSchema y = testSchema.getColumn("y8");

        // Some valid predicates:
        KuduPredicate x1 = KuduPredicate.newComparisonPredicate(x, ComparisonOp.GREATER, maxIntValue(x) - 50);
        KuduPredicate x2 = KuduPredicate.newComparisonPredicate(x, ComparisonOp.LESS, maxIntValue(x) - 40);

        // Will be NONE due to wanted values out of type range, so there are no such records:
        KuduPredicate yMaxNone = KuduPredicate.newComparisonPredicate(y, ComparisonOp.GREATER, maxIntValue(y));
        KuduPredicate yMinNone = KuduPredicate.newComparisonPredicate(y, ComparisonOp.LESS, minIntValue(y));

        pruner = createPruner(
                or(
                        and(x1, yMaxNone), // reduces to NONE
                        and(x2, yMinNone) // reduces to NONE
                ) // therefore whole OR reduces to NONE
        );

        KuduPredicate expNone = yMaxNone; // we use 1st NONE for to represent the top/general NONE
        List<List<KuduPredicate>> expected = or(and(expNone));
        List<List<KuduPredicate>> actual = pruner.reduceNonePredicates();
        assertEquals(expected, actual);
        assertTrue(pruner.isNone());
    }

    @Test
    public void complexTestInt8() {
        complexTest(testSchema.getColumn("x8"), testSchema.getColumn("y8"));
    }

    @Test
    public void complexTestInt16() {
        complexTest(testSchema.getColumn("x16"), testSchema.getColumn("y16"));
    }

    @Test
    public void complexTestInt32() {
        complexTest(testSchema.getColumn("x32"), testSchema.getColumn("y32"));
    }

    @Test
    public void complexTestInt64() {
        complexTest(testSchema.getColumn("x64"), testSchema.getColumn("y64"));
    }

    private void complexTest(ColumnSchema x, ColumnSchema y) {

        ColumnSchema key1 = testSchema.getColumn("key1");

        // Will be NONE because colum key1 has nullable=false, because it's a primary key:
        KuduPredicate key1None = KuduPredicate.newIsNullPredicate(key1);

        // Some valid predicates:
        KuduPredicate x1 = KuduPredicate.newComparisonPredicate(x, ComparisonOp.GREATER, maxIntValue(x) - 50);
        KuduPredicate x2 = KuduPredicate.newComparisonPredicate(x, ComparisonOp.LESS, maxIntValue(x) - 40);
        KuduPredicate x3 = KuduPredicate.newIsNullPredicate(x); // x is nullable column so this is valid
        KuduPredicate y1 = KuduPredicate.newComparisonPredicate(y, ComparisonOp.GREATER_EQUAL, maxIntValue(y));
        KuduPredicate y2 = KuduPredicate.newComparisonPredicate(y, ComparisonOp.LESS_EQUAL, minIntValue(y));

        // Will be NONE due to wanted values out of type range, so there are no such records:
        KuduPredicate yMaxNone = KuduPredicate.newComparisonPredicate(y, ComparisonOp.GREATER, maxIntValue(y));
        KuduPredicate yMinNone = KuduPredicate.newComparisonPredicate(y, ComparisonOp.LESS, minIntValue(y));

        pruner = createPruner(
                or(
                        and(x1, x2), // valid
                        and(x2, key1None), // reduces to NONE
                        and(x3, y1), // valid
                        and(x2, y2), // valid
                        and(y1, yMinNone), // reduces to NONE
                        and(x3, yMaxNone), // reduced to NONE
                        and(yMinNone), // test reducing single NONE
                        and(x1, x2, key1None, x3, y1, yMinNone, yMaxNone) // test reducing many NONEs
                )
        );

        List<List<KuduPredicate>> expected = or(and(x1, x2), and(x3, y1), and(x2, y2));
        List<List<KuduPredicate>> actual = pruner.reduceNonePredicates();
        assertEquals(expected, actual);
        assertFalse(pruner.isNone());
    }

}
