package org.apache.drill.store.kudu;

import org.apache.drill.exec.store.kudu.KuduUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class KuduUtilsUnitTest {

    private KuduUtils kuduUtils;
    private Schema testSchema;

    private Schema createTestSchema() {
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key1", Type.INT32).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("x8", Type.INT8).key(false).nullable(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("str",  Type.STRING).nullable(true).build());
        Schema schema = new Schema(columns);
        return schema;
    }

    private ColumnSchema getNotNullableColumn(Schema schema) {
        ColumnSchema primKey = schema.getPrimaryKeyColumns().get(0);
        assertFalse("test prerequisite failed: we need nullable=false column for test", primKey.isNullable());
        return primKey;
    }

   @Before
    public void setUp() {
        testSchema = createTestSchema();
        kuduUtils = spy(new KuduUtils());
    }

    @Test
    public void isNoneReturnsTrueForNone() {
        // We know that Kudu Client lib will optimize isNull(not-nullable-column) to NONE:
        KuduPredicate nonePredicate = KuduPredicate.newIsNullPredicate(getNotNullableColumn(testSchema));
        assertTrue(kuduUtils.isNone(nonePredicate));
    }

    @Test
    public void isNoneReturnsFalseForNotNone() {
       // test some normal predicate like x=1:
       KuduPredicate notNonePredicate = KuduPredicate.newComparisonPredicate(
               testSchema.getColumn("x8"), KuduPredicate.ComparisonOp.EQUAL, 1);
       assertFalse(kuduUtils.isNone(notNonePredicate));
    }

    @Test
    public void isNoneReturnsFalseForStrNone() {
       // Since we rely on "NONE" string then let's test a str="NONE" predicate just in case:
       KuduPredicate notNonePredicate = KuduPredicate.newComparisonPredicate(
               testSchema.getColumn("str"), KuduPredicate.ComparisonOp.EQUAL, "NONE");
       assertFalse(kuduUtils.isNone(notNonePredicate));
    }

    @Test
    public void findNoneReturnsNullForEmptyList() {
        assertNull(kuduUtils.findFirstNone(new ArrayList<>()));
    }

    @Test
    public void findNoneReturnsFirstNone() {
        List<KuduPredicate> permutation = new ArrayList<>();
        KuduPredicate nonePred = mock(KuduPredicate.class);
        KuduPredicate notNonePred = mock(KuduPredicate.class);
        doReturn(true).when(kuduUtils).isNone(nonePred);
        doReturn(false).when(kuduUtils).isNone(notNonePred);
        permutation.add(notNonePred);
        permutation.add(nonePred);
        assertSame(nonePred, kuduUtils.findFirstNone(permutation));
    }

}
