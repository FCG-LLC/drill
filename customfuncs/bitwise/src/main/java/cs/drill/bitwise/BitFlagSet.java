package cs.drill.bitwise;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;

public final class BitFlagSet {
    @FunctionTemplate(
            name = "bit_flag_set",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class BitAndOfBigIntsFunction implements DrillSimpleFunc {
        @Param BigIntHolder left;
        @Param BigIntHolder right;
        @Output BitHolder out;

        @Override
        public void setup() {
            //NOP
        }

        @Override
        public void eval() {
            out.value = (left.value & right.value) > 0 ? 1 : 0;
        }
    }

    @FunctionTemplate(
            name = "bit_flag_set",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class BitAndOfIntsFunction implements DrillSimpleFunc {
        @Param IntHolder left;
        @Param IntHolder right;
        @Output BitHolder out;

        @Override
        public void setup() {
            //NOP
        }

        @Override
        public void eval() {
            out.value = (left.value & right.value) > 0 ? 1 : 0;
        }
    }

    @FunctionTemplate(
            name = "bit_flag_set",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class BitAndOfSmallIntsFunction implements DrillSimpleFunc {
        @Param SmallIntHolder left;
        @Param SmallIntHolder right;
        @Output BitHolder out;

        @Override
        public void setup() {
            //NOP
        }

        @Override
        public void eval() {
            out.value = (left.value & right.value) > 0 ? 1 : 0;
        }
    }

    @FunctionTemplate(
            name = "bit_flag_set",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class BitAndOfTinyIntsFunction implements DrillSimpleFunc {
        @Param TinyIntHolder left;
        @Param TinyIntHolder right;
        @Output BitHolder out;

        @Override
        public void setup() {
            //NOP
        }

        @Override
        public void eval() {
            out.value = (left.value & right.value) > 0 ? 1 : 0;
        }
    }

    private BitFlagSet() {}
}
