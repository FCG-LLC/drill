package cs.drill.bitwise;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;

public final class IsBitSet {
    @FunctionTemplate(
            name = "is_bit_set",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class IsBitSetOfBigIntsFunction implements DrillSimpleFunc {
        @Param BigIntHolder number;
        @Param IntHolder bitIndex; // should be TinyIntHolder but Drill does not support it in SQL
        @Output BitHolder out;

        @Override
        public void setup() {
            //NOP
        }

        @Override
        public void eval() {
            out.value = (number.value >>> bitIndex.value & 1) > 0 && bitIndex.value < 64 ? 1 : 0;
        }
    }

    @FunctionTemplate(
            name = "is_bit_set",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class IsBitSetOfIntsFunction implements DrillSimpleFunc {
        @Param IntHolder number;
        @Param IntHolder bitIndex;
        @Output BitHolder out;

        @Override
        public void setup() {
            //NOP
        }

        @Override
        public void eval() {
            out.value = (number.value >>> bitIndex.value & 1) > 0 && bitIndex.value < 32 ? 1 : 0;
        }
    }

    @FunctionTemplate(
            name = "is_bit_set",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class IsBitSetOfSmallIntsFunction implements DrillSimpleFunc {
        @Param SmallIntHolder number;
        @Param IntHolder bitIndex;
        @Output BitHolder out;

        @Override
        public void setup() {
            //NOP
        }

        @Override
        public void eval() {
            out.value = (number.value >>> bitIndex.value & 1) > 0 && bitIndex.value < 16 ? 1 : 0;
        }
    }

    @FunctionTemplate(
            name = "is_bit_set",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class IsBitSetOfTinyIntsFunction implements DrillSimpleFunc {
        @Param TinyIntHolder number;
        @Param IntHolder bitIndex;
        @Output BitHolder out;

        @Override
        public void setup() {
            //NOP
        }

        @Override
        public void eval() {
            out.value = (number.value >>> bitIndex.value & 1) > 0 && bitIndex.value < 8 ? 1 : 0;
        }
    }

    private IsBitSet() {}
}
