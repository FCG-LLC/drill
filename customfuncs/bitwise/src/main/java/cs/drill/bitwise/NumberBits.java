package cs.drill.bitwise;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

public final class NumberBits {
    @FunctionTemplate(
            name = "number_bits",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class NumberBitsOfBigIntFunction implements DrillSimpleFunc {
        @Param BigIntHolder number;
        @Output ComplexWriter out;

        @Override
        public void setup() {}

        @Override
        public void eval() {
            long input = number.value;
            org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter list = out.rootAsList();
            list.startList();
            byte i = 0;
            while (input != 0 && i < 64) {
                if ((input & 1) > 0) {
                    list.tinyInt().writeTinyInt(i);
                }
                input >>>= 1;
                i += 1;
            }
            list.endList();
        }
    }

    @FunctionTemplate(
            name = "number_bits",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class NumberBitsOfIntFunction implements DrillSimpleFunc {
        @Param IntHolder number;
        @Output ComplexWriter out;

        @Override
        public void setup() {}

        @Override
        public void eval() {
            int input = number.value;
            org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter list = out.rootAsList();
            list.startList();
            byte i = 0;
            while (input != 0 && i < 32) {
                if ((input & 1) > 0) {
                    list.tinyInt().writeTinyInt(i);
                }
                input >>>= 1;
                i += 1;
            }
            list.endList();
        }
    }

    @FunctionTemplate(
            name = "number_bits",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class NumberBitsOfSmallIntFunction implements DrillSimpleFunc {
        @Param SmallIntHolder number;
        @Output ComplexWriter out;

        @Override
        public void setup() {}

        @Override
        public void eval() {
            short input = number.value;
            org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter list = out.rootAsList();
            list.startList();
            byte i = 0;
            while (input != 0 && i < 16) {
                if ((input & 1) > 0) {
                    list.tinyInt().writeTinyInt(i);
                }
                input >>>= 1;
                i += 1;
            }
            list.endList();
        }
    }

    @FunctionTemplate(
            name = "number_bits",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class NumberBitsOfTinyIntFunction implements DrillSimpleFunc {
        @Param TinyIntHolder number;
        @Output ComplexWriter out;

        @Override
        public void setup() {}

        @Override
        public void eval() {
            byte input = number.value;
            org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter list = out.rootAsList();
            list.startList();
            byte i = 0;
            while (input != 0 && i < 8) {
                if ((input & 1) > 0) {
                    list.tinyInt().writeTinyInt(i);
                }
                input >>>= 1;
                i += 1;
            }
            list.endList();
        }
    }

    private NumberBits() {}
}
