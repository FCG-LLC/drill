package cs.drill.bitwise;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

public final class NumberBits {
    @FunctionTemplate(
            name = "number_bits",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class NumberBitsOfBigIntFunction implements DrillSimpleFunc {
        private static final byte BITS_WIDTH = BigIntHolder.WIDTH * 8;

        @Param BigIntHolder number;
        @Output BaseWriter.ComplexWriter out;

        @Override
        public void setup() {}

        @Override
        public void eval() {
            long input = number.value;
            BaseWriter.ListWriter list = out.rootAsList();
            list.startList();
            byte i = 0;
            while (input != 0 && i < BITS_WIDTH) {
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
        private static final byte BITS_WIDTH = IntHolder.WIDTH * 8;

        @Param IntHolder number;
        @Output BaseWriter.ComplexWriter out;

        @Override
        public void setup() {}

        @Override
        public void eval() {
            int input = number.value;
            BaseWriter.ListWriter list = out.rootAsList();
            list.startList();
            byte i = 0;
            while (input != 0 && i < BITS_WIDTH) {
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
        private static final byte BITS_WIDTH = SmallIntHolder.WIDTH * 8;

        @Param SmallIntHolder number;
        @Output BaseWriter.ComplexWriter out;

        @Override
        public void setup() {}

        @Override
        public void eval() {
            short input = number.value;
            BaseWriter.ListWriter list = out.rootAsList();
            list.startList();
            byte i = 0;
            while (input != 0 && i < BITS_WIDTH) {
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
        private static final byte BITS_WIDTH = TinyIntHolder.WIDTH * 8;

        @Param TinyIntHolder number;
        @Output BaseWriter.ComplexWriter out;

        @Override
        public void setup() {}

        @Override
        public void eval() {
            byte input = number.value;
            BaseWriter.ListWriter list = out.rootAsList();
            list.startList();
            byte i = 0;
            while (input != 0 && i < BITS_WIDTH) {
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
