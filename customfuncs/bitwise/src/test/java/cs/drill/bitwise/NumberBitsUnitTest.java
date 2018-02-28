package cs.drill.bitwise;

import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.SmallIntHolder;
import org.apache.drill.exec.expr.holders.TinyIntHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.TinyIntWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NumberBitsUnitTest {
    @Mock BaseWriter.ComplexWriter writer;
    @Mock BaseWriter.ListWriter list;
    @Mock TinyIntWriter tinyIntWriter;

    private static long numberWithBits(int... bits) {
        long out = 0;
        for (int bit : bits) {
            out |= 1L << bit;
        }
        return out;
    }

    @Before
    public void setUp() {
        when(writer.rootAsList()).thenReturn(list);
        when(list.tinyInt()).thenReturn(tinyIntWriter);
    }

    @Test
    public void bigIntIsMappedCorrectly() {
        NumberBits.NumberBitsOfBigIntFunction func = new NumberBits.NumberBitsOfBigIntFunction();
        func.number = new BigIntHolder();
        func.number.value = numberWithBits(0, 2, 63, 64); // 63 - max index, 64 - ignored
        func.out = writer;

        func.eval();

        verify(tinyIntWriter).writeTinyInt((byte) 0);
        verify(tinyIntWriter).writeTinyInt((byte) 2);
        verify(tinyIntWriter).writeTinyInt((byte) 63);
        verifyNoMoreInteractions(tinyIntWriter);
    }

    @Test
    public void intIsMappedCorrectly() {
        NumberBits.NumberBitsOfIntFunction func = new NumberBits.NumberBitsOfIntFunction();
        func.number = new IntHolder();
        func.number.value = (int) numberWithBits(0, 2, 31, 32); // 31 - max index, 32 - ignored
        func.out = writer;

        func.eval();

        verify(tinyIntWriter).writeTinyInt((byte) 0);
        verify(tinyIntWriter).writeTinyInt((byte) 2);
        verify(tinyIntWriter).writeTinyInt((byte) 31);
        verifyNoMoreInteractions(tinyIntWriter);
    }

    @Test
    public void smallIntIsMappedCorrectly() {
        NumberBits.NumberBitsOfSmallIntFunction func = new NumberBits.NumberBitsOfSmallIntFunction();
        func.number = new SmallIntHolder();
        func.number.value = (short) numberWithBits(0, 2, 15, 16); // 15 - max index, 16 - ignored
        func.out = writer;

        func.eval();

        verify(tinyIntWriter).writeTinyInt((byte) 0);
        verify(tinyIntWriter).writeTinyInt((byte) 2);
        verify(tinyIntWriter).writeTinyInt((byte) 15);
        verifyNoMoreInteractions(tinyIntWriter);
    }

    @Test
    public void tinyIntIsMappedCorrectly() {
        NumberBits.NumberBitsOfTinyIntFunction func = new NumberBits.NumberBitsOfTinyIntFunction();
        func.number = new TinyIntHolder();
        func.number.value = (byte) numberWithBits(0, 2, 7, 8); // 7 - max index, 8 - ignored
        func.out = writer;

        func.eval();

        verify(tinyIntWriter).writeTinyInt((byte) 0);
        verify(tinyIntWriter).writeTinyInt((byte) 2);
        verify(tinyIntWriter).writeTinyInt((byte) 7);
        verifyNoMoreInteractions(tinyIntWriter);
    }
}
