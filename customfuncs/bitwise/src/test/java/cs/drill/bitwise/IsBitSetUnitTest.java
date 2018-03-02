package cs.drill.bitwise;

import org.apache.commons.lang.ArrayUtils;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.holders.*;
import static org.junit.Assert.assertSame;
import org.junit.Before;
import org.junit.Test;

public class IsBitSetUnitTest {
    private static final TestCase[] TESTCASES = new TestCase[] {
            // number, bitNumber, expected
            new TestCase(0, 0, false),
            new TestCase(1, 0, true),
            new TestCase(3, 0, true),
            new TestCase(3, 1, true),
            new TestCase(2, 0, false),
            new TestCase(2, 1, true)
    };
    private static final TestCase[] BIG_INT_TESTCASES = new TestCase[] {
            new TestCase(Long.MIN_VALUE, 3, false),
            new TestCase(Long.MIN_VALUE, 63, true),
            new TestCase(-1, 64, false) // out of scope
    };
    private static final TestCase[] INT_TESTCASES = new TestCase[] {
            new TestCase(Integer.MIN_VALUE, 3, false),
            new TestCase(Integer.MIN_VALUE, 31, true),
            new TestCase(-1, 32, false) // out of scope
    };
    private static final TestCase[] SMALL_INT_TESTCASES = new TestCase[] {
            new TestCase(Short.MIN_VALUE, 3, false),
            new TestCase(Short.MIN_VALUE, 15, true),
            new TestCase(-1, 16, false) // out of scope
    };
    private static final TestCase[] TINY_INT_TESTCASES = new TestCase[] {
            new TestCase(Byte.MIN_VALUE, 3, false),
            new TestCase(Byte.MIN_VALUE, 7, true),
            new TestCase(-1, 8, false) // out of scope
    };

    private static class TestCase {
        long number;
        byte bitNumber;
        boolean expected;

        TestCase(long number, int bitNumber, boolean expected) {
            this.number = number;
            this.bitNumber = (byte) bitNumber;
            this.expected = expected;
        }
    }

    TinyIntHolder bitIndexHolder;
    BitHolder outHolder;

    @Before
    public void setUp() {
        bitIndexHolder = new TinyIntHolder();
        outHolder = new BitHolder();
    }

    private void testFuncEval(DrillSimpleFunc func, byte bitIndex, boolean expected) {
        bitIndexHolder.value = bitIndex;
        func.eval();
        assertSame("bit " + bitIndex, expected, outHolder.value == 1);
    }

    @Test
    public void supportsBingInts() {
        IsBitSet.IsBitSetOfBigIntsFunction func = new IsBitSet.IsBitSetOfBigIntsFunction();
        BigIntHolder number = new BigIntHolder();
        func.number = number;
        func.bitIndex = bitIndexHolder;
        func.out = outHolder;

        for (TestCase testCase : (TestCase[]) ArrayUtils.addAll(TESTCASES, BIG_INT_TESTCASES)) {
            number.value = testCase.number;
            testFuncEval(func, testCase.bitNumber, testCase.expected);
        }
    }

    @Test
    public void supportsInts() {
        IsBitSet.IsBitSetOfIntsFunction func = new IsBitSet.IsBitSetOfIntsFunction();
        IntHolder number = new IntHolder();
        func.number = number;
        func.bitIndex = bitIndexHolder;
        func.out = outHolder;

        for (TestCase testCase : (TestCase[]) ArrayUtils.addAll(TESTCASES, INT_TESTCASES)) {
            number.value = (int) testCase.number;
            testFuncEval(func, testCase.bitNumber, testCase.expected);
        }
    }

    @Test
    public void supportsSmallInts() {
        IsBitSet.IsBitSetOfSmallIntsFunction func = new IsBitSet.IsBitSetOfSmallIntsFunction();
        SmallIntHolder number = new SmallIntHolder();
        func.number = number;
        func.bitIndex = bitIndexHolder;
        func.out = outHolder;

        for (TestCase testCase : (TestCase[]) ArrayUtils.addAll(TESTCASES, SMALL_INT_TESTCASES)) {
            number.value = (short) testCase.number;
            testFuncEval(func, testCase.bitNumber, testCase.expected);
        }
    }

    @Test
    public void supportsTinyInts() {
        IsBitSet.IsBitSetOfTinyIntsFunction func = new IsBitSet.IsBitSetOfTinyIntsFunction();
        TinyIntHolder number = new TinyIntHolder();
        func.number = number;
        func.bitIndex = bitIndexHolder;
        func.out = outHolder;

        for (TestCase testCase : (TestCase[]) ArrayUtils.addAll(TESTCASES, TINY_INT_TESTCASES)) {
            number.value = (byte) testCase.number;
            testFuncEval(func, testCase.bitNumber, testCase.expected);
        }
    }
}
