package cs.drill.geoip;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class GeoIpUnitTest {
  @RunWith(PowerMockRunner.class)
  @PrepareForTest({NullableVarCharHolder.class, DrillBuf.class, String.class})
  public static class WriteOfString {
    @Mock NullableVarCharHolder out;
    @Mock DrillBuf buffer;
    @Mock String value;

    private void callMethod() {
      GeoIp.write(out, buffer, value);
    }

    @Test
    public void setsGivenBufferAsHolderBuffer() {
      callMethod();
      assertSame(buffer, out.buffer);
    }

    @Test
    public void setsHolderStartToZero() {
      callMethod();
      assertEquals(0, out.start);
    }

    @Test
    public void setsHolderEndToValueBytesLength() {
      value = "abc123";
      callMethod();
      assertEquals(value.getBytes().length, out.end);
    }

    @Test
    public void setsHolderIsSetToOne() {
      callMethod();
      assertEquals(1, out.isSet);
    }

    @Test
    public void callsBufferSetBytesWithGivenValueBytes() {
      value = "debug1";
      callMethod();
      verify(buffer).setBytes(0, value.getBytes());
    }

    @Test
    public void doesNothingOnNullValueGiven() {
      value = null;
      callMethod();
      assertSame(0, out.isSet);
      verify(buffer, never()).setBytes(anyInt(), any(byte[].class));
    }
  }

  @RunWith(PowerMockRunner.class)
  @PrepareForTest({NullableFloat8Holder.class})
  public static class WriteOfDouble {
    @Mock NullableFloat8Holder out;
    Double value = ThreadLocalRandom.current().nextDouble();

    private void callMethod() {
      GeoIp.write(out, value);
    }

    @Test
    public void setsOutValueToGivenValue() {
      callMethod();
      assertEquals(value.doubleValue(), out.value, 0);
    }

    @Test
    public void setsOutIsSetToOne() {
      callMethod();
      assertEquals(1, out.isSet);
    }

    @Test
    public void doesNothingOnNullValue() {
      value = null;
      callMethod();
      assertEquals(0, out.value, 0);
      assertSame(0, out.isSet);
    }
  }

  @RunWith(PowerMockRunner.class)
  @PrepareForTest({
      NullableVarCharHolder.class,
      GeoIpHelper.class,
      DrillBuf.class,
      BigIntHolder.class,
      String.class,
      GeoIp.class})
  public static class GeoIpCountryEval {
    @Mock BigIntHolder ip1;
    @Mock BigIntHolder ip2;
    @Mock NullableVarCharHolder out;
    @Mock DrillBuf buffer;
    @Mock String result;
    long ip1Value = ThreadLocalRandom.current().nextLong();
    long ip2Value = ThreadLocalRandom.current().nextLong();
    GeoIp.GeoIpCountry geoIpCountry;

    @Before
    public void setUp() {
      ip1.value = ip1Value;
      ip2.value = ip2Value;
      geoIpCountry = new GeoIp.GeoIpCountry();
      geoIpCountry.ip1 = ip1;
      geoIpCountry.ip2 = ip2;
      geoIpCountry.out = out;
      geoIpCountry.buffer = buffer;

      PowerMockito.mockStatic(GeoIpHelper.class);
      PowerMockito.when(GeoIpHelper.getCountry(ip1Value, ip2Value)).thenReturn(result);
    }

    @Test
    public void callsWriteWithGeoIpHelperGetCountryResult() {
      PowerMockito.mockStatic(GeoIp.class);
      geoIpCountry.eval();
      PowerMockito.verifyStatic();
      GeoIp.write(out, buffer, result);
    }
  }

  @RunWith(PowerMockRunner.class)
  @PrepareForTest({
      NullableVarCharHolder.class,
      GeoIpHelper.class,
      DrillBuf.class,
      BigIntHolder.class,
      String.class,
      GeoIp.class})
  public static class GeoIpCityEval {
    @Mock BigIntHolder ip1;
    @Mock BigIntHolder ip2;
    @Mock NullableVarCharHolder out;
    @Mock DrillBuf buffer;
    @Mock String result;
    long ip1Value = ThreadLocalRandom.current().nextLong();
    long ip2Value = ThreadLocalRandom.current().nextLong();
    GeoIp.GeoIpCity geoIpCity;

    @Before
    public void setUp() {
      ip1.value = ip1Value;
      ip2.value = ip2Value;
      geoIpCity = new GeoIp.GeoIpCity();
      geoIpCity.ip1 = ip1;
      geoIpCity.ip2 = ip2;
      geoIpCity.out = out;
      geoIpCity.buffer = buffer;

      PowerMockito.mockStatic(GeoIpHelper.class);
      PowerMockito.when(GeoIpHelper.getCity(ip1Value, ip2Value)).thenReturn(result);
    }

    @Test
    public void callsWriteWithGeoIpHelperGetCityResult() {
      PowerMockito.mockStatic(GeoIp.class);
      geoIpCity.eval();
      PowerMockito.verifyStatic();
      GeoIp.write(out, buffer, result);
    }
  }

  @RunWith(PowerMockRunner.class)
  @PrepareForTest({
      NullableFloat8Holder.class,
      GeoIpHelper.class,
      BigIntHolder.class,
      String.class,
      GeoIp.class})
  public static class GeoIpLatitudeEval {
    @Mock BigIntHolder ip1;
    @Mock BigIntHolder ip2;
    @Mock NullableFloat8Holder out;
    Double result = ThreadLocalRandom.current().nextDouble();
    long ip1Value = ThreadLocalRandom.current().nextLong();
    long ip2Value = ThreadLocalRandom.current().nextLong();
    GeoIp.GeoIpLatitude geoIpLatitude;

    @Before
    public void setUp() {
      ip1.value = ip1Value;
      ip2.value = ip2Value;
      geoIpLatitude = new GeoIp.GeoIpLatitude();
      geoIpLatitude.ip1 = ip1;
      geoIpLatitude.ip2 = ip2;
      geoIpLatitude.out = out;

      PowerMockito.mockStatic(GeoIpHelper.class);
      PowerMockito.when(GeoIpHelper.getLatitude(ip1Value, ip2Value)).thenReturn(result);
    }

    @Test
    public void callsWriteWithGeoIpHelperGetLatitudeResult() {
      PowerMockito.mockStatic(GeoIp.class);
      geoIpLatitude.eval();
      PowerMockito.verifyStatic();
      GeoIp.write(out, result);
    }
  }

  @RunWith(PowerMockRunner.class)
  @PrepareForTest({
      NullableFloat8Holder.class,
      GeoIpHelper.class,
      BigIntHolder.class,
      String.class,
      GeoIp.class})
  public static class GeoIpLongitudeEval {
    @Mock BigIntHolder ip1;
    @Mock BigIntHolder ip2;
    @Mock NullableFloat8Holder out;
    Double result = ThreadLocalRandom.current().nextDouble();
    long ip1Value = ThreadLocalRandom.current().nextLong();
    long ip2Value = ThreadLocalRandom.current().nextLong();
    GeoIp.GeoIpLongitude geoIpLongitude;

    @Before
    public void setUp() {
      ip1.value = ip1Value;
      ip2.value = ip2Value;
      geoIpLongitude = new GeoIp.GeoIpLongitude();
      geoIpLongitude.ip1 = ip1;
      geoIpLongitude.ip2 = ip2;
      geoIpLongitude.out = out;

      PowerMockito.mockStatic(GeoIpHelper.class);
      PowerMockito.when(GeoIpHelper.getLongitude(ip1Value, ip2Value)).thenReturn(result);
    }

    @Test
    public void callsWriteWithGeoIpHelperGetLongitudeResult() {
      PowerMockito.mockStatic(GeoIp.class);
      geoIpLongitude.eval();
      PowerMockito.verifyStatic();
      GeoIp.write(out, result);
    }
  }

  public static class GeoIpIsNullInput {
    NullableBigIntHolder in1;
    NullableBigIntHolder in2;

    @Before
    public void setInputs() {
      in1 = new NullableBigIntHolder();
      in1.isSet = 1;
      in2 = new NullableBigIntHolder();
      in2.isSet = 1;
    }

    @Test
    public void returnsFalseIfBothSet() {
      boolean isNull = GeoIp.isNullInput(in1, in2);
      assertFalse(isNull);
    }

    @Test
    public void returnsTrueIfFirstNotSet() {
      in1.isSet = 0;
      boolean isNull = GeoIp.isNullInput(in1, in2);
      assertTrue(isNull);
    }

    @Test
    public void returnsTrueIfSecondNotSet() {
      in2.isSet = 0;
      boolean isNull = GeoIp.isNullInput(in1, in2);
      assertTrue(isNull);
    }

    @Test
    public void returnsTrueIfBothNotSet() {
      in1.isSet = 0;
      in2.isSet = 0;
      boolean isNull = GeoIp.isNullInput(in1, in2);
      assertTrue(isNull);
    }
  }
}
