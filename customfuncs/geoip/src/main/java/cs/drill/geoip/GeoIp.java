package cs.drill.geoip;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;

import javax.inject.Inject;

public final class GeoIp {
  public static void write(NullableVarCharHolder out, DrillBuf buffer, String value) {
    if (value == null) {
      return;
    }
    byte[] bytes = value.getBytes();
    out.buffer = buffer;
    out.start = 0;
    out.end = bytes.length;
    out.isSet = 1;
    buffer.setBytes(0, bytes);
  }

  public static void write(NullableFloat8Holder out, Double value) {
    if (value == null) {
      return;
    }
    out.value = value;
    out.isSet = 1;
  }

  @FunctionTemplate(
      name = "geoip_country",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class GeoIpCountry implements DrillSimpleFunc {
    @Param BigIntHolder ip1;
    @Param BigIntHolder ip2;
    @Output NullableVarCharHolder out;
    @Inject DrillBuf buffer;

    public void setup() {
      // NOP
    }

    public void eval() {
      String result = cs.drill.geoip.GeoIpHelper.getCountry(ip1.value, ip2.value);
      cs.drill.geoip.GeoIp.write(out, buffer, result);
    }
  }

  @FunctionTemplate(
      name = "geoip_city",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class GeoIpCity implements DrillSimpleFunc {
    @Param BigIntHolder ip1;
    @Param BigIntHolder ip2;
    @Output NullableVarCharHolder out;
    @Inject DrillBuf buffer;

    public void setup() {
      // NOP
    }

    public void eval() {
      String result = cs.drill.geoip.GeoIpHelper.getCity(ip1.value, ip2.value);
      cs.drill.geoip.GeoIp.write(out, buffer, result);
    }
  }

  @FunctionTemplate(
      name = "geoip_latitude",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class GeoIpLatitude implements DrillSimpleFunc {
    @Param BigIntHolder ip1;
    @Param BigIntHolder ip2;
    @Output NullableFloat8Holder out;

    public void setup() {
      // NOP
    }

    public void eval() {
      Double result = cs.drill.geoip.GeoIpHelper.getLatitude(ip1.value, ip2.value);
      cs.drill.geoip.GeoIp.write(out, result);
    }
  }

  @FunctionTemplate(
      name = "geoip_longitude",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class GeoIpLongitude implements DrillSimpleFunc {
    @Param BigIntHolder ip1;
    @Param BigIntHolder ip2;
    @Output NullableFloat8Holder out;

    public void setup() {
      // NOP
    }

    public void eval() {
      Double result = cs.drill.geoip.GeoIpHelper.getLongitude(ip1.value, ip2.value);
      cs.drill.geoip.GeoIp.write(out, result);
    }
  }

  public static boolean isNullInput(NullableBigIntHolder input1, NullableBigIntHolder input2) {
    return input1.isSet * input2.isSet == 0;
  }

  @FunctionTemplate(
      name = "geoip_country",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.INTERNAL
  )
  public static class NullableGeoIpCountry implements DrillSimpleFunc {
    @Param NullableBigIntHolder ip1;
    @Param NullableBigIntHolder ip2;
    @Output NullableVarCharHolder out;
    @Inject DrillBuf buffer;

    public void setup() {
      // NOP
    }

    public void eval() {
      if (cs.drill.geoip.GeoIp.isNullInput(ip1, ip2)) {
        return;
      }
      String result = cs.drill.geoip.GeoIpHelper.getCountry(ip1.value, ip2.value);
      cs.drill.geoip.GeoIp.write(out, buffer, result);
    }
  }

  @FunctionTemplate(
      name = "geoip_city",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.INTERNAL
  )
  public static class NullableGeoIpCity implements DrillSimpleFunc {
    @Param NullableBigIntHolder ip1;
    @Param NullableBigIntHolder ip2;
    @Output NullableVarCharHolder out;
    @Inject DrillBuf buffer;

    public void setup() {
      // NOP
    }

    public void eval() {
      if (cs.drill.geoip.GeoIp.isNullInput(ip1, ip2)) {
        return;
      }
      String result = cs.drill.geoip.GeoIpHelper.getCity(ip1.value, ip2.value);
      cs.drill.geoip.GeoIp.write(out, buffer, result);
    }
  }

  @FunctionTemplate(
      name = "geoip_latitude",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.INTERNAL
  )
  public static class NullableGeoIpLatitude implements DrillSimpleFunc {
    @Param NullableBigIntHolder ip1;
    @Param NullableBigIntHolder ip2;
    @Output NullableFloat8Holder out;

    public void setup() {
      // NOP
    }

    public void eval() {
      if (cs.drill.geoip.GeoIp.isNullInput(ip1, ip2)) {
        return;
      }
      Double result = cs.drill.geoip.GeoIpHelper.getLatitude(ip1.value, ip2.value);
      cs.drill.geoip.GeoIp.write(out, result);
    }
  }

  @FunctionTemplate(
      name = "geoip_longitude",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.INTERNAL
  )
  public static class NullableGeoIpLongitude implements DrillSimpleFunc {
    @Param NullableBigIntHolder ip1;
    @Param NullableBigIntHolder ip2;
    @Output NullableFloat8Holder out;

    public void setup() {
      // NOP
    }

    public void eval() {
      if (cs.drill.geoip.GeoIp.isNullInput(ip1, ip2)) {
        return;
      }
      Double result = cs.drill.geoip.GeoIpHelper.getLongitude(ip1.value, ip2.value);
      cs.drill.geoip.GeoIp.write(out, result);
    }
  }

  private GeoIp() {}
}
