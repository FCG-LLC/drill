package cs.drill.ipfun.appname;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;

import javax.inject.Inject;

public final class ApplicationName {

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

  @FunctionTemplate(
    name = "application_name",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class ApplicationNameFunctionWithPort implements DrillSimpleFunc {
    @Param BigIntHolder ip1;
    @Param BigIntHolder ip2;
    @Param IntHolder port;
    @Output NullableVarCharHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      //NOP
    }

    @Override
    public void eval() {
      // doesn't check for nulls since NULL_IF_NULL policy
      String applicationName = cs.drill.ipfun.appname.ApplicationNameResolver.getApplicationName(ip1.value, ip2.value, port.value);
      cs.drill.ipfun.appname.ApplicationName.write(out, buffer, applicationName);
    }
  }

  public static boolean isNullInput(
      NullableBigIntHolder input1,
      NullableBigIntHolder input2,
      NullableIntHolder input3
  ) {
    return input1.isSet * input2.isSet * input3.isSet == 0;
  }

  /**
   * {@link ApplicationNameFunction} for nullable input.
   */
  @FunctionTemplate(
    name = "application_name",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.INTERNAL
  )
  public static class NullableApplicationNameFunctionWithPort implements DrillSimpleFunc {
    @Param NullableBigIntHolder ip1;
    @Param NullableBigIntHolder ip2;
    @Param NullableIntHolder port;
    @Output NullableVarCharHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      //NOP
    }

    @Override
    public void eval() {
      if (cs.drill.ipfun.appname.ApplicationName.isNullInput(ip1, ip2, port)) {
        return;
      }

      String applicationName = cs.drill.ipfun.appname.ApplicationNameResolver.getApplicationName(ip1.value, ip2.value, port.value);
      cs.drill.ipfun.appname.ApplicationName.write(out, buffer, applicationName);
    }
  }

  @FunctionTemplate(
    name = "application_name",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class ApplicationNameFunction implements DrillSimpleFunc {
    @Param BigIntHolder ip1;
    @Param BigIntHolder ip2;
    @Output NullableVarCharHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      //NOP
    }

    @Override
    public void eval() {
      // doesn't check for nulls since NULL_IF_NULL policy
      String applicationName = cs.drill.ipfun.appname.ApplicationNameResolver.getApplicationName(ip1.value, ip2.value);
      cs.drill.ipfun.appname.ApplicationName.write(out, buffer, applicationName);
    }
  }

  public static boolean isNullInput(
    NullableBigIntHolder input1,
    NullableBigIntHolder input2
  ) {
    return input1.isSet * input2.isSet == 0;
  }

  /**
   * {@link ApplicationNameFunction} for nullable input.
   */
  @FunctionTemplate(
    name = "application_name",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.INTERNAL
  )
  public static class NullableApplicationNameFunction implements DrillSimpleFunc {
    @Param NullableBigIntHolder ip1;
    @Param NullableBigIntHolder ip2;
    @Output NullableVarCharHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      //NOP
    }

    @Override
    public void eval() {
      if (cs.drill.ipfun.appname.ApplicationName.isNullInput(ip1, ip2)) {
        return;
      }

      String applicationName = cs.drill.ipfun.appname.ApplicationNameResolver.getApplicationName(ip1.value, ip2.value);
      cs.drill.ipfun.appname.ApplicationName.write(out, buffer, applicationName);
    }
  }

  private ApplicationName() {}
}
