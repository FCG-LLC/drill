package cs.drill.ipfun.ipname;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;

import javax.inject.Inject;

public final class IpName {
  @FunctionTemplate(
    name = "ip_name",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class IpNameFunction implements DrillSimpleFunc {
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
      String ipName = cs.drill.topdisco.TopdiscoReader.getIpName(ip1.value, ip2.value);
      cs.drill.util.OutputWriter.write(out, buffer, ipName);
    }
  }

  @FunctionTemplate(
    name = "ip_name",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.INTERNAL
  )
  public static class NullableIpNameFunction implements DrillSimpleFunc {
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
      if (ip1.isSet * ip2.isSet == 0) return;
      String ipName = cs.drill.topdisco.TopdiscoReader.getIpName(ip1.value, ip2.value);
      cs.drill.util.OutputWriter.write(out, buffer, ipName);
    }
  }
}
