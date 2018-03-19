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

public final class RouterName {
  @FunctionTemplate(
    name = "router_name",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class RouterNameFunction implements DrillSimpleFunc {
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
      String routerName = cs.drill.topdisco.TopdiscoReader.getRouterName(ip1.value, ip2.value);
      cs.drill.util.OutputWriter.write(out, buffer, routerName);
    }
  }

  @FunctionTemplate(
    name = "router_name",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class NullableRouterNameFunction implements DrillSimpleFunc {
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
      String routerName = cs.drill.topdisco.TopdiscoReader.getRouterName(ip1.value, ip2.value);
      cs.drill.util.OutputWriter.write(out, buffer, routerName);
    }
  }
}
