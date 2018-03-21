package cs.drill.ipfun.ipname;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;

import javax.inject.Inject;

public final class InterfaceName {
  @FunctionTemplate(
    name = "interface_name",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class InterfaceNameFunction implements DrillSimpleFunc {
    @Param BigIntHolder ip1;
    @Param BigIntHolder ip2;
    @Param IntHolder interfaceNumber;
    @Output NullableVarCharHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      //NOP
    }

    @Override
    public void eval() {
      String interfaceName = cs.drill.topdisco.TopdiscoReader.getInterfaceName(ip1.value, ip2.value, interfaceNumber.value);
      cs.drill.util.OutputWriter.write(out, buffer, interfaceName);
    }
  }
}
