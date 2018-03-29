package cs.drill.util;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

public class OutputWriter {
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

  public static void write(VarCharHolder out, DrillBuf buffer, String value) {
    byte[] bytes = value.getBytes();
    out.buffer = buffer;
    out.start = 0;
    out.end = bytes.length;
    buffer.setBytes(0, bytes);
  }
}
