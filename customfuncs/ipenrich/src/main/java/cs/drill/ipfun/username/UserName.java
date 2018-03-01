package cs.drill.ipfun.username;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;

import javax.inject.Inject;

public class UserName {

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

  public static cs.drill.de.UserCacheManager cache = cs.drill.de.UserCacheManager.getInstance();

  @FunctionTemplate(
    name = "user_name",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class UserNameFunction implements DrillSimpleFunc {
    @Param BigIntHolder ip1;
    @Param BigIntHolder ip2;
    @Param BigIntHolder timestamp;
    @Output NullableVarCharHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      // We are not using this method because it's called more than once and gives us nothing more
      // than static initialization of property
    }

    @Override
    public void eval() {
      // doesn't check for nulls since NULL_IF_NULL policy
      String userName = cs.drill.ipfun.username.UserName.cache.getUser(ip1.value, ip2.value, timestamp.value);
      cs.drill.ipfun.username.UserName.write(out, buffer, userName);
    }
  }

  /* Nullable username function for some reason doesn't work. It's better to comment that out than
  to leave because now when someone will try to use this will have meaningful error than just
  random nullpointer without pointing what's wrong. $%^& drill. */
  //  public static boolean isNullInput(
  //    NullableBigIntHolder input1,
  //    NullableBigIntHolder input2
  //  ) {
  //    return input1.isSet * input2.isSet == 0;
  //  }
  //
  //  /**
  //   * {@link UserNameFunction} for nullable input.
  //   */
  //  @FunctionTemplate(
  //    name = "user_name",
  //    scope = FunctionTemplate.FunctionScope.SIMPLE,
  //    nulls = FunctionTemplate.NullHandling.INTERNAL
  //  )
  //  public static class NullableUsernNameFunction implements DrillSimpleFunc {
  //    @Param NullableBigIntHolder ip1;
  //    @Param NullableBigIntHolder ip2;
  //    @Output NullableVarCharHolder out;
  //    @Inject DrillBuf buffer;
  //
  //    @Override
  //    public void setup() {
  //      cs.drill.ipfun.username.UserName.cache.fetchCacheIfNotExist();
  //    }
  //
  //    @Override
  //    public void eval() {
  //      if (cs.drill.ipfun.username.UserName.isNullInput(ip1, ip2)) {
  //        return;
  //      }
  //
  //      String applicationName = cs.drill.ipfun.username.UserNameResolver.getUserName(ip1.value, ip2.value);
  //      cs.drill.ipfun.username.UserName.write(out, buffer, applicationName);
  //    }
  //  }

  private UserName() {}
}
