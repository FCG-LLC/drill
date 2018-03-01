package cs.drill.ipfun.username;

import cs.drill.ipfun.appname.ApplicationNameResolver;
import cs.drill.util.FileReader;
import cs.drill.util.HardIpCache;
import cs.drill.util.IpCache;
import cs.drill.util.IpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Deprecated. We left that in case of rollback.
 */
@Deprecated
public class UserNameResolver {

  private static final String FILE_NAME = "user_name.txt";
  private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationNameResolver.class);
  private static IpCache<String> userNamesMap = new HardIpCache<>();

  static {
    populateNames();
  }

  private static void populateNames() {
    FileReader fileReader = new FileReader(FILE_NAME, "\t");
    fileReader.processFile((line) -> {
      if (line.length != 2) {
        LOGGER.warn("Line doesn't have 2 expected columns");
        return;
      }

      String ip = line[0];
      String userName = line[1];

      try {
        InetAddress inetAddress = InetAddress.getByName(ip);

        if (inetAddress instanceof Inet4Address) {
          long ip2 = IpUtil.getLongIpV4Address(ip);
          userNamesMap.put(IpUtil.WKP, ip2, userName);
        } else if (inetAddress instanceof Inet6Address) {
          IpUtil.IpPair ipPair = IpUtil.getLongsIpV6Address(ip);
          userNamesMap.put(ipPair.getHighBits(), ipPair.getLowBits(), userName);
        } else {
          throw new UnknownHostException();
        }
      } catch (UnknownHostException exc) {
        throw new IllegalArgumentException("Wrong IP address" + ip);
      }
    });
  }

  public static String getUserName(long ip1, long ip2) {
    return userNamesMap.get(ip1, ip2);
  }
}
