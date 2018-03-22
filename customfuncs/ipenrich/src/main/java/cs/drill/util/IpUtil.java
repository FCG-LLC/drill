package cs.drill.util;

import lombok.Value;
import org.apache.commons.lang3.StringUtils;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

public final class IpUtil {
  public static final long WKP = 0x0064ff9b00000000L;
  private static final String IPV6_SPLIT = ":";

  public static long getLongIpV4Address(String address) {
    String[] parts = address.split("\\.");
    long lowBits = (Long.parseLong(parts[0]) << 24)
      + (Long.parseLong(parts[1]) << 16)
      + (Long.parseLong(parts[2]) << 8)
      + Long.parseLong(parts[3]);
    return lowBits;
  }

  public static IpPair getLongsIpV6Address(String ip) {
    long[] numbers = getNumbers(ip);

    long highBits = numbers[0];
    for (int i = 1; i < 4; i++) {
      highBits = (highBits << 16) + numbers[i];
    }

    long lowBits = numbers[4];
    for (int i = 5; i < 8; i++) {
      lowBits = (lowBits << 16) + numbers[i];
    }

    return new IpPair(highBits, lowBits);
  }

  public static IpPair parseIp(String ip) {
    try {
      InetAddress inetAddress = InetAddress.getByName(ip);
      if (inetAddress instanceof Inet4Address) {
        long ip2 = IpUtil.getLongIpV4Address(ip);
        return new IpPair(IpUtil.WKP, ip2);
      } else if (inetAddress instanceof Inet6Address) {
        return IpUtil.getLongsIpV6Address(ip);
      } else {
        throw new UnknownHostException();
      }
    } catch (UnknownHostException exc) {
      return null;
    }
  }

  private static long[] getNumbers(String ip) {
    long[] numbers = new long[8];
    int semicolonsCount = StringUtils.countMatches(ip, IPV6_SPLIT);
    if (semicolonsCount < 7) {
      int doubleSemicolonIndex = StringUtils.indexOf(ip, IPV6_SPLIT + IPV6_SPLIT);
      for (int i = 0; i < 7 - semicolonsCount; i++) {
        ip = insertStringAtPosition(ip, IPV6_SPLIT, doubleSemicolonIndex);
      }
    }
    String[] parts = ip.split(IPV6_SPLIT);
    for (int i = 0; i < parts.length; i++) {
      if (parts[i].isEmpty()) {
        continue;
      }
      numbers[i] = Long.parseLong(parts[i], 16);
    }
    return numbers;
  }

  private static String insertStringAtPosition(String string, String insertedString, int index) {
    StringBuilder sb = new StringBuilder(string);
    sb.insert(index, insertedString);
    return sb.toString();
  }

  @Value
  public static class IpPair {
    private long highBits;
    private long lowBits;

    public boolean isIp4() {
      return highBits == WKP;
    }
  }

  private IpUtil() {}
}
