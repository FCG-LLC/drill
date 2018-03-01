package cs.drill.geoip;

import lombok.Value;
import org.apache.commons.lang3.StringUtils;

@SuppressWarnings("PMD.ShortVariable")
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

  @SuppressWarnings({"PMD.AvoidReassigningParameters", "PMD.LongVariable"})
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
  }

  private IpUtil() {}
}
