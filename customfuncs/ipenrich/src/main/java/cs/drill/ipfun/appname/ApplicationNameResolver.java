package cs.drill.ipfun.appname;

import cs.drill.toucan.JsonAppEnrichment;
import cs.drill.toucan.ToucanAppEnrichmentManager;
import cs.drill.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;

public class ApplicationNameResolver {
  private static final String UNKNOWN_NAME = ""; // empty string is marking ip in cache as not named
  private static Map<SubnetV4, String> ipv4Subnets = new LinkedHashMap<>();
  private static Map<SubnetV6, String> ipv6Subnets = new LinkedHashMap<>();
  private static String[] portNames;
  private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationNameResolver.class);
  private static final ToucanAppEnrichmentManager MANAGER = ToucanAppEnrichmentManager.getInstance();
  /**
   * Contains resolved application names for given IP pairs.
   */
  private static final SoftIpCache<String> cache = new SoftIpCache<>();
  static JsonAppEnrichment lastJson = null;

  static {
    clear();
    MANAGER.consumer = ApplicationNameResolver::populate;
  }

  static void clear() {
    ipv4Subnets.clear();
    ipv6Subnets.clear();
    portNames = new String[49151]; // largest not ephemeral port number
    cache.clear();
  }

  static void populate(JsonAppEnrichment json) {
    if (json.equals(lastJson)) {
      LOGGER.info("No changes detected");
      return;
    }
    LOGGER.info("Populate new app enrichment: " +
      json.getNames().size() + " names and " +
      json.getPorts().size() + " ports");
    lastJson = json;
    clear();
    populateNames(json.getNames());
    populatePortNames(json.getPorts());
  }

  private static void populateNames(Map<String, String> names) {
    for (Map.Entry<String, String> entry : names.entrySet()) {
      String subnet = entry.getKey();
      String applicationName = entry.getValue();

      int index = subnet.indexOf("/");

      String address = subnet.substring(0, index);
      int maskLength = Integer.parseInt(subnet.substring(index + 1));

      try {
        InetAddress inetAddress = InetAddress.getByName(address);

        if (inetAddress instanceof Inet4Address) {
          SubnetV4 subnetV4 = new SubnetV4(address, maskLength);
          ipv4Subnets.put(subnetV4, applicationName);
        } else if (inetAddress instanceof Inet6Address) {
          SubnetV6 subnetV6 = new SubnetV6(address, maskLength);
          ipv6Subnets.put(subnetV6, applicationName);
        } else {
          throw new UnknownHostException();
        }
      } catch (UnknownHostException exc) {
        LOGGER.trace("Wrong IP address `" + address + "`", exc);
      }
    }
  }

  private static void populatePortNames(Map<Integer, String> ports) {
    for (Map.Entry<Integer, String> entry : ports.entrySet()) {
      Integer port = entry.getKey();
      String applicationName = entry.getValue();
      portNames[port] = applicationName;
    }
  }

  public static String getApplicationName(long ip1, long ip2, int port) {
    String cacheValue = cache.get(ip1, ip2);
    if (cacheValue != null) {
      return cacheValue.equals(UNKNOWN_NAME) ? getPortName(port) : cacheValue;
    }

    if (ip1 == IpUtil.WKP) {
      for (Map.Entry<SubnetV4, String> entry : ipv4Subnets.entrySet()) {
        SubnetV4 subnet = entry.getKey();
        if ((subnet.getMask() & ip2) == subnet.getAddress()) {
          cache.put(ip1, ip2, entry.getValue());
          return entry.getValue();
        }
      }
    } else {
      for (Map.Entry<SubnetV6, String> entry : ipv6Subnets.entrySet()) {
        SubnetV6 subnet = entry.getKey();
        if ((subnet.getMaskHighBits() & ip1) == subnet.getAddressHighBits()
          && (subnet.getMaskLowBits() & ip2) == subnet.getAddressLowBits()) {
          cache.put(ip1, ip2, entry.getValue());
          return entry.getValue();
        }
      }
    }

    cache.put(ip1, ip2, UNKNOWN_NAME);
    return getPortName(port);
  }

  public static String getApplicationName(long ip1, long ip2) {
    return getApplicationName(ip1, ip2, -1);
  }

  public static String getPortName(int port) {
    return port > 0 && port < portNames.length ? portNames[port] : null;
  }
}
