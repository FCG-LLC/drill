package cs.drill.topdisco;

import cs.drill.util.IpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public final class TopdiscoReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopdiscoReader.class);
  private static final Map<Long, String> IP4_NAMES = new HashMap<>();
  private static final Map<Long, Map<Long, String>> IP6_NAMES = new HashMap<>();
  private static final Map<Long, String> IP4_ROUTER_NAMES = new HashMap<>();
  private static final Map<Long, Map<Long, String>> IP6_ROUTER_NAMES = new HashMap<>();
  private static final Map<Long, Map<Integer, String>> IP4_INTERFACE_NAMES = new HashMap<>();
  private static final Map<Long, Map<Long, Map<Integer, String>>> IP6_INTERFACE_NAMES = new HashMap<>();

  static {
    TopdiscoIpEnrichmentManager.getInstance().consumer = TopdiscoReader::populate;
  }

  static void clear() {
    IP4_NAMES.clear();
    IP6_NAMES.clear();
    IP4_ROUTER_NAMES.clear();
    IP6_ROUTER_NAMES.clear();
    IP4_INTERFACE_NAMES.clear();
    IP6_INTERFACE_NAMES.clear();
  }

  static void populate(JsonIpEnrichment json) {
    clear();

    if (json.getIps() != null) {
      for (JsonIpEnrichment.Ip entity : json.getIps()) {
        populateIpEntity(entity);
      }
    }

    if (json.getInterfaces() != null) {
      for (JsonIpEnrichment.Interface entity : json.getInterfaces()) {
        populateInterfaceEntity(entity);
      }
    }
  }

  static void populateIpEntity(JsonIpEnrichment.Ip entity) {
    IpUtil.IpPair ip = IpUtil.parseIp(entity.getIp());
    if (ip == null) {
      LOGGER.warn("Unknown ip from Topdisco ip enrichment received: " + entity.getIp());
      return;
    }
    populateIpEntityName(entity, ip);
    populateIpEntityRouterName(entity, ip);
  }

  static void populateIpEntityName(JsonIpEnrichment.Ip entity, IpUtil.IpPair ip) {
    if (ip.isIp4()) {
      IP4_NAMES.put(ip.getLowBits(), entity.getName());
    } else {
      Map<Long, String> submap = IP6_NAMES.computeIfAbsent(ip.getHighBits(), k -> new HashMap<>());
      submap.put(ip.getLowBits(), entity.getName());
    }
  }

  static void populateIpEntityRouterName(JsonIpEnrichment.Ip entity, IpUtil.IpPair ip) {
    // only entryType 0 (snmp from device table) and 1 (dns names) are taken
    if (entity.getEntryType() >= 2) return;

    if (ip.isIp4()) {
      IP4_ROUTER_NAMES.put(ip.getLowBits(), entity.getName());
    } else {
      Map<Long, String> submap = IP6_ROUTER_NAMES.computeIfAbsent(ip.getHighBits(), k -> new HashMap<>());
      submap.put(ip.getLowBits(), entity.getName());
    }
  }

  static void populateInterfaceEntity(JsonIpEnrichment.Interface entity) {
    String port = entity.getPort();
    int index = entity.getIndex();
    for (String ip : entity.getIps()) {
      populateInterface(port, index, ip);
    }
  }

  static void populateInterface(String port, int index, String ip) {
    IpUtil.IpPair ipPair = IpUtil.parseIp(ip);
    if (ipPair == null) {
      LOGGER.warn("Unknown ip from Topdisco ip enrichment received: " + ip);
      return;
    }

    Map<Long, Map<Integer, String>> lowBitsMap;
    if (ipPair.isIp4()) {
      lowBitsMap = IP4_INTERFACE_NAMES;
    } else {
      lowBitsMap = IP6_INTERFACE_NAMES.computeIfAbsent(ipPair.getHighBits(), k -> new HashMap<>());
    }

    Map<Integer, String> interfacesMap = lowBitsMap.computeIfAbsent(ipPair.getLowBits(), k -> new HashMap<>());
    interfacesMap.put(index, port);
  }

  public static String getIpName(long ip1, long ip2) {
    if (ip1 == IpUtil.WKP) {
      return IP4_NAMES.get(ip2);
    } else {
      Map<Long, String> submap = IP6_NAMES.get(ip1);
      return submap == null ? null : submap.get(ip2);
    }
  }

  public static String getRouterName(long ip1, long ip2) {
    if (ip1 == IpUtil.WKP) {
      return IP4_ROUTER_NAMES.get(ip2);
    } else {
      Map<Long, String> submap = IP6_ROUTER_NAMES.get(ip1);
      return submap == null ? null : submap.get(ip2);
    }
  }

  public static String getInterfaceName(long ip1, long ip2, int interfaceNumber) {
    Map<Long, Map<Integer, String>> ip4Map = ip1 == IpUtil.WKP ? IP4_INTERFACE_NAMES : IP6_INTERFACE_NAMES.get(ip1);
    if (ip4Map == null) return null;
    Map<Integer, String> interfacesMap = ip4Map.get(ip2);
    if (interfacesMap == null) return null;
    return interfacesMap.get(interfaceNumber);
  }
}
