package cs.drill.topdisco;

import cs.drill.util.IpUtil;
import cs.drill.util.SoftIpCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.SoftReference;
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
  private static final SoftIpCache<String> IP_STRS = new SoftIpCache<>();
  private static SoftReference<Map<Integer, String>> INTERFACE_STRS = new SoftReference<>(null);

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

  private static String getIpStr(long ip1, long ip2) {
    String str = IP_STRS.get(ip1, ip2);
    if (str == null) {
      str = new IpUtil.IpPair(ip1, ip2).toString();
      IP_STRS.put(ip1, ip2, str);
    }
    return str;
  }

  private static String getInterfaceStr(int interfaceNumber) {
    Map<Integer, String> ref = INTERFACE_STRS.get();
    if (ref == null) {
      ref = new HashMap<>();
      INTERFACE_STRS = new SoftReference<>(ref);
    }
    String str = ref.get(interfaceNumber);
    if (str == null) {
      str = Integer.toString(interfaceNumber);
      ref.put(interfaceNumber, str);
    }
    return str;
  }

  public static String getIpName(long ip1, long ip2) {
    String name;
    if (ip1 == IpUtil.WKP) {
      name = IP4_NAMES.get(ip2);
    } else {
      Map<Long, String> submap = IP6_NAMES.get(ip1);
      name = submap == null ? null : submap.get(ip2);
    }
    return name == null ? getIpStr(ip1, ip2) : name;
  }

  public static String getRouterName(long ip1, long ip2) {
    String name;
    if (ip1 == IpUtil.WKP) {
      name = IP4_ROUTER_NAMES.get(ip2);
    } else {
      Map<Long, String> submap = IP6_ROUTER_NAMES.get(ip1);
      name = submap == null ? null : submap.get(ip2);
    }
    return name == null ? getIpStr(ip1, ip2) : name;
  }

  public static String getInterfaceName(long ip1, long ip2, int interfaceNumber) {
    String name = null;
    Map<Long, Map<Integer, String>> ip4Map = ip1 == IpUtil.WKP ? IP4_INTERFACE_NAMES : IP6_INTERFACE_NAMES.get(ip1);
    if (ip4Map != null) {
      Map<Integer, String> interfacesMap = ip4Map.get(ip2);
      if (interfacesMap != null) {
        name = interfacesMap.get(interfaceNumber);
      }
    }
    return name == null ? getInterfaceStr(interfaceNumber) : name;
  }
}
