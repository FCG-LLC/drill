package cs.drill.ipfun.appname;

import cs.drill.toucan.JsonAppEnrichment;
import cs.drill.util.IpUtil;
import lombok.AllArgsConstructor;
import lombok.ToString;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import org.junit.Before;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;

public class ApplicationNameResolverIntegrationTest {
  @ToString(exclude = {"validIps", "invalidIps"})
  @AllArgsConstructor
  static class NameTestCase {
    String subnet;
    String name;
    String[] validIps;
    String[] invalidIps;
  }

  @ToString(exclude = {"validAddresses", "invalidAddresses"})
  @AllArgsConstructor
  static class PortTestCase {
    Integer port;
    String name;
    String[] validAddresses;
    String[] invalidAddresses;
  }

  private static NameTestCase[] NAME_TEST_CASES = new NameTestCase[] {
    new NameTestCase(
      "11.2.0.0/16", "n1",
      new String[] {"11.2.0.0", "11.2.1.1", "11.2.255.255"},
      new String[] {"11.1.0.0", "11.3.0.0"}),
    new NameTestCase(
      "10.3.3.1/16", "n2",
      new String[] {"10.3.0.0", "10.3.3.9", "10.3.255.255"},
      new String[] {"10.2.255.255", "10.4.3.9", "10.4.0.0"}),
    new NameTestCase(
      "172.0.0.0/4", "n3",
      new String[] {"160.0.0.0", "165.1.4.1", "170.0.0.5", "175.255.255.255"},
      new String[] {"159.255.255.255", "176.0.0.0"})
  };
  private static PortTestCase[] PORT_TEST_CASES = new PortTestCase[] {
    new PortTestCase(
      8034, "p1",
      new String[] {"13.12.3.1:8034", "1.0.0.0:8034"},
      new String[] {"13.12.3.1:8035", "10.3.3.9:8034" /* should be mapped to n2 */})
  };

  @Before
  public void populate() {
    LinkedHashMap<String, String> names = new LinkedHashMap<>();
    for (NameTestCase testCase : NAME_TEST_CASES) {
      names.put(testCase.subnet, testCase.name);
    }

    LinkedHashMap<Integer, String> ports = new LinkedHashMap<>();
    for (PortTestCase testCase : PORT_TEST_CASES) {
      ports.put(testCase.port, testCase.name);
    }

    JsonAppEnrichment json = new JsonAppEnrichment(names, ports);

    ApplicationNameResolver.clear();
    ApplicationNameResolver.lastJson = null;
    ApplicationNameResolver.populate(json);
  }

  @After
  public void clear() {
    ApplicationNameResolver.clear();
    ApplicationNameResolver.lastJson = null;
  }

  private long[] parseAddress(String address) {
    String[] ipAndPort = address.split(":");
    String ip = ipAndPort[0];
    Integer port = ipAndPort.length > 1 ? Integer.valueOf(ipAndPort[1]) : -1;
    try {
      InetAddress inetAddress = InetAddress.getByName(ip);

      if (inetAddress instanceof Inet4Address) {
        long ip2 = IpUtil.getLongIpV4Address(ip);
        return new long[] {IpUtil.WKP, ip2, port};
      } else if (inetAddress instanceof Inet6Address) {
        IpUtil.IpPair ipPair = IpUtil.getLongsIpV6Address(ip);
        return new long[] {ipPair.getHighBits(), ipPair.getLowBits(), port};
      } else {
        throw new UnknownHostException();
      }
    } catch (UnknownHostException exc) {
      throw new IllegalArgumentException("Wrong IP address" + ip);
    }
  }

  @Test
  public void findProperNameEnrichment() {
    for (NameTestCase testCase : NAME_TEST_CASES) {
      for (String ip : testCase.validIps) {
        long[] chunks = parseAddress(ip);
        String result = ApplicationNameResolver.getApplicationName(chunks[0], chunks[1]);
        assertEquals("Ip " + ip + " should pass " + testCase, testCase.name, result);
      }
    }
  }

  @Test
  public void excludesInvalidNameEnrichment() {
    for (NameTestCase testCase : NAME_TEST_CASES) {
      for (String ip : testCase.invalidIps) {
        long[] chunks = parseAddress(ip);
        String result = ApplicationNameResolver.getApplicationName(chunks[0], chunks[1]);
        assertNotEquals("Ip " + ip + " should fail " + testCase, testCase.name, result);
      }
    }
  }

  @Test
  public void findProperPortEnrichment() {
    for (PortTestCase testCase : PORT_TEST_CASES) {
      for (String address : testCase.validAddresses) {
        long[] chunks = parseAddress(address);
        String result = ApplicationNameResolver.getApplicationName(chunks[0], chunks[1], (int) chunks[2]);
        assertEquals("Address " + address + " should pass " + testCase, testCase.name, result);
      }
    }
  }

  @Test
  public void excludesInvalidPortEnrichment() {
    for (PortTestCase testCase : PORT_TEST_CASES) {
      for (String address : testCase.invalidAddresses) {
        long[] chunks = parseAddress(address);
        String result = ApplicationNameResolver.getApplicationName(chunks[0], chunks[1], (int) chunks[2]);
        assertNotEquals("Address " + address + " should fail " + testCase, testCase.name, result);
      }
    }
  }
}
