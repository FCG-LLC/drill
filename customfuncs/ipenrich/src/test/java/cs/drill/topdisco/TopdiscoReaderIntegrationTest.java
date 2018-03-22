package cs.drill.topdisco;

import cs.drill.util.IpUtil;
import lombok.AllArgsConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.sql.SQLException;
import java.util.Arrays;

public class TopdiscoReaderIntegrationTest {
  static String IP4_1 = "10.12.1.42";
  static String IP4_2 = "65.19.133.113";
  static String IP6_1 = "2001:470:1:5dd::1";
  static String IP6_2 = "684d:1111:222:3333:4444:5555:6:abcd";
  static String NAME_1 = "firstTestName";
  static String NAME_2 = "secondTestName";
  static String PORT_1 = "firstTestPort";
  static String PORT_2 = "secondTestPort";
  static int INDEX_1 = 4322;
  static int INDEX_2 = 1234;

  static {
    // from the performance reasons we want to make updates made automatically in
    // production code; this makes us harder test these classes
    TopdiscoIpEnrichmentManager.AUTO_RUN = false;
  }

  TopdiscoIpEnrichmentManager manager;
  String json;

  @AllArgsConstructor
  static class JsonIp {
    String ip;
    String name;
    int entryType;

    String toJSON() {
      return "{\"ip\": \"" + ip + "\", \"name\": \"" + name + "\", \"entryType\": " + entryType + "}";
    }
  }

  @AllArgsConstructor
  static class JsonInterface {
    String port;
    int index;
    String[] ips;

    String toJSON() {
      return "{\"port\": \"" + port + "\", \"index\": " + index + ", \"ips\": [\"" + String.join("\", \"", ips) + "\"]}";
    }
  }

  @Before
  public void setUp() throws SQLException {
    // tests don't run on fresh instances, but we don't want to loose performance
    // because of introducing more free access and it helps us detect issues with clearing
    // state between updates
    manager = spy(new TopdiscoIpEnrichmentManager());
    manager.consumer = TopdiscoReader::populate;

    doAnswer(inv -> json).when(manager).fetchData();
  }

  private String produceJson(JsonIp[] ips, JsonInterface[] interfaces) {
    String[] ipsJson = Arrays.stream(ips).map(JsonIp::toJSON).toArray(String[]::new);
    String[] interfacesJson = Arrays.stream(interfaces).map(JsonInterface::toJSON).toArray(String[]::new);
    return "{\"ips\": [" + String.join(", ", ipsJson) + "], \"interfaces\": [" + String.join(", ", interfacesJson) + "]}";
  }

  private String getIpName(String ip) {
    IpUtil.IpPair ipPair = IpUtil.parseIp(ip);
    return TopdiscoReader.getIpName(ipPair.getHighBits(), ipPair.getLowBits());
  }

  private String getRouterName(String ip) {
    IpUtil.IpPair ipPair = IpUtil.parseIp(ip);
    return TopdiscoReader.getRouterName(ipPair.getHighBits(), ipPair.getLowBits());
  }

  private String getInterfaceName(String ip, int interfaceNumber) {
    IpUtil.IpPair ipPair = IpUtil.parseIp(ip);
    return TopdiscoReader.getInterfaceName(ipPair.getHighBits(), ipPair.getLowBits(), interfaceNumber);
  }

  private void populate(JsonIp[] ips, JsonInterface[] interfaces) {
    json = produceJson(ips, interfaces);
    manager.update();
  }

  private void populate(JsonIp... ips) {
    populate(ips, new JsonInterface[0]);
  }

  private void populate(JsonInterface... interfaces) {
    populate(new JsonIp[0], interfaces);
  }

  @Test
  public void returnsPopulatedIp4Name() {
    populate(new JsonIp(IP4_1, NAME_1, 0));
    assertEquals(NAME_1, getIpName(IP4_1));
  }

  @Test
  public void doesNotReturnNotPopulatedIp4Name() {
    populate(new JsonIp(IP4_1, NAME_1, 0));
    assertNull(getIpName(IP4_2));
  }

  @Test
  public void returnsPopulatedIp6Name() {
    populate(new JsonIp(IP6_1, NAME_1, 0));
    assertEquals(NAME_1, getIpName(IP6_1));
  }

  @Test
  public void doesNotReturnNotPopulatedIp6Name() {
    populate(new JsonIp(IP6_1, NAME_1, 0));
    assertNull(getIpName(IP6_2));
  }

  @Test
  public void clearsIpNamesBetweenUpdates() {
    populate(new JsonIp(IP4_1, NAME_1, 0), new JsonIp(IP6_1, NAME_1, 0));
    populate(new JsonIp(IP4_2, NAME_1, 0), new JsonIp(IP6_2, NAME_1, 0));
    assertNull(getIpName(IP4_1));
    assertNull(getIpName(IP6_1));
  }

  @Test
  public void returnsPopulatedIp4EntryTypeZeroAndOneRouterName() {
    populate(new JsonIp(IP4_1, NAME_1, 0), new JsonIp(IP4_2, NAME_2, 1));
    assertEquals(NAME_1, getRouterName(IP4_1));
    assertEquals(NAME_2, getRouterName(IP4_2));
  }

  @Test
  public void doesNotReturnNotPopulatedIp4EntryTypeZeroRouterName() {
    populate(new JsonIp(IP4_1, NAME_1, 0));
    assertNull(getRouterName(IP4_2));
  }

  @Test
  public void doesNotReturnNotPopulatedIp4EntryTypeOneRouterName() {
    populate(new JsonIp(IP4_1, NAME_1, 1));
    assertNull(getRouterName(IP4_2));
  }

  @Test
  public void returnsPopulatedIp6EntryTypeZeroAndOneRouterName() {
    populate(new JsonIp(IP6_1, NAME_1, 0), new JsonIp(IP6_2, NAME_2, 1));
    assertEquals(NAME_1, getRouterName(IP6_1));
    assertEquals(NAME_2, getRouterName(IP6_2));
  }

  @Test
  public void doesNotReturnNotPopulatedIp6EntryTypeZeroRouterName() {
    populate(new JsonIp(IP6_1, NAME_1, 0));
    assertNull(getRouterName(IP6_2));
  }

  @Test
  public void doesNotReturnNotPopulatedIp6EntryTypeOneRouterName() {
    populate(new JsonIp(IP6_1, NAME_1, 1));
    assertNull(getRouterName(IP6_2));
  }

  @Test
  public void doesNotReturnPopulatedIp4EntryTypeThreeRouterName() {
    populate(new JsonIp(IP4_1, NAME_1, 3));
    assertNull(getRouterName(IP4_1));
  }

  @Test
  public void doesNotReturnPopulatedIp6EntryTypeThreeRouterName() {
    populate(new JsonIp(IP6_1, NAME_1, 3));
    assertNull(getRouterName(IP6_1));
  }

  @Test
  public void clearsRouterNamesBetweenUpdates() {
    populate(new JsonIp(IP4_1, NAME_1, 0), new JsonIp(IP6_1, NAME_1, 0));
    populate(new JsonIp(IP4_2, NAME_1, 0), new JsonIp(IP6_2, NAME_1, 0));
    assertNull(getRouterName(IP4_1));
    assertNull(getRouterName(IP6_1));
  }

  @Test
  public void returnsPopulatedInterfaceName() {
    populate(new JsonInterface(PORT_1, INDEX_1, new String[] { IP4_1, IP6_1 }));
    assertEquals(PORT_1, getInterfaceName(IP4_1, INDEX_1));
    assertEquals(PORT_1, getInterfaceName(IP6_1, INDEX_1));
  }

  @Test
  public void doesNotReturnNotPopulatedInterfaceName() {
    populate(new JsonInterface(PORT_2, INDEX_1, new String[] { IP4_1, IP6_1 }));

    // by different ip
    assertNull(getInterfaceName(IP4_2, INDEX_1));
    assertNull(getInterfaceName(IP6_2, INDEX_1));

    // by different index
    assertNull(getInterfaceName(IP4_1, INDEX_2));
    assertNull(getInterfaceName(IP6_1, INDEX_2));
  }

  @Test
  public void clearsInterfaceNamesBetweenUpdates() {
    populate(new JsonInterface(PORT_1, INDEX_1, new String[] { IP4_1, IP6_1 }));
    populate(new JsonInterface(PORT_1, INDEX_1, new String[] { IP4_2, IP6_2 }));
    assertNull(getInterfaceName(IP4_1, INDEX_1));
    assertNull(getInterfaceName(IP6_1, INDEX_1));
  }
}
