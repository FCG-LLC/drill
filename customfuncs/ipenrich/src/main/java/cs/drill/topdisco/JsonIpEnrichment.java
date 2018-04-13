package cs.drill.topdisco;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.List;
import java.util.Set;

@AllArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonIpEnrichment {
  private final List<Ip> ips;
  private final List<Interface> interfaces;

  @AllArgsConstructor
  @Getter
  @ToString
  @EqualsAndHashCode
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Ip {
    private final String ip;
    private final String name;

    /**
     * Entry types:
     * 0 = SNMP device name
     * 1 = dns name from device
     * 2 = neighbor monitoring name
     * 3 = dns record entry
     *
     * Postgres type: smallint
     */
    private final short entryType;
  }

  @AllArgsConstructor
  @Getter
  @ToString
  @EqualsAndHashCode
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Interface {
    public final String port;
    public final int index;
    public final Set<String> ips;
  }
}
