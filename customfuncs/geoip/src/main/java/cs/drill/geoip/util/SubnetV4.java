package cs.drill.geoip.util;

import cs.drill.geoip.IpUtil;
import lombok.Getter;

@Getter
@SuppressWarnings("PMD.SingularField")
public class SubnetV4 implements Subnet {
  private final long address;
  private final long mask;

  public SubnetV4(String address, int maskLength) {
    this.mask = get32Mask(maskLength);
    this.address = this.mask & IpUtil.getLongIpV4Address(address);
  }
}
