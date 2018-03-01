package cs.drill.util;

import lombok.Getter;

@Getter
public class SubnetV4 implements Subnet {
  private long address;
  private long mask;

  public SubnetV4(String address, int maskLength) {
    this.mask = get32Mask(maskLength);
    this.address = this.mask & IpUtil.getLongIpV4Address(address);
  }
}
