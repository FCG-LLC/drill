package cs.drill.geoip.util;

import cs.drill.geoip.IpUtil;
import lombok.Getter;

@Getter
@SuppressWarnings("PMD.SingularField")
public class SubnetV6 implements Subnet {

  private long addressHighBits;
  private long addressLowBits;
  private long maskHighBits;
  private long maskLowBits;

  public SubnetV6(String address, int maskLength) {
    processMask(maskLength);
    processIp(address);
  }

  private void processMask(int maskLength) {
    if (maskLength > Long.SIZE) {
      maskHighBits = get64Mask(Long.SIZE);
      maskLowBits = get64Mask(maskLength - Long.SIZE);
    } else {
      maskHighBits = get64Mask(maskLength);
      maskLowBits = 0L;
    }
  }

  @SuppressWarnings("PMD.ShortVariable")
  private void processIp(String ip) {
    IpUtil.IpPair ipPair = IpUtil.getLongsIpV6Address(ip);
    addressHighBits = this.maskHighBits & ipPair.getHighBits();
    addressLowBits = this.maskLowBits & ipPair.getLowBits();
  }
}
