package cs.drill.util;

import lombok.Getter;

@Getter
public class SubnetV6 implements Subnet{

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

  private void processIp(String ip) {
    IpUtil.IpPair ipPair = IpUtil.getLongsIpV6Address(ip);
    addressHighBits = maskHighBits & ipPair.getHighBits();
    addressLowBits = maskLowBits & ipPair.getLowBits();
  }
}
