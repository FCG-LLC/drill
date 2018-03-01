package cs.drill.geoip.util;

public interface Subnet {

  /**
   * Gets mask representation for given length and size.
   * @param maskLength length of the mask, cannot surpass maskSize
   * @param maskSize maximum of 64
   * @return mask representation in long value
   */
  default long getMask(int maskLength, int maskSize) {
    long mask = 0L;
    for (int i = maskSize - 1; i >= maskSize - maskLength; i--) {
      mask |= 1L << i;
    }
    return mask;
  }

  default long get64Mask(int maskLength) {
    return getMask(maskLength, Long.SIZE);
  }

  default long get32Mask(int maskLength) {
    return getMask(maskLength, Integer.SIZE);
  }
}
