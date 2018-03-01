package cs.drill.util;

import java.util.HashMap;
import java.util.Map;

public class HardIpCache<T> implements IpCache<T> {
  private Map<Long, Map<Long, T>> map = new HashMap<>();

  public T get(long ip1, long ip2) {
    Map<Long, T> inner = map.get(ip1);
    return inner == null ? null : inner.get(ip2);
  }

  public void put(long ip1, long ip2, T value) {
    Map<Long, T> inner = map.computeIfAbsent(ip1, k -> new HashMap<>());
    inner.put(ip2, value);
  }
}
