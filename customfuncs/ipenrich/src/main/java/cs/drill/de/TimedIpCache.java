package cs.drill.de;

public interface TimedIpCache<T> {
  T get(long ip1, long ip2, long timestamp);
  void put(long ip1, long ip2, long startTs, long endTs, T value);
}
