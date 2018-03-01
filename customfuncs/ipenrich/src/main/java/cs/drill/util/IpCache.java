package cs.drill.util;

public interface IpCache<T> {
  T get(long ip1, long ip2);
  void put(long ip1, long ip2, T value);
}
