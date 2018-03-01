package cs.drill.de;

import java.util.HashMap;
import java.util.Map;

/**
 * Cache responsible for storing ip-user relations in time.
 */
public class UserTimedIpCache implements TimedIpCache<String> {
  private Map<Long, Map<Long, TimeUserCache>> cache = new HashMap<>();

  @Override
  public String get(long ip1, long ip2, long timestamp) {
    Map<Long, TimeUserCache> ip1Cache = cache.get(ip1);
    if (ip1Cache == null) {
      return null;
    }
    TimeUserCache timedUserCache = ip1Cache.get(ip2);
    if (timedUserCache == null) {
      return null;
    }
    return timedUserCache.getUser(timestamp);
  }

  @Override
  public void put(long ip1, long ip2, long startTs, long endTs, String userName) {
    Map<Long, TimeUserCache> ip1Cache = cache.computeIfAbsent(ip1, k -> new HashMap<>());
    TimeUserCache timedUserCache = ip1Cache.computeIfAbsent(ip2, k -> new TimeUserCache());
    timedUserCache.addNextUser(startTs, endTs, userName);
  }
}
