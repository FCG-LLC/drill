package cs.drill.de;

import com.fasterxml.jackson.databind.ObjectMapper;
import cs.drill.rest.RestClient;
import cs.drill.rest.RestClientException;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class UserCacheManager {
  // TODO: When migrate to presto we need to handle that properly.
  // Don't want to lose time with drill resource management
  private static final String DE_ENDPOINT = "http://data-enrichment:8888/ip-user";
  private static final int RELOAD_PERIOD_MIN = 5;
  private static final ScheduledExecutorService SCHEDULED_THREAD = Executors.newSingleThreadScheduledExecutor();
  private SoftReference<TimedIpCache<String>> cacheInstance = new SoftReference<>(null);
  private final RestClient restClient;
  private final Object cacheInitializationLock = new Object();
  private long lastUpdateTime = 0;

  private static class LazyHolder {
    static final UserCacheManager INSTANCE = new UserCacheManager();
  }

  public static UserCacheManager getInstance() {
    return LazyHolder.INSTANCE;
  }

  private UserCacheManager() {
    this.restClient = new RestClient();
    cs.drill.util.Logger.info("Scheduling UserCacheReloader");
    SCHEDULED_THREAD.scheduleAtFixedRate(
        new UserCacheReloader(this),
        0,
        RELOAD_PERIOD_MIN,
        TimeUnit.MINUTES
    );
  }

  public String getUser(long ip1, long ip2, long timestamp) {
    TimedIpCache<String> cache = cacheInstance.get();
    if (cache == null) {
      // Disabled throwing on performance concerns
      return null;
    } else {
      return cache.get(ip1, ip2, timestamp);
    }
  }

  /**
   * Synchronized method for reloading cache synchronously (only when it is not fetched already).
   */
  public void fetchCacheIfNotExist() throws CacheException {
    synchronized (cacheInitializationLock) {
      if (cacheInstance.get() == null) {
        forceRefreshCacheSync();
        cs.drill.util.Logger.debug("Cache refreshed");
      } else {
        cs.drill.util.Logger.debug("Nothing to refresh");
      }
    }
  }

  boolean doesCacheExist() {
    return cacheInstance.get() != null;
  }

  void forceRefreshCacheSync() throws CacheException {
    cs.drill.util.Logger.debug("Cache is refreshing");
    TimedIpCache<String> newCache = createCache();
    setCacheInstance(newCache);
  }

  private void setCacheInstance(TimedIpCache<String> newCache) {
    cacheInstance = new SoftReference<>(newCache);
  }

  /**
   * Updates existing cache.
   * @throws CacheException
   */
  void updateCache() throws CacheException {
    if (!doesCacheExist()) {
      throw new CacheException("Cache is not initialized");
    }
    List<EnrichedUser> enrichedUsers = getEnrichedUsers(lastUpdateTime);
    populateCache(cacheInstance.get(), enrichedUsers);
  }

  private TimedIpCache<String> createCache() throws CacheException {
    List<EnrichedUser> enrichedUsers = getEnrichedUsers();

    UserTimedIpCache newCache = new UserTimedIpCache();
    populateCache(newCache, enrichedUsers);

    return newCache;
  }

  private void populateCache(TimedIpCache<String> cache, List<EnrichedUser> enrichedUsers) {
    // that kind of sort can be slow
    enrichedUsers.sort(Comparator.comparing(EnrichedUser::getStartTs));

    for (EnrichedUser enrichedUser : enrichedUsers) {
      cache.put(
        enrichedUser.getIp().getHighBits(),
        enrichedUser.getIp().getLowBits(),
        enrichedUser.getStartTs(),
        enrichedUser.getEndTs(),
        enrichedUser.getUser()
      );
    }

    lastUpdateTime = System.currentTimeMillis();
  }

  /**
   * Gets user names for ips from DataEnrichment project.
   * If timestamp is not provided (null) all records will be fetched.
   * @param timestamp
   * @return
   * @throws CacheException
   */
  private List<EnrichedUser> getEnrichedUsers(Long timestamp) throws CacheException {
    try {
      String getParams = "";
      if (timestamp != null) {
        getParams = "?ts_from=" + timestamp;
      }
      String json = restClient.getJson(DE_ENDPOINT + getParams);
      ObjectMapper objectMapper = new ObjectMapper();
      JsonResults results = objectMapper.readValue(json, JsonResults.class);
      return results.getEnrichedUsers();
    } catch (IOException | RestClientException exc) {
      throw new CacheException("Cache initialization exception", exc);
    }
  }

  private List<EnrichedUser> getEnrichedUsers() throws CacheException {
    return getEnrichedUsers(null);
  }


}
