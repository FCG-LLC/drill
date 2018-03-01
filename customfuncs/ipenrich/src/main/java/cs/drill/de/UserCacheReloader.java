package cs.drill.de;

import lombok.AllArgsConstructor;

@AllArgsConstructor
class UserCacheReloader implements Runnable {
  private final UserCacheManager cacheManager;

  @Override
  public void run() {
    try {
      cs.drill.util.Logger.debug("Reloading cache");
      if (cacheManager.doesCacheExist()) {
        // fetch new entries
        cacheManager.updateCache();
      } else {
        // build new cache
        cacheManager.forceRefreshCacheSync();
      }
    } catch (Exception exc) {
      cs.drill.util.Logger.trace("An exception occurred during cache reload", exc);
    }
  }
}
