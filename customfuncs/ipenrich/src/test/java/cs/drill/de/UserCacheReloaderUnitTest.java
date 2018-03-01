package cs.drill.de;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UserCacheReloaderUnitTest {

  @RunWith(MockitoJUnitRunner.class)
  public static class Run {
    @Mock UserCacheManager manager;

    @Test
    public void doesCallForceRefreshCacheIfNoCache() throws CacheException {
      when(manager.doesCacheExist()).thenReturn(false);

      Runnable reloader = new UserCacheReloader(manager);
      reloader.run();

      verify(manager).forceRefreshCacheSync();
    }

    @Test
    public void doesCallUpdateCacheWhenCacheExist() throws CacheException {
      when(manager.doesCacheExist()).thenReturn(true);

      Runnable reloader = new UserCacheReloader(manager);
      reloader.run();

      verify(manager).updateCache();
    }
  }
}
