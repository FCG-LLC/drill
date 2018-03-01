package cs.drill.de;

import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UserTimedIpCacheUnitTest {
  public static class Get {
    private static final Long IP_1 = 1L;
    private static final Long IP_2 = 2L;
    private static final Long TIMESTAMP = 3L;
    private static final String USER = "someUser";
    private static final Long NON_EXISTING_IP_1 = 5L;
    private static final Long NON_EXISTING_IP_2 = 6L;

    private UserTimedIpCache userTimedIpCache = new UserTimedIpCache();
    private Map<Long, Map<Long, TimeUserCache>> internalCache = new HashMap<>();

    @Before
    public void setUp() {
      TimeUserCache timeUserCache = mock(TimeUserCache.class);
      when(timeUserCache.getUser(TIMESTAMP)).thenReturn(USER);
      Map<Long, TimeUserCache> ip2TimeUserInternalCache = new HashMap<>();
      ip2TimeUserInternalCache.put(IP_2, timeUserCache);
      internalCache.put(IP_1, ip2TimeUserInternalCache);

      Whitebox.setInternalState(userTimedIpCache, "cache", internalCache);
    }

    @Test
    public void returnsNullIfNoIp1InCache() {
      String result = userTimedIpCache.get(NON_EXISTING_IP_1, IP_2, TIMESTAMP);
      assertNull(result);
    }

    @Test
    public void returnsNullIfNoIp2InCache() {
      String result = userTimedIpCache.get(IP_1, NON_EXISTING_IP_2, TIMESTAMP);
      assertNull(result);
    }

    @Test
    public void returnsCacheValueIfExists() {
      String result = userTimedIpCache.get(IP_1, IP_2, TIMESTAMP);
      assertEquals(USER, result);
    }
  }
}
