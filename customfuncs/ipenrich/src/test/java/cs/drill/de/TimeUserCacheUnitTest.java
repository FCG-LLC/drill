package cs.drill.de;

import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TimeUserCacheUnitTest {
  static final String USER_0 = "user0";
  static final String USER_1 = "user1";
  static final String USER_2 = "user2";

  public static class GetUser {
    TimeUserCache timeUserCache = new TimeUserCache();

    @Test
    public void returnsNullIfNotPopulated() {
      assertNull(timeUserCache.getUser(1L));
    }

    private void populateCache() {
      List<Long> timestamps = (List) Whitebox.getInternalState(timeUserCache, "timestamps");
      List<String> users = (List)Whitebox.getInternalState(timeUserCache, "users");
      timestamps.addAll(Arrays.asList(
          100L, 120L, 140L, 180L, 250L
      ));
      users.addAll(Arrays.asList(
          USER_0, null, USER_1, USER_2, null
      ));
    }

    @Test
    public void returnsCorrectNonNullUserForTimestamp() {
      populateCache();

      String actualUser = timeUserCache.getUser(100L);

      assertEquals(USER_0, actualUser);
    }

    @Test
    public void returnsCorrectNullUserForTimestampWhenNoUser() {
      populateCache();

      String actualUser = timeUserCache.getUser(120L);

      assertNull(actualUser);
    }

    @Test
    public void returnsNullUserForTimestampAfterLastAdded() {
      populateCache();

      String actualUser = timeUserCache.getUser(300L);

      assertNull(actualUser);
    }

  }

  public static class AddNextUser {
    private static final String NEW_USER = "someUser";
    TimeUserCache timeUserCache = new TimeUserCache();

    private List<Long> getCacheTimestamps() {
      return (List) Whitebox.getInternalState(timeUserCache, "timestamps");
    }

    private List<String> getCacheUsers() {
      return (List)Whitebox.getInternalState(timeUserCache, "users");
    }

    private void populateCache() {
      getCacheTimestamps().addAll(Arrays.asList(
          100L, 120L, 140L, 180L, 250L
      ));
      getCacheUsers().addAll(Arrays.asList(
          USER_0, null, USER_1, USER_2, null
      ));
    }

    @Test
    public void initializesEmptyCache() {
      assertTrue(getCacheTimestamps().isEmpty());
      assertTrue(getCacheUsers().isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsWhenAddingNotToTheEnd() {
      populateCache();
      timeUserCache.addNextUser(80L, 110L, NEW_USER);
    }

    @Test
    public void addsMiniBucketWhenWiderWithSameStartTsProvided() {
      populateCache();
      timeUserCache.addNextUser(180L, 270L, NEW_USER);

      assertEquals(
        Arrays.asList(100L, 120L, 140L, 180L, 250L, 270L),
        getCacheTimestamps()
      );
      assertEquals(
        Arrays.asList(USER_0, null, USER_1, USER_2, NEW_USER, null),
        getCacheUsers()
      );
    }

    @Test
    public void updatesBucketWhenShorterWithSameStartTsProvided() {
      populateCache();
      timeUserCache.addNextUser(180L, 230L, NEW_USER);

      assertEquals(
        Arrays.asList(100L, 120L, 140L, 180L, 230L),
        getCacheTimestamps()
      );
      assertEquals(
        Arrays.asList(USER_0, null, USER_1, NEW_USER, null),
        getCacheUsers()
      );
    }

    @Test
    public void splitsBucketWhenNewerStartTsProvided() {
      populateCache();
      timeUserCache.addNextUser(190L, 230L, NEW_USER);

      assertEquals(
        Arrays.asList(100L, 120L, 140L, 180L, 190L, 230L),
        getCacheTimestamps()
      );
      assertEquals(
        Arrays.asList(USER_0, null, USER_1, USER_2, NEW_USER, null),
        getCacheUsers()
      );
    }

    @Test
    public void addsNextEntryWhenTimeRangeCatchOnLastOne() {
      populateCache();
      timeUserCache.addNextUser(250L, 280L, NEW_USER);

      assertEquals(
          Arrays.asList(100L, 120L, 140L, 180L, 250L, 280L),
          getCacheTimestamps()
      );
      assertEquals(
          Arrays.asList(USER_0, null, USER_1, USER_2, NEW_USER, null),
          getCacheUsers()
      );
    }

    @Test
    public void addsNextEntryWhenTimeRangeIsWithDelayToLastOne() {
      populateCache();
      timeUserCache.addNextUser(260L, 280L, NEW_USER);

      assertEquals(
          Arrays.asList(100L, 120L, 140L, 180L, 250L, 260L, 280L),
          getCacheTimestamps()
      );
      assertEquals(
          Arrays.asList(USER_0, null, USER_1, USER_2, null, NEW_USER, null),
          getCacheUsers()
      );
    }
  }
}
