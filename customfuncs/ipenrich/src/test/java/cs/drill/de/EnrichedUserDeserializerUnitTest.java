package cs.drill.de;

import com.fasterxml.jackson.databind.ObjectMapper;
import cs.drill.util.IpUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class EnrichedUserDeserializerUnitTest {
  public static class Deserialize {
    private ObjectMapper mapper = new ObjectMapper();

    @Test(expected = IllegalArgumentException.class)
    public void throwsWhenNoResultsFieldInJson() throws IOException {
      String json = "{\"not_results\": 1}";

      mapper.readValue(json, JsonResults.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsWhenResultsIsNotAnArray() throws IOException {
      String json = "{\"Results\": {\"a\": 1}}";

      mapper.readValue(json, JsonResults.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsWhenResultIsNotAnArray() throws IOException {
      String json = "{\"Results\": [{\"a\": 1}]}";

      mapper.readValue(json, JsonResults.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsWhenResultsArrayHasWrongSize() throws IOException {
      String json = "{\"Results\": [[0, 1, 2, 3, 4, 5]]}";

      mapper.readValue(json, JsonResults.class);
    }

    @Test
    public void correctlyDeserializesJson() throws IOException {
      String json = "{\"Results\": [[123, 256, 0, 0, \"user0\"], [256, null, 0, 0, \"user1\"]]}";

      JsonResults results = mapper.readValue(json, JsonResults.class);
      List<EnrichedUser> enrichedUsers = results.getEnrichedUsers();
      assertEquals(2, enrichedUsers.size());

      List<EnrichedUser> expectedUsers = Arrays.asList(
        new EnrichedUser(123L, 256L, new IpUtil.IpPair(0L, 0L), "user0"),
        new EnrichedUser(256L, null, new IpUtil.IpPair(0L, 0L), "user1")
        );
      assertEquals(expectedUsers, enrichedUsers);
    }
  }
}
