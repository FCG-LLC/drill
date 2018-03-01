package cs.drill.de;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import cs.drill.util.IpUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ResultsDeserializer extends StdDeserializer<JsonResults> {

  public ResultsDeserializer() {
    this(null);
  }

  protected ResultsDeserializer(Class<?> vc) {
    super(vc);
  }

  /**
   * Deserializes results to the list of enriched users (JsonResult).
   *
   * Response example:
   *
   * {[[1507035602958, 1507035901958, 28428538856079360, 168558853, "fake_user4"],
   * [1507035602958, 1507035901958, 28428538856079360, 168558854, "fake_user5"]]}
   * @param jsonParser
   * @param deserializationContext
   * @return
   * @throws IOException
   */
  @Override
  public JsonResults deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    JsonNode node = jsonParser.readValueAsTree();
    JsonNode results = node.get("Results");
    if (results == null) {
      throw new IllegalArgumentException("Json doesn't have results field " + node.toString());
    }
    if (results.isArray()) {
      ArrayNode resultArray = (ArrayNode) results;
      List<EnrichedUser> enrichedUsers = new ArrayList<>();
      for (JsonNode result : resultArray) {
        if (!result.isArray()) {
          throw new IllegalArgumentException("Result is not an array: " + node.toString());
        }
        if (result.size() != 5) {
          throw new IllegalArgumentException("Result array has wrong size (expected: 5, actual: " + result.size() + ", " + result.toString());
        }
        enrichedUsers.add(getEnrichedUser(result));
      }
      return new JsonResults(enrichedUsers);
    } else {
      throw new IllegalArgumentException("JsonResults are not an array: " + results.toString());
    }
  }

  private EnrichedUser getEnrichedUser(JsonNode result) {
    Long startTs = result.get(0).isNull() ? null : result.get(0).asLong();
    Long endTs = result.get(1).isNull() ? null : result.get(1).asLong();
    long highBits = result.get(2).asLong();
    long lowBits = result.get(3).asLong();
    String user = result.get(4).asText();
    return new EnrichedUser(startTs, endTs, new IpUtil.IpPair(highBits, lowBits), user);
  }
}
