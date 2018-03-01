package cs.drill.de;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
@JsonDeserialize(using = ResultsDeserializer.class)
public class JsonResults {
  private List<EnrichedUser> enrichedUsers;
}
