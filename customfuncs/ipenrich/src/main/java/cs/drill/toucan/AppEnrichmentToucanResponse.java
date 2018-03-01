package cs.drill.toucan;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class AppEnrichmentToucanResponse {
  String status;
  String key;
  JsonAppEnrichment value;
}
