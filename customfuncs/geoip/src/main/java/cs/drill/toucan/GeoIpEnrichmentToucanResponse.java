package cs.drill.toucan;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class GeoIpEnrichmentToucanResponse {
  String status;
  String key;
  JsonGeoIpEnrichments value;
}
