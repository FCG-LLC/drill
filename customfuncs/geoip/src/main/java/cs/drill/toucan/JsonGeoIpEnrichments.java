package cs.drill.toucan;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.ArrayList;

@AllArgsConstructor
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonGeoIpEnrichments extends ArrayList<JsonGeoIpEnrichments.Entry> {
  @AllArgsConstructor
  @Getter
  @ToString
  @EqualsAndHashCode
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Entry {
    private String subnet;
    private String city;
    private String country;
    private Double lon;
    private Double lat;
  }
}
