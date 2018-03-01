package cs.drill.toucan;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.LinkedHashMap;

@AllArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonAppEnrichment {
  private LinkedHashMap<String, String> names;
  private LinkedHashMap<Integer, String> ports;
}
