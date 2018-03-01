package cs.drill.toucan;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import cs.drill.rest.RestClient;
import cs.drill.rest.RestClientException;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class ToucanAppEnrichmentManager {
  private static final String TOUCAN_ENDPOINT = "http://toucan:3000/config/";
  private static final String TOUCAN_APP_GLOBAL_ENDPOINT = TOUCAN_ENDPOINT + "drill/app_enrichment_global";
  private static final String TOUCAN_APP_USER_ENDPOINT = TOUCAN_ENDPOINT + "drill/app_enrichment_user";
  private static final int RELOAD_PERIOD_MINUTES = 5;
  private static final ScheduledExecutorService SCHEDULED_THREAD = Executors.newSingleThreadScheduledExecutor();
  private final RestClient restClient;
  public Consumer<JsonAppEnrichment> consumer;

  private static class LazyHolder {
    static final ToucanAppEnrichmentManager INSTANCE = new ToucanAppEnrichmentManager();
  }

  public static ToucanAppEnrichmentManager getInstance() {
    return LazyHolder.INSTANCE;
  }

  private ToucanAppEnrichmentManager() {
    this.restClient = new RestClient();
    cs.drill.util.Logger.info("Scheduling ToucanAppEnrichment");
    SCHEDULED_THREAD.scheduleAtFixedRate(
        this::update,
        0,
        RELOAD_PERIOD_MINUTES,
        TimeUnit.MINUTES
    );
  }

  void update() {
    try {
      cs.drill.util.Logger.info("Reloading Toucan app enrichment cache");
      JsonAppEnrichment[] jsons = new JsonAppEnrichment[] {
          fetchData(TOUCAN_APP_USER_ENDPOINT),
          fetchData(TOUCAN_APP_GLOBAL_ENDPOINT)
      };
      if (consumer != null) {
        consumer.accept(mergeJsons(jsons));
      }
    } catch (Exception exc) {
      cs.drill.util.Logger.trace("An exception occurred during Toucan app enrichment cache reload", exc);
    }
  }

  JsonAppEnrichment fetchData(String url) {
    String json;

    try {
      json = restClient.getJson(url);
    } catch (RestClientException exc) {
      cs.drill.util.Logger.trace("Cannot load `" + url + "`", exc);
      return null;
    }

    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    AppEnrichmentToucanResponse result;
    try {
      result = objectMapper.readValue(json, AppEnrichmentToucanResponse.class);
    } catch (IOException exc) {
      cs.drill.util.Logger.trace("Cannot parse `" + url + "`", exc);
      return null;
    }

    return result.getValue();
  }

  JsonAppEnrichment mergeJsons(JsonAppEnrichment[] jsons) {
    LinkedHashMap<String, String> names = new LinkedHashMap<>();
    LinkedHashMap<Integer, String> ports = new LinkedHashMap<>();
    JsonAppEnrichment result = new JsonAppEnrichment(names, ports);

    for (JsonAppEnrichment json : jsons) {
      if (json == null) {
        continue;
      }

      Map<String, String> newNames = json.getNames();
      if (newNames != null) {
        newNames.forEach(names::putIfAbsent);
      }

      Map<Integer, String> newPorts = json.getPorts();
      if (newPorts != null) {
        newPorts.forEach(ports::putIfAbsent);
      }
    }

    return result;
  }
}
