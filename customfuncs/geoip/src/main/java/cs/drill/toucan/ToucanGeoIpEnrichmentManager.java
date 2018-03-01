package cs.drill.toucan;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import cs.drill.rest.RestClient;
import cs.drill.rest.RestClientException;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@SuppressWarnings({
    "PMD.AccessorClassGeneration",
    "PMD.LongVariable",
    "PMD.AvoidCatchingGenericException"
    })
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class ToucanGeoIpEnrichmentManager {
  private static final String TOUCAN_ENDPOINT = "http://toucan:3000/config/";
  private static final String TOUCAN_GEO_IP_USER_ENDPOINT =
      TOUCAN_ENDPOINT + "drill/geoip_enrichment_user";
  private static final int RELOAD_PERIOD_MINUTES = 5;
  private static final ScheduledExecutorService SCHEDULED_THREAD =
      Executors.newSingleThreadScheduledExecutor();
  private final RestClient restClient;
  public Consumer<JsonGeoIpEnrichments> consumer;

  private static class LazyHolder {
    static final ToucanGeoIpEnrichmentManager INSTANCE = new ToucanGeoIpEnrichmentManager();
  }

  public static ToucanGeoIpEnrichmentManager getInstance() {
    return LazyHolder.INSTANCE;
  }

  private ToucanGeoIpEnrichmentManager() {
    this.restClient = new RestClient();
    cs.drill.util.Logger.info("Scheduling ToucanGeoIpEnrichment");
    SCHEDULED_THREAD.scheduleAtFixedRate(
        this::update,
        0,
        RELOAD_PERIOD_MINUTES,
        TimeUnit.MINUTES
    );
  }

  void update() {
    try {
      cs.drill.util.Logger.info("Reloading Toucan geoip enrichment cache");
      JsonGeoIpEnrichments json = fetchData(TOUCAN_GEO_IP_USER_ENDPOINT);
      if (json == null) {
        cs.drill.util.Logger.debug("GeoIpEnrichment received null from toucan");
        return;
      }
      if (consumer != null) {
        consumer.accept(json);
      }
    } catch (Exception exc) {
      cs.drill.util.Logger.trace(
          "An exception occurred during Toucan geoip enrichment cache reload", exc
      );
    }
  }

  JsonGeoIpEnrichments fetchData(String url) throws IOException, RestClientException {
    String json;

    try {
      json = restClient.getJson(url);
    } catch (RestClientException exc) {
      cs.drill.util.Logger.trace("Cannot load `" + url + "`", exc);
      throw exc;
    }

    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    GeoIpEnrichmentToucanResponse result;
    try {
      result = objectMapper.readValue(json, GeoIpEnrichmentToucanResponse.class);
    } catch (IOException exc) {
      cs.drill.util.Logger.trace("Cannot parse `" + url + "`", exc);
      throw exc;
    }

    return result.getValue();
  }
}
