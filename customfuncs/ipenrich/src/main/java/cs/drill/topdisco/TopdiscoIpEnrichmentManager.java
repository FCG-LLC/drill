package cs.drill.topdisco;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class TopdiscoIpEnrichmentManager {
  public static boolean AUTO_RUN = true;
  private static final String JDBC_URL = "jdbc:postgresql://postgres:5432/netdisco";
  private static final String JDBC_USERNAME = "netdisco";
  private static final String JDBC_PASSWORD = "netdisco_passw0rd";
  private static final String SQL = "SELECT row_to_json(r)\n" +
    "FROM (WITH ips AS\n" +
    "        (SELECT array_to_json(array_agg(t)) AS col\n" +
    "         FROM\n" +
    "           (SELECT ine.ip,\n" +
    "                   ine.name,\n" +
    "                   min(ine.entry_type) AS \"entryType\"\n" +
    "            FROM public.ip_name_enrichment ine\n" +
    "            GROUP BY ine.ip,\n" +
    "                     ine.name) t) ,\n" +
    "           interfaces AS\n" +
    "        (SELECT array_to_json(array_agg(t)) AS col\n" +
    "         FROM\n" +
    "           (SELECT port,\n" +
    "                   if_index AS INDEX,\n" +
    "                   array_to_json(array_agg(ip)) AS \"ips\"\n" +
    "            FROM device_port\n" +
    "            GROUP BY port,\n" +
    "                     if_index) t)\n" +
    "      SELECT ips.col AS \"ips\",\n" +
    "             interfaces.col AS \"interfaces\"\n" +
    "      FROM ips,\n" +
    "           interfaces) r";
  private static final int RELOAD_PERIOD_MINUTES = 15;
  private static final ScheduledExecutorService SCHEDULED_THREAD = Executors.newSingleThreadScheduledExecutor();
  public Consumer<JsonIpEnrichment> consumer;
  private int lastResponseHash = 0;

  static {
    try {
      // load Postgres JDBC driver in runtime
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public static void main(String... args) {
    getInstance();
  }

  private static class LazyHolder {
    static final TopdiscoIpEnrichmentManager INSTANCE = new TopdiscoIpEnrichmentManager();
  }

  public static TopdiscoIpEnrichmentManager getInstance() {
    return LazyHolder.INSTANCE;
  }

  TopdiscoIpEnrichmentManager() {
    if (AUTO_RUN) {
      scheduleUpdates();
    }
  }

  void scheduleUpdates() {
    cs.drill.util.Logger.info("Scheduling TopdiscoIpEnrichment");
    SCHEDULED_THREAD.scheduleAtFixedRate(
      this::update,
      0,
      RELOAD_PERIOD_MINUTES,
      TimeUnit.MINUTES
    );
  }

  void update() {
    try {
      cs.drill.util.Logger.info("Reloading Topdisco ip enrichment cache");
      String response = fetchData();
      if (response == null) {
        cs.drill.util.Logger.warn("Cannot reload Topdisco ip enrichment - no response from Postgres received");
        return;
      }
      if (response.hashCode() == lastResponseHash) {
        cs.drill.util.Logger.info("No changes in Topdisco ip enrichment found");
        return;
      }
      lastResponseHash = response.hashCode();
      JsonIpEnrichment json = parseData(response);
      if (json != null && consumer != null) {
        int ipsLength = json.getIps() == null ? 0 : json.getIps().size();
        int interfacesLength = json.getInterfaces() == null ? 0 : json.getInterfaces().size();
        cs.drill.util.Logger.info("Topdisco ip enrichment updated: " +
            ipsLength + " ips and " + interfacesLength + " interfaces");
        consumer.accept(json);
      }
    } catch (Exception exc) {
      cs.drill.util.Logger.error("An exception occurred during Topdisco ip enrichment cache reload", exc);
    }
  }

  String fetchData() throws SQLException {
    Connection connection = DriverManager.getConnection(JDBC_URL, JDBC_USERNAME, JDBC_PASSWORD);
    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(SQL)) {
        return resultSet.next() ? resultSet.getString(1) : null;
      }
    }
  }

  JsonIpEnrichment parseData(String json) {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    try {
      return objectMapper.readValue(json, JsonIpEnrichment.class);
    } catch (IOException exc) {
      cs.drill.util.Logger.error("Cannot parse Topdisco ip enrichment data from Postgres", exc);
      return null;
    }
  }
}
