package cs.drill.geoip;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import cs.drill.geoip.util.SubnetV4;
import cs.drill.geoip.util.SubnetV6;
import cs.drill.toucan.JsonGeoIpEnrichments;
import cs.drill.toucan.ToucanGeoIpEnrichmentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.SoftReference;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@SuppressWarnings({"PMD.LongVariable", "PMD.TooManyFields"})
public final class GeoIpHelper {
  static final String CITY_MMDB_PATH = "GeoLite2-City.mmdb";
  static final String COUNTRY_MMDB_PATH = "GeoLite2-Country.mmdb";
  static final long WKP = 0x0064ff9b00000000L;
  static final Logger LOGGER = LoggerFactory.getLogger(GeoIpHelper.class);
  static DatabaseReader cityReader = null;
  static DatabaseReader countryReader = null;
  static ResultProvider<String> cityProvider = (inet) ->
      getCityDatabaseReader().city(inet).getCity().getName();
  static ResultProvider<String> countryProvider = (inet) ->
      getCountryDatabaseReader().country(inet).getCountry().getName();
  static ResultProvider<Double> latitudeProvider = (inet) ->
      getCityDatabaseReader().city(inet).getLocation().getLatitude();
  static ResultProvider<Double> longitudeProvider = (inet) ->
      getCityDatabaseReader().city(inet).getLocation().getLongitude();
  static SoftCache<String> cityCache = new SoftCache<>();
  static Map<SubnetV4, String> localCityV4Map = new LinkedHashMap<>();
  static Map<SubnetV6, String> localCityV6Map = new LinkedHashMap<>();
  static SoftCache<String> countryCache = new SoftCache<>();
  static Map<SubnetV4, String> localCountryV4Map = new LinkedHashMap<>();
  static Map<SubnetV6, String> localCountryV6Map = new LinkedHashMap<>();
  static SoftCache<Double> latitudeCache = new SoftCache<>();
  static Map<SubnetV4, Double> localLatitudeV4Map = new LinkedHashMap<>();
  static Map<SubnetV6, Double> localLatitudeV6Map = new LinkedHashMap<>();
  static SoftCache<Double> longitudeCache = new SoftCache<>();
  static Map<SubnetV4, Double> localLongitudeV4Map = new LinkedHashMap<>();
  static Map<SubnetV6, Double> localLongitudeV6Map = new LinkedHashMap<>();
  static final ToucanGeoIpEnrichmentManager MANAGER =
      ToucanGeoIpEnrichmentManager.getInstance();
  static JsonGeoIpEnrichments lastJson = null;

  static {
    clearLocalMaps();
    MANAGER.consumer = GeoIpHelper::populateLocalMaps;
  }

  static void clearLocalMaps() {
    localCityV4Map.clear();
    localCountryV4Map.clear();
    localLatitudeV4Map.clear();
    localLongitudeV4Map.clear();
    localCityV6Map.clear();
    localCountryV6Map.clear();
    localLatitudeV6Map.clear();
    localLongitudeV6Map.clear();

    // update of local cache also clears whole cache
    cityCache.clear();
    countryCache.clear();
    latitudeCache.clear();
    longitudeCache.clear();
  }

  static void populateLocalMaps(JsonGeoIpEnrichments json) {
    if (json.equals(lastJson)) {
      LOGGER.info("No changes detected");
      return;
    }
    LOGGER.info(
        "Populating new local GeoIp enrichment with: " + json.size() + " entries"
    );

    lastJson = json;
    clearLocalMaps();

    for (JsonGeoIpEnrichments.Entry entry : json) {
      int index = entry.getSubnet().indexOf('/');

      String address = entry.getSubnet().substring(0, index);
      int maskLength = Integer.parseInt(entry.getSubnet().substring(index + 1));

      try {
        InetAddress inetAddress = InetAddress.getByName(address);

        if (inetAddress instanceof Inet4Address) {
          SubnetV4 subnetV4 = new SubnetV4(address, maskLength);
          localCityV4Map.put(subnetV4, entry.getCity());
          localCountryV4Map.put(subnetV4, entry.getCountry());
          localLatitudeV4Map.put(subnetV4, entry.getLat());
          localLongitudeV4Map.put(subnetV4, entry.getLon());
        } else if (inetAddress instanceof Inet6Address) {
          SubnetV6 subnetV6 = new SubnetV6(address, maskLength);
          localCityV6Map.put(subnetV6, entry.getCity());
          localCountryV6Map.put(subnetV6, entry.getCountry());
          localLatitudeV6Map.put(subnetV6, entry.getLat());
          localLongitudeV6Map.put(subnetV6, entry.getLon());
        } else {
          throw new UnknownHostException();
        }
      } catch (UnknownHostException exc) {
        LOGGER.trace("Wrong IP address " + address, exc);
      }
    }
  }

  static class SoftCache<T> {
    // TODO: after longer validation on larger amounts of data and different server configurations,
    //       this approach can be changed into keeping SoftReferences for inner map keys instead of
    //       one SoftReference for the whole map
    SoftReference<Map<Long, Map<Long, T>>> mapRef = new SoftReference<>(null);

    Map<Long, T> getInnerMap(long ip1) {
      Map<Long, Map<Long, T>> map = mapRef.get();
      if (map == null) {
        map = new HashMap<>();
        mapRef = new SoftReference<>(map);
      }

      Map<Long, T> inner = map.get(ip1);
      if (inner == null) {
        inner = new HashMap<>();
        map.put(ip1, inner);
      }
      return inner;
    }

    void clear() {
      mapRef.clear();
    }
  }

  @FunctionalInterface
  interface ResultProvider<R> {
    R apply(InetAddress inetAddress) throws GeoIp2Exception, IOException;
  }

  @SuppressWarnings({"PMD.AvoidThrowingRawExceptionTypes"})
  static DatabaseReader getDatabaseReader(String path) {
    InputStream database = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
    try {
      return new DatabaseReader.Builder(database).build();
    } catch (IOException error) {
      throw new RuntimeException(error);
    }
  }

  static DatabaseReader getCityDatabaseReader() {
    return cityReader == null ? (cityReader = getDatabaseReader(CITY_MMDB_PATH)) : cityReader;
  }

  static DatabaseReader getCountryDatabaseReader() {
    return countryReader == null
      ? (countryReader = getDatabaseReader(COUNTRY_MMDB_PATH))
      : countryReader;
  }

  static InetAddress getAddressFromIps(long ip1, long ip2) throws IOException {
    ByteBuffer buffer;
    if (ip1 == WKP) {
      buffer = ByteBuffer.allocate(Integer.BYTES);
      buffer.putInt((int) ip2);
    } else {
      buffer = ByteBuffer.allocate(Long.BYTES * 2);
      buffer.putLong(ip1);
      buffer.putLong(Long.BYTES, ip2);
    }
    return InetAddress.getByAddress(buffer.array());
  }

  @SuppressWarnings({"PMD.EmptyCatchBlock", "PMD.CyclomaticComplexity"})
  static <R> R getValue(long ip1, long ip2, ResultProvider<R> provider, SoftCache<R> cache,
                        Map<SubnetV4, R> v4localProvider, Map<SubnetV6, R> v6localProvider) {
    // check cache
    Map<Long, R> inner = cache.getInnerMap(ip1);
    if (inner.containsKey(ip2)) {
      return inner.get(ip2);
    }

    R result = null;

    // check local subnets
    if (ip1 == WKP) {
      for (Map.Entry<SubnetV4, R> entry : v4localProvider.entrySet()) {
        SubnetV4 subnet = entry.getKey();
        if ((subnet.getMask() & ip2) == subnet.getAddress()) {
          result = entry.getValue();
          inner.put(ip2, result);
          return result;
        }
      }
    } else {
      for (Map.Entry<SubnetV6, R> entry : v6localProvider.entrySet()) {
        SubnetV6 subnet = entry.getKey();
        if ((subnet.getMaskHighBits() & ip1) == subnet.getAddressHighBits()
            && (subnet.getMaskLowBits() & ip2) == subnet.getAddressLowBits()) {
          result = entry.getValue();
          inner.put(ip2, result);
          return result;
        }
      }
    }

    // check maxmind database
    try {
      InetAddress address = getAddressFromIps(ip1, ip2);
      if (!address.isSiteLocalAddress() && !address.isLinkLocalAddress()) {
        result = provider.apply(address);
      }
    } catch (GeoIp2Exception | IOException error) {
      // NOP
    }
    inner.put(ip2, result);
    return result;
  }

  public static String getCity(long ip1, long ip2) {
    return getValue(ip1, ip2, cityProvider, cityCache, localCityV4Map, localCityV6Map);
  }

  public static String getCountry(long ip1, long ip2) {
    return getValue(ip1, ip2, countryProvider, countryCache, localCountryV4Map, localCountryV6Map);
  }

  public static Double getLatitude(long ip1, long ip2) {
    return getValue(ip1, ip2, latitudeProvider, latitudeCache, localLatitudeV4Map,
      localLatitudeV6Map);
  }

  public static Double getLongitude(long ip1, long ip2) {
    return getValue(ip1, ip2, longitudeProvider, longitudeCache, localLongitudeV4Map,
      localLongitudeV6Map);
  }

  private GeoIpHelper() {}
}
