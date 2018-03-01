package cs.drill.geoip;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import cs.drill.geoip.util.SubnetV4;
import cs.drill.geoip.util.SubnetV6;
import cs.drill.toucan.JsonGeoIpEnrichments;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ThreadLocalRandom;

import static cs.drill.geoip.GeoIpHelper.CITY_MMDB_PATH;
import static cs.drill.geoip.GeoIpHelper.COUNTRY_MMDB_PATH;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@SuppressWarnings({"PMD.TooManyMethods", "PMD.AvoidUsingHardCodedIP"})
public class GeoIpHelperUnitTest {
  @RunWith(PowerMockRunner.class)
  @PrepareForTest({GeoIpHelper.class})
  public static class GetCityDatabaseReader {
    @Mock DatabaseReader reader;

    @After
    public void clear() {
      GeoIpHelper.cityReader = null;
    }

    @Test
    public void returnsCityReaderIfExists() {
      GeoIpHelper.cityReader = reader;
      assertSame(reader, GeoIpHelper.getCityDatabaseReader());
    }

    @Test
    public void createsNewReader() {
      PowerMockito.mockStatic(GeoIpHelper.class);
      PowerMockito.when(GeoIpHelper.getCityDatabaseReader()).thenCallRealMethod();
      PowerMockito.when(GeoIpHelper.getDatabaseReader(CITY_MMDB_PATH)).thenReturn(reader);
      assertSame(reader, GeoIpHelper.getCityDatabaseReader());
      assertSame(reader, GeoIpHelper.cityReader);
    }
  }

  @RunWith(PowerMockRunner.class)
  @PrepareForTest({GeoIpHelper.class})
  public static class GetCountryDatabaseReader {
    @Mock DatabaseReader reader;

    @After
    public void clear() {
      GeoIpHelper.countryReader = null;
    }

    @Test
    public void returnsCountryReaderIfExists() {
      GeoIpHelper.countryReader = reader;
      assertSame(reader, GeoIpHelper.getCountryDatabaseReader());
    }

    @Test
    public void createsNewReader() {
      PowerMockito.mockStatic(GeoIpHelper.class);
      PowerMockito.when(GeoIpHelper.getCountryDatabaseReader()).thenCallRealMethod();
      PowerMockito.when(GeoIpHelper.getDatabaseReader(COUNTRY_MMDB_PATH)).thenReturn(reader);
      assertSame(reader, GeoIpHelper.getCountryDatabaseReader());
      assertSame(reader, GeoIpHelper.countryReader);
    }
  }

  public static class GetAddressFromIps {
    @Test
    public void returnsInetAddressOfIpV4() throws IOException {
      long ip1 = 0x0064ff9b00000000L;
      long ip2 = 0x7b7b7b7bL;
      assertEquals(
          InetAddress.getByName("123.123.123.123"),
          GeoIpHelper.getAddressFromIps(ip1, ip2)
      );
    }

    @Test
    public void returnsInetAddressOfIpV6() throws IOException {
      long ip1 = 0x20010db800000001L;
      long ip2 = 0xfffa0001L;
      assertEquals(
          InetAddress.getByName("2001:db8:0:1::fffa:1"),
          GeoIpHelper.getAddressFromIps(ip1, ip2)
      );
    }
  }

  @RunWith(PowerMockRunner.class)
  @PrepareForTest({GeoIpHelper.class})
  public static class GetValue {
    long ip1 = ThreadLocalRandom.current().nextLong();
    long ip2 = ThreadLocalRandom.current().nextLong();
    @Mock GeoIpHelper.ResultProvider resultProvider;
    Map<SubnetV4, String> localV4Provider = spy(new HashMap<SubnetV4, String>());
    Map<SubnetV6, String> localV6Provider = spy(new HashMap<SubnetV6, String>());
    GeoIpHelper.SoftCache<String> softCache = spy(new GeoIpHelper.SoftCache<>());
    @Mock InetAddress inetAddress;
    @Mock String result;

    @Before
    public void setUp() throws IOException, GeoIp2Exception {
      PowerMockito.mockStatic(GeoIpHelper.class);
      PowerMockito.when(GeoIpHelper.getAddressFromIps(ip1, ip2)).thenReturn(inetAddress);
      when(resultProvider.apply(inetAddress)).thenAnswer(inv -> result);
      PowerMockito
          .when(GeoIpHelper.getValue(ip1, ip2, resultProvider, softCache, localV4Provider,
            localV6Provider))
          .thenCallRealMethod();
      softCache.getInnerMap(ip1);
    }

    @Test
    public void returnsGivenProviderWithGetAddressFromIpsResult() {
      assertSame(result, GeoIpHelper.getValue(ip1, ip2, resultProvider, softCache, localV4Provider,
          localV6Provider));
    }

    @Test
    public void returnsNullForNullProviderResult() {
      result = null;
      assertNull(GeoIpHelper.getValue(ip1, ip2, resultProvider, softCache, localV4Provider,
          localV6Provider));
    }

    @Test
    public void returnsValueFromCache() throws IOException, GeoIp2Exception {
      softCache.mapRef.get().put(ip1, new WeakHashMap<>());
      softCache.mapRef.get().get(ip1).put(ip2, result);
      assertEquals(result, GeoIpHelper.getValue(ip1, ip2, resultProvider, softCache,
          localV4Provider, localV6Provider));
      verify(resultProvider, never()).apply(any());
    }

    @Test
    public void returnsNullValueFromCache() throws IOException, GeoIp2Exception {
      softCache.mapRef.get().put(ip1, new WeakHashMap<>());
      softCache.mapRef.get().get(ip1).put(ip2, null);
      assertNull(GeoIpHelper.getValue(ip1, ip2, resultProvider, softCache, localV4Provider,
          localV6Provider));
      verify(resultProvider, never()).apply(any());
    }

    @Test
    public void savesResultIntoCache() {
      GeoIpHelper.getValue(ip1, ip2, resultProvider, softCache, localV4Provider, localV6Provider);
      assertSame(result, softCache.mapRef.get().get(ip1).get(ip2));
    }

    @Test
    public void returnsNullForGeoIp2Exception() throws IOException {
      PowerMockito.when(GeoIpHelper.getAddressFromIps(ip1, ip2)).thenThrow(mock(IOException.class));
      assertNull(GeoIpHelper.getValue(ip1, ip2, resultProvider, softCache, localV4Provider,
          localV6Provider));
    }

    @Test
    public void returnsNullForIoException() throws IOException, GeoIp2Exception {
      when(resultProvider.apply(inetAddress)).thenThrow(mock(GeoIp2Exception.class));
      assertNull(GeoIpHelper.getValue(ip1, ip2, resultProvider, softCache, localV4Provider,
          localV6Provider));
    }
  }

  @RunWith(PowerMockRunner.class)
  @PrepareForTest({GeoIpHelper.class})
  public static class GetCity {
    long ip1 = ThreadLocalRandom.current().nextLong();
    long ip2 = ThreadLocalRandom.current().nextLong();

    @Test
    public void callsGetValueWithCityProvider() {
      PowerMockito.mockStatic(GeoIpHelper.class);
      PowerMockito.when(GeoIpHelper.getCity(ip1, ip2)).thenCallRealMethod();
      GeoIpHelper.getCity(ip1, ip2);
      PowerMockito.verifyStatic();
      GeoIpHelper.getValue(ip1, ip2, GeoIpHelper.cityProvider, GeoIpHelper.cityCache,
          GeoIpHelper.localCityV4Map, GeoIpHelper.localCityV6Map);
    }
  }

  @RunWith(PowerMockRunner.class)
  @PrepareForTest({GeoIpHelper.class})
  public static class GetCountry {
    long ip1 = ThreadLocalRandom.current().nextLong();
    long ip2 = ThreadLocalRandom.current().nextLong();

    @Test
    public void callsGetValueWithCountryProvider() {
      PowerMockito.mockStatic(GeoIpHelper.class);
      PowerMockito.when(GeoIpHelper.getCountry(ip1, ip2)).thenCallRealMethod();
      GeoIpHelper.getCountry(ip1, ip2);
      PowerMockito.verifyStatic();
      GeoIpHelper.getValue(ip1, ip2, GeoIpHelper.countryProvider, GeoIpHelper.countryCache,
          GeoIpHelper.localCountryV4Map, GeoIpHelper.localCountryV6Map);
    }
  }

  @RunWith(PowerMockRunner.class)
  @PrepareForTest({GeoIpHelper.class})
  public static class GetLatitude {
    long ip1 = ThreadLocalRandom.current().nextLong();
    long ip2 = ThreadLocalRandom.current().nextLong();

    @Test
    public void callsGetValueWithLatitudeProvider() {
      PowerMockito.mockStatic(GeoIpHelper.class);
      PowerMockito.when(GeoIpHelper.getLatitude(ip1, ip2)).thenCallRealMethod();
      GeoIpHelper.getLatitude(ip1, ip2);
      PowerMockito.verifyStatic();
      GeoIpHelper.getValue(ip1, ip2, GeoIpHelper.latitudeProvider, GeoIpHelper.latitudeCache,
          GeoIpHelper.localLatitudeV4Map, GeoIpHelper.localLatitudeV6Map);
    }
  }

  @RunWith(PowerMockRunner.class)
  @PrepareForTest({GeoIpHelper.class})
  public static class GetLongitude {
    long ip1 = ThreadLocalRandom.current().nextLong();
    long ip2 = ThreadLocalRandom.current().nextLong();

    @Test
    public void callsGetValueWithLongitudeProvider() {
      PowerMockito.mockStatic(GeoIpHelper.class);
      PowerMockito.when(GeoIpHelper.getLongitude(ip1, ip2)).thenCallRealMethod();
      GeoIpHelper.getLongitude(ip1, ip2);
      PowerMockito.verifyStatic();
      GeoIpHelper.getValue(ip1, ip2, GeoIpHelper.longitudeProvider, GeoIpHelper.longitudeCache,
          GeoIpHelper.localLongitudeV4Map, GeoIpHelper.localLongitudeV6Map);
    }
  }

  @RunWith(PowerMockRunner.class)
  @PrepareForTest({GeoIpHelper.class})
  public static class PopulateLocalMaps {
    @Test
    public void doesntCallClearMapsWhenTheSameJson() {
      PowerMockito.mockStatic(GeoIpHelper.class);
      GeoIpHelper.lastJson = mock(JsonGeoIpEnrichments.class);
      PowerMockito.doCallRealMethod().when(GeoIpHelper.class);
      GeoIpHelper.populateLocalMaps(GeoIpHelper.lastJson);

      GeoIpHelper.populateLocalMaps(GeoIpHelper.lastJson);
      PowerMockito.verifyStatic(never());
      GeoIpHelper.clearLocalMaps();
    }
  }
}
