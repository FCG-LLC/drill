package cs.drill.toucan;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class JsonGeoIpEncrichmentsTest {
  public static class Equals {
    String subnet = "";
    String city = "";
    String country = "";
    Double lon = 1.0;
    Double lat = -1.0;

    @Test
    public void returnsFalseIfWrongNumberOfElements() {
      JsonGeoIpEnrichments first = new JsonGeoIpEnrichments();
      first.add(new JsonGeoIpEnrichments.Entry(subnet, city, country, lon, lat));
      JsonGeoIpEnrichments second = new JsonGeoIpEnrichments();
      second.add(new JsonGeoIpEnrichments.Entry(subnet, city, country, lon, lat));
      second.add(new JsonGeoIpEnrichments.Entry(subnet, city, country, lon, lat));
      assertNotEquals(first, second);
    }

    @Test
    public void returnsFalseWhenElementsDoesntMatch() {
      JsonGeoIpEnrichments first = new JsonGeoIpEnrichments();
      first.add(new JsonGeoIpEnrichments.Entry(subnet, city, country, lon, lat));
      JsonGeoIpEnrichments second = new JsonGeoIpEnrichments();

      second.add(new JsonGeoIpEnrichments.Entry(subnet + "1", city, country, lon, lat));
      assertNotEquals(first, second);

      second.clear();
      second.add(new JsonGeoIpEnrichments.Entry(subnet, city + "1", country, lon, lat));
      assertNotEquals(first, second);

      second.clear();
      second.add(new JsonGeoIpEnrichments.Entry(subnet, city, country + "1", lon, lat));
      assertNotEquals(first, second);

      second.clear();
      second.add(new JsonGeoIpEnrichments.Entry(subnet, city, country, lon + 1.0, lat));
      assertNotEquals(first, second);

      second.clear();
      second.add(new JsonGeoIpEnrichments.Entry(subnet, city, country, lon, lat + 1.0));
      assertNotEquals(first, second);
    }

    @Test
    public void returnsFalseIfElementsInWrongOrder() {
      JsonGeoIpEnrichments first = new JsonGeoIpEnrichments();
      first.add(new JsonGeoIpEnrichments.Entry(subnet, city, country, lon, lat));
      first.add(new JsonGeoIpEnrichments.Entry(subnet, city, country, lon, lat + 1.0));
      JsonGeoIpEnrichments second = new JsonGeoIpEnrichments();
      second.add(new JsonGeoIpEnrichments.Entry(subnet, city, country, lon, lat + 1.0));
      second.add(new JsonGeoIpEnrichments.Entry(subnet, city, country, lon, lat));

      assertNotEquals(first, second);
    }

    @Test
    public void returnsTrueIfTheSameInstance() {
      JsonGeoIpEnrichments first = new JsonGeoIpEnrichments();
      first.add(new JsonGeoIpEnrichments.Entry(subnet, city, country, lon, lat));
      JsonGeoIpEnrichments second = new JsonGeoIpEnrichments();
      second.add(new JsonGeoIpEnrichments.Entry(subnet, city, country, lon, lat));

      assertEquals(first, second);
    }
  }
}
