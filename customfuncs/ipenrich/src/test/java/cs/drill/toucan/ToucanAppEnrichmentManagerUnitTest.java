package cs.drill.toucan;

import cs.drill.rest.RestClient;
import cs.drill.rest.RestClientException;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.any;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.LinkedHashMap;
import java.util.function.Consumer;

public class ToucanAppEnrichmentManagerUnitTest {
  @RunWith(MockitoJUnitRunner.class)
  public static class Update {
    @Mock Consumer consumer;
    ToucanAppEnrichmentManager manager;
    @Mock JsonAppEnrichment elem;

    @Before
    public void setUp() {
      manager = spy(new ToucanAppEnrichmentManager(null, consumer));
      doReturn(null).when(manager).fetchData(any());
    }

    @Test
    public void callsConsumerWithMergedJson() {
      doReturn(elem).when(manager).mergeJsons(any());
      manager.update();
      verify(consumer).accept(elem);
    }

    @Test
    public void doesNotMergeJsonsWhenNoConsumer() {
      manager.consumer = null;
      manager.update();
      verify(manager, never()).mergeJsons(any());
    }

    @Test
    public void catchesEndpointException() {
      doThrow(Exception.class).when(manager).fetchData(any());
      manager.update();
    }

    @Test
    public void catchesMergeJsonsException() {
      doThrow(Exception.class).when(manager).mergeJsons(any());
      manager.update();
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class FetchData {
    @Mock RestClient restClient;
    ToucanAppEnrichmentManager manager;
    String url = "testUrl";
    LinkedHashMap<String, String> names = new LinkedHashMap<>();
    LinkedHashMap<Integer, String> ports = new LinkedHashMap<>();
    JsonAppEnrichment json = new JsonAppEnrichment(names, ports);

    @Before
    public void setUp() {
      manager = new ToucanAppEnrichmentManager(restClient, null);
    }

    @Test
    public void returnsNullWhenRestClientThrows() throws RestClientException {
      when(restClient.getJson(any())).thenThrow(RestClientException.class);
      assertNull(manager.fetchData(url));
    }

    @Test
    public void returnsDeserializedJson() throws RestClientException {
      when(restClient.getJson(url)).thenReturn(
          "{\"value\": {\"names\": {\"a\": \"b\"}, \"ports\": {\"2\": \"port2\"}}}"
      );

      names.put("a", "b");
      ports.put(2, "port2");

      JsonAppEnrichment result = manager.fetchData(url);
      assertEquals(json, result);
    }

    @Test
    public void returnsDeserializedJsonWithMissedNames() throws RestClientException {
      when(restClient.getJson(url)).thenReturn(
          "{\"value\": {\"ports\": {\"2\": \"port2\"}}}"
      );

      JsonAppEnrichment json = new JsonAppEnrichment(null, ports);
      ports.put(2, "port2");

      JsonAppEnrichment result = manager.fetchData(url);
      assertEquals(json, result);
    }

    @Test
    public void returnsDeserializedJsonWithMissedPorts() throws RestClientException {
      when(restClient.getJson(url)).thenReturn(
          "{\"value\": {\"names\": {\"a\": \"b\"}}}"
      );

      JsonAppEnrichment json = new JsonAppEnrichment(names, null);
      names.put("a", "b");

      JsonAppEnrichment result = manager.fetchData(url);
      assertEquals(json, result);
    }

    @Test
    public void returnsNullOnWrongJson() throws RestClientException {
      when(restClient.getJson(url)).thenReturn("{pancakes}");
      assertNull(manager.fetchData(url));
    }
  }

  public static class MergeJsons {
    ToucanAppEnrichmentManager manager = ToucanAppEnrichmentManager.getInstance();
    LinkedHashMap<String, String> names1 = new LinkedHashMap<>();
    LinkedHashMap<Integer, String> ports1 = new LinkedHashMap<>();
    JsonAppEnrichment elem1 = new JsonAppEnrichment(names1, ports1);
    LinkedHashMap<String, String> names2 = new LinkedHashMap<>();
    LinkedHashMap<Integer, String> ports2 = new LinkedHashMap<>();
    JsonAppEnrichment elem2 = new JsonAppEnrichment(names2, ports2);

    @Test
    public void returnsResultForEmptyArray() {
      JsonAppEnrichment result = manager.mergeJsons(new JsonAppEnrichment[] {});
      assertNotNull(result);
    }

    @Test
    public void returnsResultForNullElements() {
      JsonAppEnrichment result = manager.mergeJsons(new JsonAppEnrichment[] {null, null});
      assertNotNull(result);
    }

    @Test
    public void mergesGivenElements() {
      names1.put("name1", "val1");
      names1.put("name2", "val2");
      ports1.put(1, "port1");
      ports1.put(5, "port5");
      names2.put("name2", "val2-overridden");
      names2.put("name3", "val3");
      ports2.put(5, "port5-overridden");
      ports2.put(7, "port7");

      LinkedHashMap<String, String> exNames = new LinkedHashMap<>();
      LinkedHashMap<Integer, String> exPorts = new LinkedHashMap<>();
      exNames.put("name1", "val1");
      exNames.put("name2", "val2");
      exNames.put("name3", "val3");
      exPorts.put(1, "port1");
      exPorts.put(5, "port5");
      exPorts.put(7, "port7");
      JsonAppEnrichment expected = new JsonAppEnrichment(exNames, exPorts);

      JsonAppEnrichment result = manager.mergeJsons(new JsonAppEnrichment[] {elem1, null, elem2});

      assertEquals(expected, result);
    }

    @Test
    public void givenElementNamesCanBeNull() {
      JsonAppEnrichment elem = new JsonAppEnrichment(null, ports1);
      JsonAppEnrichment result = manager.mergeJsons(new JsonAppEnrichment[] {elem});
      assertNotNull(result);
    }

    @Test
    public void givenElementPortsCanBeNull() {
      JsonAppEnrichment elem = new JsonAppEnrichment(names1, null);
      JsonAppEnrichment result = manager.mergeJsons(new JsonAppEnrichment[] {elem});
      assertNotNull(result);
    }
  }
}
