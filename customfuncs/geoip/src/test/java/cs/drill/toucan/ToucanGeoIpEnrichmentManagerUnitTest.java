package cs.drill.toucan;

import cs.drill.rest.RestClientException;
import org.junit.Test;

import java.io.IOException;
import java.util.function.Consumer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ToucanGeoIpEnrichmentManagerUnitTest {
  public static class Update {
    @Test
    public void callsConsumerWithFetchResult() throws IOException, RestClientException {
      ToucanGeoIpEnrichmentManager manager = spy(ToucanGeoIpEnrichmentManager.getInstance());
      JsonGeoIpEnrichments result = mock(JsonGeoIpEnrichments.class);
      doReturn(result).when(manager).fetchData(any());
      manager.consumer = mock(Consumer.class);

      manager.update();

      verify(manager.consumer, times(1)).accept(result);
    }

    @Test
    public void doesntCallConsumerWhenIoErrorOnFetching() throws IOException, RestClientException {
      ToucanGeoIpEnrichmentManager manager = spy(ToucanGeoIpEnrichmentManager.getInstance());
      doThrow(IOException.class).when(manager).fetchData(any());
      manager.consumer = mock(Consumer.class);

      manager.update();

      JsonGeoIpEnrichments result = mock(JsonGeoIpEnrichments.class);
      verify(manager.consumer, never()).accept(result);
    }

    @Test
    public void doesntCallConsumerWhenRestErrorOnFetching()
        throws IOException, RestClientException {
      ToucanGeoIpEnrichmentManager manager = spy(ToucanGeoIpEnrichmentManager.getInstance());
      doThrow(RestClientException.class).when(manager).fetchData(any());
      manager.consumer = mock(Consumer.class);

      manager.update();

      JsonGeoIpEnrichments result = mock(JsonGeoIpEnrichments.class);
      verify(manager.consumer, never()).accept(result);
    }

    @Test
    public void doesntCallConsumerWhenNullReceived() throws IOException, RestClientException {
      ToucanGeoIpEnrichmentManager manager = spy(ToucanGeoIpEnrichmentManager.getInstance());
      doReturn(null).when(manager).fetchData(any());
      manager.consumer = mock(Consumer.class);

      manager.update();

      verify(manager.consumer, never()).accept(any());
    }
  }
}
