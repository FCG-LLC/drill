package cs.drill.rest;

import lombok.NonNull;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class RestClient {

  /**
   * Performs GET method on desired address and returns payload as json.
   * @param address address of service
   * @return response payload as json
   * @throws RestClientException in case of error
   */
  public String getJson(String address) throws RestClientException {
    try {
      // for some reason there is need of using obsolete default http client
      // because drill cannot find newer packages
      HttpClient httpClient = new org.apache.http.impl.client.DefaultHttpClient();
      HttpGet httpGet = new HttpGet(address);
      HttpResponse response = httpClient.execute(httpGet);
      HttpEntity entity = response.getEntity();
      if (entity == null) {
        throw new RestClientException("Empty response");
      } else {
        return processResponse(entity);
      }
    } catch (IOException exc) {
      throw new RestClientException("Error while fetching data", exc);
    }
  }

  /**
   * Extracts payload from {@link HttpEntity}.
   * @param entity http entity with content
   * @return content of entity as String
   * @throws IOException if something went wrong with extracting
   */
  private String processResponse(@NonNull HttpEntity entity) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent()));
    StringBuilder result = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      result.append(line);
    }

    return result.toString();
  }
}
