package cs.drill.rest;

public class RestClientException extends Exception {
  public RestClientException(String message) {
    super(message);
  }

  public RestClientException(String message, Throwable cause) {
    super(message, cause);
  }
}
