package cs.drill.geoip;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.function.Consumer;

public class FileReader {
  private final String filePath;
  private final String splitSign;

  public FileReader(String filePath, String splitSign) {
    this.filePath = filePath;
    this.splitSign = splitSign;
  }

  @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
  public void processFile(Consumer<String[]> rowConsumer) {
    try (BufferedReader reader = getBufferedReader(filePath)) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] values = line.split(splitSign);
        rowConsumer.accept(values);
      }
    } catch (IOException exc) {
      throw new RuntimeException("Problem with reading " + filePath + " file", exc);
    }
  }

  private BufferedReader getBufferedReader(String filePath) {
    InputStream inputStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream(filePath);
    return new BufferedReader(
      new InputStreamReader(inputStream)
    );
  }
}
