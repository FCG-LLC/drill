package cs.drill.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;

public class FileReader {
  private String filePath;
  private String splitSign;

  public FileReader(String filePath, String splitSign) {
    this.filePath = filePath;
    this.splitSign = splitSign;
  }

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

  private BufferedReader getBufferedReader(String filePath) throws IOException {
    InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath);
    if (inputStream == null) {
      // external file
      Path path = Paths.get(filePath);
      return Files.newBufferedReader(path);
    } else {
      // resources file
      return new BufferedReader(
        new InputStreamReader(inputStream)
      );
    }
  }
}
