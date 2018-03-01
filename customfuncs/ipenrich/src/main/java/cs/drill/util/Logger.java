package cs.drill.util;

import org.slf4j.LoggerFactory;

/**
 * This class is so stupid example of workaround that it should be shown why drill shouldn't
 * be considered as a serious sql engine.
 */
public class Logger {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger("cs.drill.CustomFunction");

  public static void info(String message, Throwable exception) {
    LOGGER.info(message, exception);
  }

  public static void info(String message) {
    LOGGER.info(message);
  }

  public static void debug(String message, Throwable exception) {
    LOGGER.debug(message, exception);
  }

  public static void debug(String message) {
    LOGGER.debug(message);
  }

  public static void trace(String message) {
    LOGGER.trace(message);
  }

  public static void trace(String message, Throwable exception) {
    LOGGER.trace(message, exception);
  }

  public static void warn(String message, Throwable exception) {
    LOGGER.warn(message, exception);
  }

  public static void warn(String message) {
    LOGGER.warn(message);
  }

  public static void error(String message, Throwable exception) {
    LOGGER.error(message, exception);
  }

  public static void error(String message) {
    LOGGER.error(message);
  }
}
