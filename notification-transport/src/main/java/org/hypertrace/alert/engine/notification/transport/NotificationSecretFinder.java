package org.hypertrace.alert.engine.notification.transport;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Optional;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationSecretFinder {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationSecretFinder.class);
  private static final String ROOT_PATH = "/var/notification/secrets";
  private static final String API_TOKEN_SYS_PROP = "notification.api.token";

  public static Optional<String> findSecret(String key) {
    String apiToken = System.getProperty(API_TOKEN_SYS_PROP);
    if (apiToken != null) {
      return Optional.of(apiToken);
    }

    File file = new File(ROOT_PATH + "/" + key);
    if (!file.exists() || !file.canRead()) {
      LOG.error("Unable to read file: {}", file.getPath());
      return Optional.empty();
    }

    StringBuilder value = new StringBuilder();
    try (Stream<String> stream = Files.lines(file.toPath(), StandardCharsets.UTF_8)) {
      stream.forEach((value::append));
      return Optional.of(value.toString());
    } catch (IOException e) {
      LOG.error("Unable to read lines from file : {} " + file.getPath(), e);
      return Optional.empty();
    }
  }
}
