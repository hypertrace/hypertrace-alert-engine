package org.hypertrace.alert.engine.metric.anomaly.task.manager.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class FileSystemDataSource implements DataSource {
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  Config config;

  public void init(Config config) {
    this.config = config;
  }

  public List<JsonNode> getAllNotificationRules() {
    try {
      String path = config.getString("path");
      JsonNode jsonNode = OBJECT_MAPPER.readTree(new File(path));
      if (jsonNode.isArray()) {
        return StreamSupport.stream(jsonNode.spliterator(), false).collect(Collectors.toList());
      }
    } catch (Exception exception) {
      // report failure
    }
    return List.of();
  }
}
