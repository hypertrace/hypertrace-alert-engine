package org.hypertrace.alert.engine.metric.anomaly.task.manager.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import java.util.List;

public interface DataSource {
  void init(Config config);

  List<JsonNode> getAllNotificationRules();
}
