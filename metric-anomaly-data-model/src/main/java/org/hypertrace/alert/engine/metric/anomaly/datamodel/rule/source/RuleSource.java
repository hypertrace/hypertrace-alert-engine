package org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.List;
import org.hypertrace.core.documentstore.Document;
import org.slf4j.Logger;

public interface RuleSource {
  List<Document> getAllEventConditions(String type) throws IOException;

  List<JsonNode> getJsonNodes(Config config, String pathConfig, Logger logger) throws IOException;
}
