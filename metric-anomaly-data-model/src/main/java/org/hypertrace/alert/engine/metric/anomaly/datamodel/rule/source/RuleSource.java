package org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import org.hypertrace.core.documentstore.Document;

public interface RuleSource {
  List<Document> getAllEventConditions(String type) throws IOException;

  List<JsonNode> getJsonNodes(String fsPath) throws IOException;
}
