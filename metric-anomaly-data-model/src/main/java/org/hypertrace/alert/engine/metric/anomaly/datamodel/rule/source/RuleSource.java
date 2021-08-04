package org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;
import org.hypertrace.core.documentstore.Document;

public interface RuleSource {
  List<Document> getAllRules(Predicate<JsonNode> predicate) throws IOException;
}
