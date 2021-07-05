package org.hypertrace.alert.engine.metric.anomaly.task.manager.rule.source;

import java.io.IOException;
import java.util.List;
import org.hypertrace.core.documentstore.Document;

public interface RuleSource {
  List<Document> getAllEventConditions(String type) throws IOException;
}
