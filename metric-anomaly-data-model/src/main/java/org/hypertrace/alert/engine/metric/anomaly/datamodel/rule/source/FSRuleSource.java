package org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source;

import com.typesafe.config.Config;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FSRuleSource implements RuleSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(FSRuleSource.class);
  private static final String PATH_CONFIG = "path";
  private static final String EVENT_CONDITION_TYPE_KEY = "eventConditionType";
  private final Config fsConfig;

  public FSRuleSource(Config fsConfig) {
    this.fsConfig = fsConfig;
  }

  public List<Document> getAllEventConditions(String type) throws IOException {
    return RuleSource.getJsonNodes(fsConfig, PATH_CONFIG, LOGGER).stream()
        .filter(node -> node.get(EVENT_CONDITION_TYPE_KEY).textValue().equals(type))
        .map(JSONDocument::new)
        .collect(Collectors.toList());
  }
}
