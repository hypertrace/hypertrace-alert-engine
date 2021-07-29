package org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FSRuleSource implements RuleSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(FSRuleSource.class);
  private static final String PATH_CONFIG = "path";
  private static final String EVENT_CONDITION_TYPE_KEY = "eventConditionType";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Config fsConfig;

  public FSRuleSource(Config fsConfig) {
    this.fsConfig = fsConfig;
  }

  public List<Document> getAllEventConditions(String type) throws IOException {
    return getJsonNodes(fsConfig, PATH_CONFIG).stream()
        .filter(node -> node.get(EVENT_CONDITION_TYPE_KEY).textValue().equals(type))
        .map(JSONDocument::new)
        .collect(Collectors.toList());
  }

  public static List<JsonNode> getJsonNodes(Config config, String pathConfig) throws IOException {
    String fsPath = config.getString(pathConfig);
    LOGGER.debug("Reading rules from file path:{}", fsPath);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(new File(fsPath).getAbsoluteFile());
    if (!jsonNode.isArray()) {
      throw new IOException("File should contain an array of notification rules");
    }

    LOGGER.info("Reading rules {}", jsonNode.toPrettyString());
    return StreamSupport.stream(jsonNode.spliterator(), false)
        .collect(Collectors.toUnmodifiableList());
  }
}
