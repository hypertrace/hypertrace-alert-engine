package org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FSRuleSource implements RuleSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(FSRuleSource.class);
  private static final String PATH_CONFIG = "path";
  private final Config fsConfig;
  ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public FSRuleSource(Config fsConfig) {
    this.fsConfig = fsConfig;
  }

  public List<Document> getAllRules(Predicate<JsonNode> predicate) throws IOException {
    return getJsonNodes(fsConfig.getString(PATH_CONFIG)).stream()
        .filter(predicate)
        .map(JSONDocument::new)
        .collect(Collectors.toList());
  }

  private List<JsonNode> getJsonNodes(String fsPath) throws IOException {
    LOGGER.debug("Reading rules from file path:{}", fsPath);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(new File(fsPath).getAbsoluteFile());
    if (!jsonNode.isArray()) {
      throw new IOException("File should contain an array of notification rules");
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Reading document  {}", jsonNode.toPrettyString());
    }
    return StreamSupport.stream(jsonNode.spliterator(), false)
        .collect(Collectors.toUnmodifiableList());
  }
}
