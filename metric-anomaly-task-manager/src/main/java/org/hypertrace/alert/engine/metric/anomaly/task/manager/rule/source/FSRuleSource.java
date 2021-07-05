package org.hypertrace.alert.engine.metric.anomaly.task.manager.rule.source;

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

class FSRuleSource implements RuleSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(FSRuleSource.class);
  private static final String PATH_CONFIG = "path";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private Config fsConfig;

  public FSRuleSource(Config fsConfig) {
    this.fsConfig = fsConfig;
  }

  public List<Document> getAllEventConditions(String type) throws IOException {
    String fsPath = fsConfig.getString(PATH_CONFIG);
    LOGGER.debug("Reading rules rules from file path:{}", fsPath);

    JsonNode jsonNode = OBJECT_MAPPER.readTree(new File(fsPath));
    if (!jsonNode.isArray()) {
      throw new IOException("File should contain an array of notification rules");
    }

    return StreamSupport.stream(jsonNode.spliterator(), false)
        .map(node -> new JSONDocument(node))
        .collect(Collectors.toList());
  }
}
