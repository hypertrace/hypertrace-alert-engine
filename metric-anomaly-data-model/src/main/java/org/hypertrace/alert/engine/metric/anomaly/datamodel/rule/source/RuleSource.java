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
import org.slf4j.Logger;

public interface RuleSource {

  ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  List<Document> getAllEventConditions(String type) throws IOException;

  static List<JsonNode> getJsonNodes(Config config, String pathConfig, Logger logger)
      throws IOException {
    String fsPath = config.getString(pathConfig);
    logger.debug("Reading rules from file path:{}", fsPath);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(new File(fsPath).getAbsoluteFile());
    if (!jsonNode.isArray()) {
      throw new IOException("File should contain an array of notification rules");
    }

    logger.info("Reading rules {}", jsonNode.toPrettyString());
    return StreamSupport.stream(jsonNode.spliterator(), false)
        .collect(Collectors.toUnmodifiableList());
  }
}
