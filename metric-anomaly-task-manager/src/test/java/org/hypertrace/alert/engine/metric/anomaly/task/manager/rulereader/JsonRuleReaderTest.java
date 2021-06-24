package org.hypertrace.alert.engine.metric.anomaly.task.manager.rulereader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.MetricAnomalyAlertRule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JsonRuleReaderTest {


  public void testJsonRuleReader() throws Exception {
    JsonRuleReader underTest = new JsonRuleReader();
    MetricAnomalyAlertRule alertRule = underTest.readRule();
    String alertRuleJson = JsonFormat.printer().print(alertRule);
    Assertions.assertNotNull(alertRuleJson);

    ObjectMapper objectMapper = new ObjectMapper();
    JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
    JsonNode jsonNode = objectMapper.readTree(Thread.currentThread().getContextClassLoader().getResource("alert_rule_test.json"));
    if (jsonNode.isArray()) {
      ArrayNode arrayNode = (ArrayNode) jsonNode;
      List<MetricAnomalyAlertRule> rules = StreamSupport.stream(arrayNode.spliterator(), false).map(node -> {
        MetricAnomalyAlertRule.Builder builder = MetricAnomalyAlertRule.newBuilder();
        try {
          parser.merge(objectMapper.writeValueAsString(node), builder);
        } catch (Exception e) {
          e.printStackTrace();
        }
        return builder.build();
      }).collect(Collectors.toList());
      Assertions.assertTrue(rules.size() > 0);
    }
  }

  @Test
  void testTrue() {
    Assertions.assertTrue(true);
  }

  @Test
  void xyz() {
    Assertions.fail();
  }
}
