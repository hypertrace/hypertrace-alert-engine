package org.hypertrace.alert.engine.metric.anomaly.task.manager.rulereader;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Optional;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.AlertTask;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.NotificationRule;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.common.AlertTaskProvider;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.common.DataSource;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.common.FileSystemDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JsonRuleReaderTest {

  @Test
  public void testJsonRuleReader() throws Exception {
    JsonRuleReader underTest = new JsonRuleReader();
    NotificationRule alertRule = (NotificationRule) underTest.readRule();
    String alertRuleJson = JsonFormat.printer().print(alertRule);
    Assertions.assertNotNull(alertRuleJson);
  }

  @Test
  void testDataSource() throws Exception {
    DataSource dataSource = new FileSystemDataSource();
    dataSource.init(
        ConfigFactory.parseURL(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("application.conf")
                    .toURI()
                    .toURL())
            .getConfig("fs"));
    List<JsonNode> jsonNodes = dataSource.getAllNotificationRules();
    jsonNodes.forEach(jsonNode -> System.out.println(jsonNode.toPrettyString()));
    Assertions.assertTrue(true);
  }

  @Test
  void testAlertTasks() throws Exception {
    DataSource dataSource = new FileSystemDataSource();
    dataSource.init(
        ConfigFactory.parseURL(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("application.conf")
                    .toURI()
                    .toURL())
            .getConfig("fs"));
    List<JsonNode> jsonNodes = dataSource.getAllNotificationRules();
    List<Optional<AlertTask>> optionals = AlertTaskProvider.prepareTasks(jsonNodes);
    optionals.forEach(
        alertTask -> {
          if (alertTask.isPresent()) {
            System.out.println(alertTask.get());
          }
        });
    Assertions.assertTrue(true);
  }
}
