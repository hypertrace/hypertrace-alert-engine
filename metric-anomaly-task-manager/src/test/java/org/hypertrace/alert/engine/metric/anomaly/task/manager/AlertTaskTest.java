package org.hypertrace.alert.engine.metric.anomaly.task.manager;

import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.METRIC_ANOMALY_EVENT_CONDITION;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source.RuleSource;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskConverter;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobManager;
import org.hypertrace.alerting.config.service.v1.Attribute;
import org.hypertrace.alerting.config.service.v1.BaselineThresholdCondition;
import org.hypertrace.alerting.config.service.v1.Filter;
import org.hypertrace.alerting.config.service.v1.LeafFilter;
import org.hypertrace.alerting.config.service.v1.LhsExpression;
import org.hypertrace.alerting.config.service.v1.MetricAggregationFunction;
import org.hypertrace.alerting.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alerting.config.service.v1.MetricSelection;
import org.hypertrace.alerting.config.service.v1.RhsExpression;
import org.hypertrace.alerting.config.service.v1.Severity;
import org.hypertrace.alerting.config.service.v1.StaticThresholdCondition;
import org.hypertrace.alerting.config.service.v1.StaticThresholdOperator;
import org.hypertrace.alerting.config.service.v1.ValueOperator;
import org.hypertrace.alerting.config.service.v1.ViolationCondition;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class AlertTaskTest {

  private static AlertTaskJobManager alertTaskJobManager;

  @BeforeAll
  public static void setup() {
    alertTaskJobManager =
        new AlertTaskJobManager(
            new PlatformServiceLifecycle() {
              @Override
              public CompletionStage<Void> shutdownComplete() {
                return new CompletableFuture<Void>().minimalCompletionStage();
              }

              @Override
              public State getState() {
                return null;
              }
            });
  }

  @ParameterizedTest
  @MethodSource("provideInvalidRules")
  void testInvalidAlertTask(String ruleId) throws Exception {
    URL url =
        Thread.currentThread().getContextClassLoader().getResource("./invalid-rules/" + ruleId);
    File file = Paths.get(url.toURI()).toFile();
    String absolutePath = file.getAbsolutePath();

    Config ruleSourceConfig = ConfigFactory.parseMap(Map.of("type", "fs", "fs.path", absolutePath));
    RuleSource ruleSource = alertTaskJobManager.getRuleSource(ruleSourceConfig);
    Predicate<JsonNode> PREDICATE =
        node -> (node.get("eventConditionType").textValue().equals(METRIC_ANOMALY_EVENT_CONDITION));
    List<Document> documents = ruleSource.getAllRules(PREDICATE);
    Assertions.assertTrue(documents.size() > 0);

    Config jobConfig =
        ConfigFactory.parseMap(
            Map.of(
                "delayInMinutes", "1",
                "executionWindowInMinutes", 1,
                "tenant_id", "__default"));
    Optional<AlertTask.Builder> alertTaskBuilder =
        new AlertTaskConverter(jobConfig).toAlertTaskBuilder(documents.get(0));
    Assertions.assertTrue(alertTaskBuilder.isEmpty());
  }

  private static Stream<Arguments> provideInvalidRules() {
    return Stream.of(
        Arguments.arguments("invalid_alert_rule1.json"),
        Arguments.arguments("invalid_alert_rule2.json"),
        Arguments.arguments("invalid_alert_rule3.json"));
  }

  @Test
  void testValidAlertTaskForBaseline() throws Exception {
    URL url =
        Thread.currentThread()
            .getContextClassLoader()
            .getResource("./valid-rules/valid_alert_rule1.json");
    File file = Paths.get(url.toURI()).toFile();
    String absolutePath = file.getAbsolutePath();

    Config ruleSourceConfig = ConfigFactory.parseMap(Map.of("type", "fs", "fs.path", absolutePath));
    RuleSource ruleSource = alertTaskJobManager.getRuleSource(ruleSourceConfig);
    Predicate<JsonNode> PREDICATE =
        node -> (node.get("eventConditionType").textValue().equals(METRIC_ANOMALY_EVENT_CONDITION));
    List<Document> documents = ruleSource.getAllRules(PREDICATE);
    Assertions.assertTrue(documents.size() > 0);

    Config jobConfig =
        ConfigFactory.parseMap(
            Map.of(
                "delayInMinutes", "1",
                "executionWindowInMinutes", 1,
                "tenant_id", "__default"));
    Optional<AlertTask.Builder> alertTaskBuilder =
        new AlertTaskConverter(jobConfig).toAlertTaskBuilder(documents.get(0));
    assertEquals("metricAnomalyEventCondition", alertTaskBuilder.get().getEventConditionType());
    assertEquals("channel-1", alertTaskBuilder.get().getChannelId());
    MetricAnomalyEventCondition actual =
        MetricAnomalyEventCondition.parseFrom(alertTaskBuilder.get().getEventConditionValue());
    assertEquals(prepareMetricAnomalyEventCondition(getViolationConditionForBaseline()), actual);
  }

  @Test
  void testValidAlertTaskForStaticCondition() throws Exception {
    URL url =
        Thread.currentThread()
            .getContextClassLoader()
            .getResource("./valid-rules/valid_alert_rule2.json");
    File file = Paths.get(url.toURI()).toFile();
    String absolutePath = file.getAbsolutePath();

    Config ruleSourceConfig = ConfigFactory.parseMap(Map.of("type", "fs", "fs.path", absolutePath));
    RuleSource ruleSource = alertTaskJobManager.getRuleSource(ruleSourceConfig);
    Predicate<JsonNode> PREDICATE =
        node -> (node.get("eventConditionType").textValue().equals(METRIC_ANOMALY_EVENT_CONDITION));
    List<Document> documents = ruleSource.getAllRules(PREDICATE);
    Assertions.assertTrue(documents.size() > 0);

    Config jobConfig =
        ConfigFactory.parseMap(
            Map.of(
                "delayInMinutes", "1",
                "executionWindowInMinutes", 1,
                "tenant_id", "__default"));
    Optional<AlertTask.Builder> alertTaskBuilder =
        new AlertTaskConverter(jobConfig).toAlertTaskBuilder(documents.get(0));
    assertEquals("metricAnomalyEventCondition", alertTaskBuilder.get().getEventConditionType());
    assertEquals("channel-1", alertTaskBuilder.get().getChannelId());
    MetricAnomalyEventCondition actual =
        MetricAnomalyEventCondition.parseFrom(alertTaskBuilder.get().getEventConditionValue());
    assertEquals(
        prepareMetricAnomalyEventCondition(getViolationConditionForStaticCondition()), actual);
  }

  private ViolationCondition getViolationConditionForBaseline() {
    return ViolationCondition.newBuilder()
        .setBaselineThresholdCondition(
            BaselineThresholdCondition.newBuilder().setBaselineDuration("PT5M").build())
        .build();
  }

  private ViolationCondition getViolationConditionForStaticCondition() {
    return ViolationCondition.newBuilder()
        .setStaticThresholdCondition(
            StaticThresholdCondition.newBuilder()
                .setOperator(StaticThresholdOperator.STATIC_THRESHOLD_OPERATOR_GT)
                .setValue(15.0)
                .setSeverity(Severity.SEVERITY_CRITICAL)
                .build())
        .build();
  }

  private MetricAnomalyEventCondition prepareMetricAnomalyEventCondition(
      ViolationCondition violationCondition) {
    MetricAnomalyEventCondition.Builder builder = MetricAnomalyEventCondition.newBuilder();
    builder.setMetricSelection(
        MetricSelection.newBuilder()
            .setMetricAttribute(
                Attribute.newBuilder().setKey("duration").setScope("SERVICE").build())
            .setMetricAggregationFunction(
                MetricAggregationFunction.METRIC_AGGREGATION_FUNCTION_TYPE_AVG)
            .setMetricAggregationInterval("PT15S")
            .setFilter(
                Filter.newBuilder()
                    .setLeafFilter(
                        LeafFilter.newBuilder()
                            .setValueOperator(ValueOperator.VALUE_OPERATOR_EQ)
                            .setLhsExpression(
                                LhsExpression.newBuilder()
                                    .setAttribute(
                                        Attribute.newBuilder()
                                            .setKey("id")
                                            .setScope("SERVICE")
                                            .build())
                                    .build())
                            .setRhsExpression(
                                RhsExpression.newBuilder().setStringValue("1234").build())
                            .build())
                    .build())
            .build());

    builder.setEvaluationWindowDuration("PT5M");
    builder.addViolationCondition(violationCondition);
    return builder.build();
  }
}
