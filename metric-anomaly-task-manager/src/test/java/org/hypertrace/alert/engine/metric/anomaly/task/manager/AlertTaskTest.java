package org.hypertrace.alert.engine.metric.anomaly.task.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.Attribute;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.Filter;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.LeafFilter;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.LhsExpression;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAggregationFunction;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricSelection;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.RhsExpression;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.Severity;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdOperator;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.ValueOperator;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.ViolationCondition;
import org.hypertrace.alert.engine.metric.anomaly.data.model.rule.source.RuleSource;
import org.hypertrace.alert.engine.metric.anomaly.data.model.rule.source.RuleSourceProvider;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskConverter;
import org.hypertrace.core.documentstore.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AlertTaskTest {

  @Test
  void testAlertTask() throws Exception {
    URL url = Thread.currentThread().getContextClassLoader().getResource("rules.json");

    File file = Paths.get(url.toURI()).toFile();
    String absolutePath = file.getAbsolutePath();

    Config ruleSourceConfig = ConfigFactory.parseMap(Map.of("type", "fs", "fs.path", absolutePath));
    RuleSource ruleSource = RuleSourceProvider.getProvider(ruleSourceConfig);
    List<Document> documents = ruleSource.getAllEventConditions("MetricAnomalyEventCondition");
    Assertions.assertTrue(documents.size() > 0);

    Config jobConfig =
        ConfigFactory.parseMap(
            Map.of(
                "delayInMinutes", "1",
                "executionWindowInMinutes", 1,
                "tenant_id", "__default"));
    AlertTask.Builder alertTaskBuilder =
        new AlertTaskConverter(jobConfig).toAlertTaskBuilder(documents.get(0));
    assertEquals("MetricAnomalyEventCondition", alertTaskBuilder.getEventConditionType());
    assertEquals("channel-1", alertTaskBuilder.getChannelId());
    MetricAnomalyEventCondition actual =
        MetricAnomalyEventCondition.parseFrom(alertTaskBuilder.getEventConditionValue());
    assertEquals(prepareMetricAnomalyEventCondition(), actual);
  }

  private MetricAnomalyEventCondition prepareMetricAnomalyEventCondition() {
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

    builder.addViolationCondition(
        ViolationCondition.newBuilder()
            .setStaticThresholdCondition(
                StaticThresholdCondition.newBuilder()
                    .setOperator(StaticThresholdOperator.STATIC_THRESHOLD_OPERATOR_GT)
                    .setMinimumViolationDuration("PT5M")
                    .setValue(15)
                    .setSeverity(Severity.SEVERITY_CRITICAL)
                    .build())
            .build());
    return builder.build();
  }
}
