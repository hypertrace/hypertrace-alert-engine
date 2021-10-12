package org.hypertrace.alert.engine.metric.anomaly.detector;

import static org.hypertrace.alert.engine.metric.anomaly.detector.evaluator.AlertRuleEvaluatorTest.createLeafFilter;
import static org.hypertrace.alert.engine.metric.anomaly.detector.evaluator.AlertRuleEvaluatorTest.createLhsExpression;
import static org.hypertrace.alert.engine.metric.anomaly.detector.evaluator.AlertRuleEvaluatorTest.createRhsExpression;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class MetricAnomalyDetectorTest {

  @Test
  @Disabled
  void testStaticRuleEvaluation() throws URISyntaxException, IOException {

    LhsExpression lhsExpression = createLhsExpression("name", "SERVICE");
    RhsExpression rhsExpression = createRhsExpression("customer");
    LeafFilter leafFilter =
        createLeafFilter(ValueOperator.VALUE_OPERATOR_EQ, lhsExpression, rhsExpression);

    MetricSelection metricSelection =
        MetricSelection.newBuilder()
            .setMetricAggregationInterval("PT15s")
            .setMetricAggregationFunction(
                // todo fix this MetricAggregationFunction.METRIC_AGGREGATION_FUNCTION_TYPE_P50)
                MetricAggregationFunction.METRIC_AGGREGATION_FUNCTION_TYPE_AVGRATE)
            .setFilter(Filter.newBuilder().setLeafFilter(leafFilter).build())
            .setMetricAttribute(
                Attribute.newBuilder().setKey("duration").setScope("SERVICE").build())
            .build();

    MetricAnomalyEventCondition.Builder metricAnomalyEventConditionBuilder =
        MetricAnomalyEventCondition.newBuilder();
    metricAnomalyEventConditionBuilder.setMetricSelection(metricSelection);
    metricAnomalyEventConditionBuilder.setEvaluationWindowDuration("PT100M");
    metricAnomalyEventConditionBuilder.addViolationCondition(
        ViolationCondition.newBuilder()
            .setStaticThresholdCondition(
                StaticThresholdCondition.newBuilder()
                    .setOperator(StaticThresholdOperator.STATIC_THRESHOLD_OPERATOR_GT)
                    .setValue(15)
                    .setSeverity(Severity.SEVERITY_CRITICAL)
                    .build())
            .build());

    AlertTask.Builder alertTaskBuilder = AlertTask.newBuilder();
    alertTaskBuilder.setCurrentExecutionTime(System.currentTimeMillis());
    alertTaskBuilder.setLastExecutionTime(
        System.currentTimeMillis() - Duration.ofMinutes(100).toMillis());
    alertTaskBuilder.setEventConditionId("event-condition-1");
    alertTaskBuilder.setEventConditionType("MetricAnomalyEventCondition");
    alertTaskBuilder.setTenantId("__default");
    alertTaskBuilder.setEventConditionValue(
        metricAnomalyEventConditionBuilder.build().toByteString().asReadOnlyByteBuffer());
    alertTaskBuilder.setChannelId("channel1");

    Config config =
        ConfigFactory.parseURL(
            Thread.currentThread()
                .getContextClassLoader()
                .getResource("application.conf")
                .toURI()
                .toURL());

    NotificationEventProducer notificationEventProducer =
        new NotificationEventProducer(
            config.getConfig(MetricAnomalyDetectorService.KAFKA_QUEUE_CONFIG_KEY));
    MetricAnomalyDetector metricAnomalyDetector =
        new MetricAnomalyDetector(config, notificationEventProducer);

    /**
     * Query that's hitting pinot
     *
     * <p>Select
     * dateTimeConvert(start_time_millis,'1:MILLISECONDS:EPOCH','1:MILLISECONDS:EPOCH','15:SECONDS'),
     * SUM(duration_millis) FROM rawServiceView WHERE tenant_id = 'default' AND ( (
     * start_time_millis >= ? AND start_time_millis < ? ) AND service_name = 'customer' ) GROUP BY
     * dateTimeConvert(start_time_millis,'1:MILLISECONDS:EPOCH','1:MILLISECONDS:EPOCH','15:SECONDS')
     * limit 10000
     */
    metricAnomalyDetector.process(alertTaskBuilder.build());
  }

  @Test
  @Disabled
  void testDynamicRuleEvaluation() throws URISyntaxException, IOException {

    LhsExpression lhsExpression = createLhsExpression("name", "SERVICE");
    RhsExpression rhsExpression = createRhsExpression("frontend");
    LeafFilter leafFilter =
        createLeafFilter(ValueOperator.VALUE_OPERATOR_EQ, lhsExpression, rhsExpression);

    MetricSelection metricSelection =
        MetricSelection.newBuilder()
            .setMetricAggregationInterval("PT1M")
            .setMetricAggregationFunction(
                MetricAggregationFunction.METRIC_AGGREGATION_FUNCTION_TYPE_AVGRATE)
            .setFilter(Filter.newBuilder().setLeafFilter(leafFilter).build())
            .setMetricAttribute(
                Attribute.newBuilder().setKey("errorCount").setScope("SERVICE").build())
            .build();

    MetricAnomalyEventCondition.Builder metricAnomalyEventConditionBuilder =
        MetricAnomalyEventCondition.newBuilder();
    metricAnomalyEventConditionBuilder.setEvaluationWindowDuration("PT1M");
    metricAnomalyEventConditionBuilder.setMetricSelection(metricSelection);
    metricAnomalyEventConditionBuilder.addViolationCondition(
        ViolationCondition.newBuilder()
            .setBaselineThresholdCondition(
                BaselineThresholdCondition.newBuilder().setBaselineDuration("PT5M").build())
            .build());

    AlertTask.Builder alertTaskBuilder = AlertTask.newBuilder();
    alertTaskBuilder.setCurrentExecutionTime(System.currentTimeMillis());
    alertTaskBuilder.setLastExecutionTime(
        System.currentTimeMillis() - Duration.ofMinutes(15).toMillis());
    alertTaskBuilder.setEventConditionId("event-condition-1");
    alertTaskBuilder.setEventConditionType("MetricAnomalyEventCondition");
    alertTaskBuilder.setTenantId("__default");
    alertTaskBuilder.setEventConditionValue(
        metricAnomalyEventConditionBuilder.build().toByteString().asReadOnlyByteBuffer());
    alertTaskBuilder.setChannelId("channel1");

    Config config =
        ConfigFactory.parseURL(
            Thread.currentThread()
                .getContextClassLoader()
                .getResource("application.conf")
                .toURI()
                .toURL());

    NotificationEventProducer notificationEventProducer =
        new NotificationEventProducer(
            config.getConfig(MetricAnomalyDetectorService.KAFKA_QUEUE_CONFIG_KEY));
    MetricAnomalyDetector metricAnomalyDetector =
        new MetricAnomalyDetector(config, notificationEventProducer);

    /**
     * Query that's hitting pinot
     *
     * <p>Select
     * dateTimeConvert(start_time_millis,'1:MILLISECONDS:EPOCH','1:MILLISECONDS:EPOCH','15:SECONDS'),
     * SUM(duration_millis) FROM rawServiceView WHERE tenant_id = 'default' AND ( (
     * start_time_millis >= ? AND start_time_millis < ? ) AND service_name = 'customer' ) GROUP BY
     * dateTimeConvert(start_time_millis,'1:MILLISECONDS:EPOCH','1:MILLISECONDS:EPOCH','15:SECONDS')
     * limit 10000
     */
    metricAnomalyDetector.process(alertTaskBuilder.build());
  }
}
