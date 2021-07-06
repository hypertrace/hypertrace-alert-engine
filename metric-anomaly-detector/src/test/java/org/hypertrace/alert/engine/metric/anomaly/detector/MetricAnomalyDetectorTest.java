package org.hypertrace.alert.engine.metric.anomaly.detector;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.time.Duration;
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
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class MetricAnomalyDetectorTest {

  @Test
  @Disabled
  void testRuleEvaluation() throws URISyntaxException, MalformedURLException {

    LhsExpression lhsExpression =
        LhsExpression.newBuilder()
            .setAttribute(Attribute.newBuilder().setKey("name").setScope("SERVICE").build())
            .build();
    RhsExpression rhsExpression = RhsExpression.newBuilder().setStringValue("customer").build();
    LeafFilter leafFilter =
        LeafFilter.newBuilder()
            .setValueOperator(ValueOperator.VALUE_OPERATOR_EQ)
            .setLhsExpression(lhsExpression)
            .setRhsExpression(rhsExpression)
            .build();

    MetricSelection metricSelection =
        MetricSelection.newBuilder()
            .setMetricAggregationInterval("PT15s")
            .setMetricAggregationFunction(
                MetricAggregationFunction.METRIC_AGGREGATION_FUNCTION_TYPE_SUM)
            .setFilter(Filter.newBuilder().setLeafFilter(leafFilter).build())
            .setMetricAttribute(
                Attribute.newBuilder().setKey("duration").setScope("SERVICE").build())
            .build();

    MetricAnomalyEventCondition.Builder metricAnomalyEventConditionBuilder =
        MetricAnomalyEventCondition.newBuilder();

    metricAnomalyEventConditionBuilder.setMetricSelection(metricSelection);

    metricAnomalyEventConditionBuilder.addViolationCondition(
        ViolationCondition.newBuilder()
            .setStaticThresholdCondition(
                StaticThresholdCondition.newBuilder()
                    .setOperator(StaticThresholdOperator.STATIC_THRESHOLD_OPERATOR_GT)
                    .setMinimumViolationDuration("PT5M")
                    .setValue(15)
                    .setSeverity(Severity.SEVERITY_CRITICAL)
                    .build())
            .build());

    AlertTask.Builder alertTaskBuilder = AlertTask.newBuilder();

    alertTaskBuilder.setCurrentExecutionTime(System.currentTimeMillis());
    alertTaskBuilder.setLastExecutionTime(
        System.currentTimeMillis() - Duration.ofMinutes(1).toMillis());
    alertTaskBuilder.setEventConditionId("event-condition-1");
    alertTaskBuilder.setEventConditionType("MetricAnomalyEventCondition");
    alertTaskBuilder.setTenantId("__default");
    alertTaskBuilder.setEventConditionValue(
        metricAnomalyEventConditionBuilder.build().toByteString().asReadOnlyByteBuffer());

    Config config =
        ConfigFactory.parseURL(
            Thread.currentThread()
                .getContextClassLoader()
                .getResource("application.conf")
                .toURI()
                .toURL());

    MetricAnomalyDetector metricAnomalyDetector = new MetricAnomalyDetector(config);

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