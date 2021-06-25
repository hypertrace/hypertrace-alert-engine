package org.hypertrace.alert.engine.metric.anomaly.task.manager.rulereader;

import org.hypertrace.alert.engine.eventcondition.config.service.v1.Attribute;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.Filter;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.LeafFilter;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.LhsExpression;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAggregationFunction;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricSelection;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.NotificationRule;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.RhsExpression;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.Severity;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdOperator;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.ValueOperator;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.ViolationCondition;

public class JsonRuleReader implements RuleReader {

  @Override
  public Object readRule() {
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

    NotificationRule.Builder nBuilder = NotificationRule.newBuilder();
    nBuilder.setId("notification_rule_1");
    nBuilder.setRuleName("high_avg_latency");
    nBuilder.setDescription("Alert for high avg latency of payment service");
    nBuilder.setEventConditionId("event_condition_1");
    nBuilder.setEventConditionType("MetricAnomalyEventCondition");
    return nBuilder.build();
  }
}
