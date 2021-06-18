package org.hypertrace.alert.engine.metric.anomaly.task.manager.rulereader;

import java.util.List;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.AggregationInterval;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.Always;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.Attribute;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.BaselineCondition;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.BaselineFunctionType;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.BoundingBox;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.BoundingBoxFunctionType;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.DynamicThreshold;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.EvaluationCondition;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.Expression;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.Filter;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.Label;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.Literal;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.LongevityCondition;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.MetricAggregationFunctionType;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.MetricAnomalyAlertRule;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.MetricSelection;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.Operator;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.Period;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.Severity;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.Value;
import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.ValueType;

public class JsonRuleReader implements RuleReader {

  @Override
  public MetricAnomalyAlertRule readRule() {

    MetricAnomalyAlertRule.Builder metricAnomalyAlertRuleBuilder =
        MetricAnomalyAlertRule.newBuilder();

    // set name and metadata labels
    metricAnomalyAlertRuleBuilder.setName("high_avg_latency");
    metricAnomalyAlertRuleBuilder.addAllLabels(
        List.of(Label.newBuilder().setKey("environment").setValue("dev").build()));

    // set metric selection
    metricAnomalyAlertRuleBuilder.setMetricSelection(
        MetricSelection.newBuilder()
            .setKey("duration")
            .setScope("SERVICE")
            .setAggFunctionType(MetricAggregationFunctionType.METRIC_AGGREGATION_FUNCTION_TYPE_AVG)
            .setAggInterval(
                AggregationInterval.newBuilder()
                    .setPeriod(Period.newBuilder().setUnit("seconds").setValue(15).build())
                    .build())
            .addFilters(
                Filter.newBuilder()
                    .setLhs(
                        Expression.newBuilder()
                            .setAttribute(Attribute.newBuilder().setKey("id").build())
                            .build())
                    .setLhs(
                        Expression.newBuilder()
                            .setLiteral(
                                Literal.newBuilder()
                                    .setValue(
                                        Value.newBuilder()
                                            .setValueType(ValueType.VALUE_TYPE_STRING)
                                            .setString("1234")
                                            .build())
                                    .build())
                            .build())
                    .setOperator(Operator.OPERATOR_EQ)
                    .build())
            .build());

    // add baseline condition
    metricAnomalyAlertRuleBuilder.addBaselineCondition(
        BaselineCondition.newBuilder()
            .setDynamicThreshold(
                DynamicThreshold.newBuilder()
                    .setBaselineFunctionType(BaselineFunctionType.BASELINE_FUNCTION_TYPE_AVG)
                    .setLookBackWindow(Period.newBuilder().setUnit("days").setValue(1).build())
                    .setBoundingBoxFunctionType(
                        BoundingBoxFunctionType.BOUNDING_BOX_FUNCTION_TYPE_STANDARD_DEVIATION)
                    .setBoundingBox(BoundingBox.newBuilder().setUpper(2).setLower(2).build())
                    .build())
            .setSeverity(Severity.SEVERITY_CRITICAL)
            .build());

    // add continuity condition
    metricAnomalyAlertRuleBuilder.setLongevityCondition(
        LongevityCondition.newBuilder()
            .setDuration(Period.newBuilder().setUnit("minute").setValue(5).build())
            .build());

    // set evaluation condition
    metricAnomalyAlertRuleBuilder.setEvaluationCondition(
        EvaluationCondition.newBuilder().setAlways(Always.newBuilder().build()).build());

    // return an object
    return metricAnomalyAlertRuleBuilder.build();
  }
}
