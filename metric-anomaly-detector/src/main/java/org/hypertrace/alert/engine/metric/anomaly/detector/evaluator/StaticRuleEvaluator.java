package org.hypertrace.alert.engine.metric.anomaly.detector.evaluator;

import static org.hypertrace.alert.engine.metric.anomaly.detector.MetricAnomalyDetectorConstants.TENANT_ID_KEY;

import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyNotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.StaticRuleViolationSummary;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.StaticThresholdOperator;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.ViolationSummary;
import org.hypertrace.alerting.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alerting.config.service.v1.StaticThresholdCondition;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticRuleEvaluator {

  private static final Logger LOGGER = LoggerFactory.getLogger(StaticRuleEvaluator.class);
  private static final ConcurrentMap<String, Timer> staticRuleTimer = new ConcurrentHashMap<>();
  private static final String STATIC_RULE_TIMER =
      "hypertrace.metric.anomaly.detector.static.rule.latency";
  private final MetricCache metricCache;

  public StaticRuleEvaluator(MetricCache metricCache) {
    this.metricCache = metricCache;
  }

  Optional<MetricAnomalyNotificationEvent> evaluateRule(
      MetricAnomalyEventCondition metricAnomalyEventCondition, AlertTask alertTask) {
    Instant startTime = Instant.now();
    StaticThresholdCondition staticThresholdCondition =
        metricAnomalyEventCondition
            .getViolationConditionList()
            .get(0)
            .getStaticThresholdCondition();

    int dataCount = 0, violationCount = 0;
    List<Double> metricValues = new ArrayList<>();

    List<Pair<Long, Double>> dataList =
        metricCache.getMetricValues(
            Map.of(TENANT_ID_KEY, alertTask.getTenantId()),
            metricAnomalyEventCondition.getMetricSelection(),
            alertTask.getTenantId(),
            alertTask.getCurrentExecutionTime()
                - java.time.Duration.parse(
                        metricAnomalyEventCondition.getEvaluationWindowDuration())
                    .toMillis(),
            alertTask.getCurrentExecutionTime());

    for (Pair<Long, Double> timeStampedValue : dataList) {
      metricValues.add(timeStampedValue.getValue());
      dataCount++;
      if (compareThreshold(timeStampedValue.getValue(), staticThresholdCondition)) {
        violationCount++;
      }
    }

    staticRuleTimer
        .computeIfAbsent(
            alertTask.getTenantId(),
            k -> PlatformMetricsRegistry.registerTimer(STATIC_RULE_TIMER, Map.of("tenantId", k)))
        .record(Duration.between(startTime, Instant.now()).toMillis(), TimeUnit.MILLISECONDS);

    if (!EvaluatorUtil.isViolation(dataCount, violationCount)) {
      LOGGER.debug("Rule violated. dataCount {}, violationCount {}", dataCount, violationCount);
      return Optional.empty();
    }

    LOGGER.debug("Rule normal. dataCount {} violationCount {}", dataCount, violationCount);

    return getNotificationEvent(
        alertTask,
        dataCount,
        violationCount,
        staticThresholdOperatorToOperatorConvertor(staticThresholdCondition.getOperator()),
        metricValues,
        getConditionRhs(staticThresholdCondition),
        metricAnomalyEventCondition.getEvaluationWindowDuration());
  }

  boolean compareThreshold(double lhs, StaticThresholdCondition staticThresholdCondition) {
    double rhs = staticThresholdCondition.getValue();
    return evalOperator(staticThresholdCondition.getOperator(), lhs, rhs);
  }

  private double getConditionRhs(StaticThresholdCondition staticThresholdCondition) {
    return staticThresholdCondition.getValue();
  }

  private boolean evalOperator(
      org.hypertrace.alerting.config.service.v1.StaticThresholdOperator operator,
      double lhs,
      double rhs) {
    switch (operator) {
      case STATIC_THRESHOLD_OPERATOR_GT:
        return lhs > rhs;
      case STATIC_THRESHOLD_OPERATOR_LT:
        return lhs < rhs;
      case STATIC_THRESHOLD_OPERATOR_GTE:
        return lhs >= rhs;
      case STATIC_THRESHOLD_OPERATOR_LTE:
        return lhs <= rhs;
      default:
        throw new UnsupportedOperationException(
            "Unsupported threshold condition operator: " + operator);
    }
  }

  private static StaticThresholdOperator staticThresholdOperatorToOperatorConvertor(
      org.hypertrace.alerting.config.service.v1.StaticThresholdOperator operator) {
    switch (operator) {
      case STATIC_THRESHOLD_OPERATOR_GT:
        return StaticThresholdOperator.STATIC_THRESHOLD_OPERATOR_GT;
      case STATIC_THRESHOLD_OPERATOR_LT:
        return StaticThresholdOperator.STATIC_THRESHOLD_OPERATOR_LT;
      case STATIC_THRESHOLD_OPERATOR_GTE:
        return StaticThresholdOperator.STATIC_THRESHOLD_OPERATOR_GTE;
      case STATIC_THRESHOLD_OPERATOR_LTE:
        return StaticThresholdOperator.STATIC_THRESHOLD_OPERATOR_LTE;
      default:
        throw new UnsupportedOperationException(
            "Unsupported threshold condition operator: " + operator);
    }
  }

  private Optional<MetricAnomalyNotificationEvent> getNotificationEvent(
      AlertTask alertTask,
      int dataCount,
      int violationCount,
      StaticThresholdOperator operator,
      List<Double> metricValues,
      double staticThreshold,
      String evaluationWindowDuration) {

    List<ViolationSummary> violationSummaryList = new ArrayList<>();

    violationSummaryList.add(
        ViolationSummary.newBuilder()
            .setViolationSummary(
                StaticRuleViolationSummary.newBuilder()
                    .setMetricValues(new ArrayList<>(metricValues))
                    .setStaticThreshold(staticThreshold)
                    .setDataCount(dataCount)
                    .setViolationCount(violationCount)
                    .setOperator(operator)
                    .setEvaluationWindowDuration(evaluationWindowDuration)
                    .build())
            .build());

    MetricAnomalyNotificationEvent metricAnomalyNotificationEvent =
        MetricAnomalyNotificationEvent.newBuilder()
            .setViolationTimestamp(alertTask.getCurrentExecutionTime())
            .setChannelId(alertTask.getChannelId())
            .setEventConditionId(alertTask.getEventConditionId())
            .setEventConditionType(alertTask.getEventConditionType())
            .setViolationSummaryList(violationSummaryList)
            .setEventTimeMillis(alertTask.getCurrentExecutionTime())
            .setTenantId(alertTask.getTenantId())
            .build();

    LOGGER.debug("Notification Event {}", metricAnomalyNotificationEvent);
    return Optional.of(metricAnomalyNotificationEvent);
  }
}
