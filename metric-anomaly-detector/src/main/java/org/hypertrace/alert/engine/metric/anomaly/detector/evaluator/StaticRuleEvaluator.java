package org.hypertrace.alert.engine.metric.anomaly.detector.evaluator;

import static org.hypertrace.alert.engine.metric.anomaly.detector.MetricAnomalyDetectorConstants.TENANT_ID_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdCondition;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.EventRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyNotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.StaticRuleViolationSummary;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.StaticThresholdOperator;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.ViolationSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticRuleEvaluator {

  private static final Logger LOGGER = LoggerFactory.getLogger(StaticRuleEvaluator.class);

  private final MetricCache metricCache;

  public StaticRuleEvaluator(MetricCache metricCache) {
    this.metricCache = metricCache;
  }

  Optional<NotificationEvent> evaluateRule(
      MetricAnomalyEventCondition metricAnomalyEventCondition,
      AlertTask alertTask)
      throws IOException {

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
            alertTask.getCurrentExecutionTime() - java.time.Duration.parse(metricAnomalyEventCondition.getRuleDuration()).toMillis(),
            alertTask.getCurrentExecutionTime());

    for (Pair<Long, Double> timeStampedValue : dataList) {
      metricValues.add(timeStampedValue.getValue());
      dataCount++;
      if (compareThreshold(timeStampedValue.getValue(), staticThresholdCondition)) {
        violationCount++;
      }
    }

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
        metricAnomalyEventCondition.getRuleDuration());
  }

  boolean compareThreshold(double lhs, StaticThresholdCondition staticThresholdCondition) {
    double rhs = staticThresholdCondition.getValue();
    return evalOperator(staticThresholdCondition.getOperator(), lhs, rhs);
  }

  private double getConditionRhs(StaticThresholdCondition staticThresholdCondition) {
    return staticThresholdCondition.getValue();
  }

  private boolean evalOperator(
      org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdOperator operator,
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
      org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdOperator
          operator) {
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

  private Optional<NotificationEvent> getNotificationEvent(
      AlertTask alertTask,
      int dataCount,
      int violationCount,
      StaticThresholdOperator operator,
      List<Double> metricValues,
      double staticThreshold,
      String ruleDuration)
      throws IOException {

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
                    .setRuleDuration(ruleDuration)
                    .build())
            .build());

    MetricAnomalyNotificationEvent metricAnomalyNotificationEvent =
        MetricAnomalyNotificationEvent.newBuilder()
            .setViolationTimestamp(alertTask.getCurrentExecutionTime())
            .setChannelId(alertTask.getChannelId())
            .setEventConditionId(alertTask.getEventConditionId())
            .setEventConditionType(alertTask.getEventConditionType())
            .setViolationSummaryList(violationSummaryList)
            .build();

    EventRecord eventRecord =
        EventRecord.newBuilder()
            .setEventType(AlertRuleEvaluator.METRIC_ANOMALY_ACTION_EVENT_TYPE)
            .setEventRecordMetadata(Map.of())
            .setEventValue(metricAnomalyNotificationEvent.toByteBuffer())
            .build();

    NotificationEvent notificationEvent =
        NotificationEvent.newBuilder()
            .setTenantId(alertTask.getTenantId())
            .setNotificationEventMetadata(Map.of())
            .setEventTimeMillis(alertTask.getCurrentExecutionTime())
            .setEventRecord(eventRecord)
            .build();

    LOGGER.debug("Notification Event {}", notificationEvent);
    return Optional.of(notificationEvent);
  }
}
