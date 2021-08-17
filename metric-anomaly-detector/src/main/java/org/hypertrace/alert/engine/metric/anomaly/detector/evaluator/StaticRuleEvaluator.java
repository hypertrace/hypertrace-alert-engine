package org.hypertrace.alert.engine.metric.anomaly.detector.evaluator;

import static org.hypertrace.alert.engine.metric.anomaly.detector.MetricAnomalyDetectorConstants.TENANT_ID_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdCondition;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.EventRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyNotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.StaticRuleViolationSummary;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.StaticThresholdOperator;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.ViolationSummary;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticRuleEvaluator {

  private static final Logger LOGGER = LoggerFactory.getLogger(StaticRuleEvaluator.class);

  private final QueryRequestHandler queryRequestHandler;

  public StaticRuleEvaluator(QueryRequestHandler queryRequestHandler) {
    this.queryRequestHandler = queryRequestHandler;
  }

  Optional<NotificationEvent> evaluateRule(
      MetricAnomalyEventCondition metricAnomalyEventCondition, AlertTask alertTask)
      throws IOException {

    QueryRequest queryRequest =
        queryRequestHandler.getQueryRequest(
            metricAnomalyEventCondition,
            alertTask.getTenantId(),
            alertTask.getLastExecutionTime(),
            alertTask.getCurrentExecutionTime());
    LOGGER.debug("Query request {}", queryRequest);

    Iterator<ResultSetChunk> iterator =
        queryRequestHandler.executeQuery(
            Map.of(TENANT_ID_KEY, alertTask.getTenantId()), queryRequest);

    StaticThresholdCondition staticThresholdCondition =
        metricAnomalyEventCondition
            .getViolationConditionList()
            .get(0)
            .getStaticThresholdCondition();

    int dataCount = 0, violationCount = 0;
    List<Double> metricValues = new ArrayList<>();

    while (iterator.hasNext()) {
      ResultSetChunk resultSetChunk = iterator.next();
      for (Row row : resultSetChunk.getRowList()) {
        Value value = row.getColumn(1);
        if (value.getValueType() != ValueType.STRING) {
          throw new IllegalArgumentException(
              "Expecting value of type string, received valueType: " + value.getValueType());
        }
        dataCount++;
        LOGGER.debug("Metric data {}", value.getString());
        if (compareThreshold(value, staticThresholdCondition)) {
          violationCount++;
        }
        metricValues.add(value.getDouble());
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
        metricAnomalyEventCondition.getMetricSelection().getDuration());
  }

  boolean compareThreshold(Value value, StaticThresholdCondition staticThresholdCondition) {
    double lhs = Double.parseDouble(value.getString());
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
