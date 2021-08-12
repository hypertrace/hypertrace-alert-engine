package org.hypertrace.alert.engine.metric.anomaly.detector.evaluator;

import static org.hypertrace.alert.engine.metric.anomaly.detector.MetricAnomalyDetectorConstants.TENANT_ID_KEY;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math.stat.descriptive.rank.Median;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.DynamicThresholdCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.DynamicRuleViolationSummary;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.EventRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyNotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.ViolationSummary;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.gateway.service.v1.baseline.Baseline;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaselineRuleEvaluator {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaselineRuleEvaluator.class);

  private final QueryRequestHandler queryRequestHandler;

  public BaselineRuleEvaluator(QueryRequestHandler queryRequestHandler) {
    this.queryRequestHandler = queryRequestHandler;
  }

  Optional<NotificationEvent> evaluateRule(
      MetricAnomalyEventCondition metricAnomalyEventCondition, AlertTask alertTask)
      throws IOException {
    DynamicThresholdCondition dynamicThresholdCondition =
        metricAnomalyEventCondition
            .getViolationConditionList()
            .get(0)
            .getDynamicThresholdCondition();

    Duration duration = java.time.Duration.parse(dynamicThresholdCondition.getBaselineDuration());
    long durationMillis = duration.toMillis();

    // this query will fetch metric data, which will used for baseline calculation and evaluation
    QueryRequest queryRequest =
        queryRequestHandler.getQueryRequest(
            metricAnomalyEventCondition,
            alertTask.getTenantId(),
            alertTask.getCurrentExecutionTime() - durationMillis,
            alertTask.getCurrentExecutionTime());

    LOGGER.debug("Rule id {}, Query request {}", alertTask.getEventConditionId(), queryRequest);

    Iterator<ResultSetChunk> iterator =
        queryRequestHandler.executeQuery(
            Map.of(TENANT_ID_KEY, alertTask.getTenantId()), queryRequest);

    List<Double> metricValuesForBaseline = new ArrayList<>();
    List<Double> metricValuesForEvaluation = new ArrayList<>();

    iterator.forEachRemaining(
        v -> {
          for (Row row : v.getRowList()) {
            if (row.getColumnCount() >= 2
                && row.getColumn(1).getValueType()
                    == org.hypertrace.core.query.service.api.ValueType.STRING) {
              double value = Double.parseDouble(row.getColumn(1).getString());
              metricValuesForBaseline.add(value);
              if (Long.parseLong(row.getColumn(0).getString())
                  >= alertTask.getLastExecutionTime()) {
                metricValuesForEvaluation.add(value);
              }
            }
          }
        });

    LOGGER.debug(
        "Rule id {}, Metric data for baseline {}, evaluation {}",
        alertTask.getEventConditionId(),
        metricValuesForBaseline,
        metricValuesForEvaluation);

    Baseline baseline =
        getBaseline(metricValuesForBaseline.stream().mapToDouble(Double::doubleValue).toArray());

    LOGGER.debug("Rule id {}, Baseline value {}", alertTask.getEventConditionId(), baseline);

    int dataCount = 0, violationCount = 0;
    for (Double metricValue : metricValuesForBaseline) {
      dataCount++;
      if (metricValue < baseline.getLowerBound().getDouble()
          || metricValue > baseline.getUpperBound().getDouble()) {
        violationCount++;
      }
    }

    LOGGER.debug(
        "Rule id {}, DataCount {}, ViolationCount {}",
        alertTask.getEventConditionId(),
        dataCount,
        violationCount);

    if (!StaticRuleEvaluator.isViolation(dataCount, violationCount)) {
      return Optional.empty();
    }

    return getNotificationEvent(
        alertTask,
        dataCount,
        violationCount,
        metricValuesForEvaluation,
        baseline.getLowerBound().getDouble(),
        baseline.getUpperBound().getDouble());
  }

  private static Baseline getBaseline(double[] metricValueArray) {
    Median median = new Median();
    StandardDeviation standardDeviation = new StandardDeviation();
    double medianValue = median.evaluate(metricValueArray);
    double sd = standardDeviation.evaluate(metricValueArray);
    double lowerBound = Math.max(0, medianValue - (2 * sd));
    double upperBound = medianValue + (2 * sd);
    Baseline baseline =
        Baseline.newBuilder()
            .setLowerBound(
                Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(lowerBound).build())
            .setUpperBound(
                Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(upperBound).build())
            .setValue(
                Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(medianValue).build())
            .build();
    return baseline;
  }

  private Optional<NotificationEvent> getNotificationEvent(
      AlertTask alertTask,
      int dataCount,
      int violationCount,
      List<Double> metricValues,
      double baselineLowerBound,
      double baselineUpperBound)
      throws IOException {

    List<ViolationSummary> violationSummaryList = new ArrayList<>();

    violationSummaryList.add(
        ViolationSummary.newBuilder()
            .setViolationSummary(
                DynamicRuleViolationSummary.newBuilder()
                    .setMetricValues(new ArrayList<>(metricValues))
                    .setDataCount(dataCount)
                    .setViolationCount(violationCount)
                    .setBaselineLowerBound(baselineLowerBound)
                    .setBaselineUpperBound(baselineUpperBound)
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
