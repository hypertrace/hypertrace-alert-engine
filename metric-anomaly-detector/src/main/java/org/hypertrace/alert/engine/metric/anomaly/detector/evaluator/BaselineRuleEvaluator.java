package org.hypertrace.alert.engine.metric.anomaly.detector.evaluator;

import static org.hypertrace.alert.engine.metric.anomaly.detector.MetricAnomalyDetectorConstants.TENANT_ID_KEY;
import static org.hypertrace.gateway.service.baseline.lib.BaselineCalculator.getBaseline;

import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.BaselineThresholdCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.BaselineRuleViolationSummary;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.EventRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyNotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.ViolationSummary;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.gateway.service.v1.baseline.Baseline;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaselineRuleEvaluator {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaselineRuleEvaluator.class);
  private static final ConcurrentMap<String, Timer> baselineRuleTimer = new ConcurrentHashMap<>();
  private static final String BASELINE_RULE_TIMER =
      "hypertrace.metric.anomaly.detector.baseline.rule.latency";
  private final MetricCache metricCache;

  public BaselineRuleEvaluator(MetricCache metricCache) {
    this.metricCache = metricCache;
  }

  Optional<NotificationEvent> evaluateRule(
      MetricAnomalyEventCondition metricAnomalyEventCondition, AlertTask alertTask)
      throws IOException {
    Instant startTime = Instant.now();
    BaselineThresholdCondition dynamicThresholdCondition =
        metricAnomalyEventCondition
            .getViolationConditionList()
            .get(0)
            .getBaselineThresholdCondition();

    long baselineDurationMillis =
        java.time.Duration.parse(dynamicThresholdCondition.getBaselineDuration()).toMillis();
    long ruleEvaluationStartTime =
        alertTask.getCurrentExecutionTime()
            - java.time.Duration.parse(metricAnomalyEventCondition.getRuleDuration()).toMillis();

    // this query will fetch metric data, which will used for baseline calculation and evaluation
    List<Pair<Long, Double>> dataList =
        metricCache.getMetricValues(
            Map.of(TENANT_ID_KEY, alertTask.getTenantId()),
            metricAnomalyEventCondition.getMetricSelection(),
            alertTask.getTenantId(),
            ruleEvaluationStartTime - baselineDurationMillis,
            alertTask.getCurrentExecutionTime());

    List<Double> metricValuesForBaseline = new ArrayList<>();
    List<Double> metricValuesForEvaluation = new ArrayList<>();

    for (Pair<Long, Double> timeStampedValue : dataList) {
      if (timeStampedValue.getKey() >= ruleEvaluationStartTime) {
        metricValuesForEvaluation.add(timeStampedValue.getValue());
      } else {
        metricValuesForBaseline.add(timeStampedValue.getValue());
      }
    }

    LOGGER.debug(
        "Rule id {}, Metric data for baseline {}, evaluation {}",
        alertTask.getEventConditionId(),
        metricValuesForBaseline,
        metricValuesForEvaluation);

    Baseline baseline =
        getBaseline(
            metricValuesForBaseline.stream()
                .map(x -> Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(x).build())
                .collect(Collectors.toList()));

    LOGGER.debug("Rule id {}, Baseline value {}", alertTask.getEventConditionId(), baseline);

    int dataCount = 0, violationCount = 0;
    for (Double metricValue : metricValuesForEvaluation) {
      dataCount++;
      if (metricValue < baseline.getLowerBound().getDouble()
          || metricValue > baseline.getUpperBound().getDouble()) {
        violationCount++;
      }
    }

    baselineRuleTimer
        .computeIfAbsent(
            alertTask.getTenantId(),
            k -> PlatformMetricsRegistry.registerTimer(BASELINE_RULE_TIMER, Map.of("tenantId", k)))
        .record(Duration.between(startTime, Instant.now()).toMillis(), TimeUnit.MILLISECONDS);

    LOGGER.debug(
        "Rule id {}, DataCount {}, ViolationCount {}",
        alertTask.getEventConditionId(),
        dataCount,
        violationCount);

    if (!EvaluatorUtil.isViolation(dataCount, violationCount)) {
      return Optional.empty();
    }

    return getNotificationEvent(
        alertTask,
        dataCount,
        violationCount,
        metricValuesForEvaluation,
        baseline.getLowerBound().getDouble(),
        baseline.getUpperBound().getDouble(),
        metricAnomalyEventCondition.getRuleDuration());
  }

  private Optional<NotificationEvent> getNotificationEvent(
      AlertTask alertTask,
      int dataCount,
      int violationCount,
      List<Double> metricValues,
      double baselineLowerBound,
      double baselineUpperBound,
      String ruleDuration)
      throws IOException {

    List<ViolationSummary> violationSummaryList = new ArrayList<>();

    violationSummaryList.add(
        ViolationSummary.newBuilder()
            .setViolationSummary(
                BaselineRuleViolationSummary.newBuilder()
                    .setMetricValues(new ArrayList<>(metricValues))
                    .setDataCount(dataCount)
                    .setViolationCount(violationCount)
                    .setBaselineLowerBound(baselineLowerBound)
                    .setBaselineUpperBound(baselineUpperBound)
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
