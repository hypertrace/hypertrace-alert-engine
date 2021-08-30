package org.hypertrace.alert.engine.metric.anomaly.detector.evaluator;

import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.Config;
import io.micrometer.core.instrument.Counter;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.ViolationCondition;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
import org.hypertrace.core.attribute.service.client.AttributeServiceClient;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertRuleEvaluator {

  private static final Logger LOGGER = LoggerFactory.getLogger(AlertRuleEvaluator.class);
  static final String METRIC_ANOMALY_ACTION_EVENT_TYPE = "MetricAnomalyViolation";

  private static final String METRIC_ANOMALY_EVENT_CONDITION = "MetricAnomalyEventCondition";
  private static final ConcurrentMap<String, Counter> staticRuleCounter = new ConcurrentHashMap<>();
  private static final String STATIC_RULE_COUNTER =
      "hypertrace.metric.anomaly.detector.static.rule.count";
  private static final ConcurrentMap<String, Counter> baselineRuleCounter =
      new ConcurrentHashMap<>();
  private static final String BASELINE_RULE_COUNTER =
      "hypertrace.metric.anomaly.detector.baseline.rule.count";
  private final StaticRuleEvaluator staticRuleEvaluator;
  private final BaselineRuleEvaluator baselineRuleEvaluator;

  public AlertRuleEvaluator(Config appConfig) {
    QueryRequestHandler queryRequestHandler = new QueryRequestHandler(appConfig);
    MetricCache metricCache = new MetricCache(queryRequestHandler);
    staticRuleEvaluator = new StaticRuleEvaluator(metricCache);
    baselineRuleEvaluator = new BaselineRuleEvaluator(metricCache);
  }

  // used for testing with mock clients passed as parameters
  AlertRuleEvaluator(
      Config appConfig, AttributeServiceClient asClient, QueryServiceClient queryServiceClient) {
    QueryRequestHandler queryRequestHandler =
        new QueryRequestHandler(appConfig, queryServiceClient, asClient);
    MetricCache metricCache = new MetricCache(queryRequestHandler);
    staticRuleEvaluator = new StaticRuleEvaluator(metricCache);
    baselineRuleEvaluator = new BaselineRuleEvaluator(metricCache);
  }

  public Optional<NotificationEvent> process(AlertTask alertTask) throws IOException {
    MetricAnomalyEventCondition metricAnomalyEventCondition;

    if (alertTask.getEventConditionType().equals(METRIC_ANOMALY_EVENT_CONDITION)) {
      try {
        metricAnomalyEventCondition =
            MetricAnomalyEventCondition.parseFrom(alertTask.getEventConditionValue());
      } catch (InvalidProtocolBufferException e) {
        LOGGER.error("Exception while parsing event condition", e);
        return Optional.empty();
      }
    } else {
      LOGGER.debug(
          "Not processing alert task of EventConditionType: {}", alertTask.getEventConditionType());
      return Optional.empty();
    }

    if (metricAnomalyEventCondition.getViolationConditionList().isEmpty()) {
      LOGGER.debug(
          "Received rule with empty violation conditions. tenantId: {}, eventConditionId: {}",
          alertTask.getTenantId(),
          alertTask.getEventConditionId());
      return Optional.empty();
    }

    LOGGER.debug(
        "Starting rule evaluation for rule Id {} start {} & end time {}",
        alertTask.getEventConditionId(),
        Instant.ofEpochMilli(alertTask.getLastExecutionTime()),
        Instant.ofEpochMilli(alertTask.getCurrentExecutionTime()));

    // todo handle multiple violation conditions
    ViolationCondition violationCondition =
        metricAnomalyEventCondition.getViolationConditionList().get(0);

    switch (violationCondition.getConditionCase()) {
      case STATIC_THRESHOLD_CONDITION:
        staticRuleCounter
            .computeIfAbsent(
                alertTask.getTenantId(),
                k ->
                    PlatformMetricsRegistry.registerCounter(
                        STATIC_RULE_COUNTER, Map.of("tenantId", k)))
            .increment();
        return staticRuleEvaluator.evaluateRule(metricAnomalyEventCondition, alertTask);
      case BASELINE_THRESHOLD_CONDITION:
        baselineRuleCounter
            .computeIfAbsent(
                alertTask.getTenantId(),
                k ->
                    PlatformMetricsRegistry.registerCounter(
                        BASELINE_RULE_COUNTER, Map.of("tenantId", k)))
            .increment();
        return baselineRuleEvaluator.evaluateRule(metricAnomalyEventCondition, alertTask);
    }

    return Optional.empty();
  }
}
