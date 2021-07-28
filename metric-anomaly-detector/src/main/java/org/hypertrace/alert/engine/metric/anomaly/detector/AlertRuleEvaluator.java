package org.hypertrace.alert.engine.metric.anomaly.detector;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.ViolationCondition;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.EventRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyNotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.Operator;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.ViolationSummary;
import org.hypertrace.core.attribute.service.client.AttributeServiceClient;
import org.hypertrace.core.attribute.service.client.config.AttributeServiceClientConfig;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.core.query.service.client.QueryServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertRuleEvaluator {

  private static final Logger LOGGER = LoggerFactory.getLogger(AlertRuleEvaluator.class);
  private static final String METRIC_ANOMALY_ACTION_EVENT_TYPE = "MetricAnomalyViolation";
  private static final String QUERY_SERVICE_CONFIG_KEY = "query.service.config";
  private static final String REQUEST_TIMEOUT_CONFIG_KEY = "request.timeout";
  private static final String METRIC_ANOMALY_EVENT_CONDITION = "MetricAnomalyEventCondition";
  private static final int DEFAULT_REQUEST_TIMEOUT_MILLIS = 10000;
  static final String TENANT_ID_KEY = "x-tenant-id";

  private final MetricQueryBuilder metricQueryBuilder;
  private final QueryServiceClient queryServiceClient;
  private final int qsRequestTimeout;
  private final Multimap<Double, Double> violationSummaryMap = ArrayListMultimap.create();

  public AlertRuleEvaluator(Config appConfig) {
    AttributeServiceClientConfig asConfig = AttributeServiceClientConfig.from(appConfig);
    ManagedChannel attributeServiceChannel =
        ManagedChannelBuilder.forAddress(asConfig.getHost(), asConfig.getPort())
            .usePlaintext()
            .build();
    AttributeServiceClient asClient = new AttributeServiceClient(attributeServiceChannel);

    Config qsConfig = appConfig.getConfig(QUERY_SERVICE_CONFIG_KEY);
    queryServiceClient = new QueryServiceClient(new QueryServiceConfig(qsConfig));
    qsRequestTimeout =
        appConfig.hasPath(REQUEST_TIMEOUT_CONFIG_KEY)
            ? appConfig.getInt(REQUEST_TIMEOUT_CONFIG_KEY)
            : DEFAULT_REQUEST_TIMEOUT_MILLIS;

    metricQueryBuilder = new MetricQueryBuilder(asClient);
  }

  // used for testing with mock clients passed as parameters
  AlertRuleEvaluator(
      Config appConfig, AttributeServiceClient asClient, QueryServiceClient queryServiceClient) {
    this.queryServiceClient = queryServiceClient;
    qsRequestTimeout =
        appConfig.hasPath(REQUEST_TIMEOUT_CONFIG_KEY)
            ? appConfig.getInt(REQUEST_TIMEOUT_CONFIG_KEY)
            : DEFAULT_REQUEST_TIMEOUT_MILLIS;
    metricQueryBuilder = new MetricQueryBuilder(asClient);
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

    QueryRequest queryRequest = getQueryRequest(metricAnomalyEventCondition, alertTask);
    LOGGER.debug("Query request {}", queryRequest);

    Iterator<ResultSetChunk> iterator =
        executeQuery(Map.of(TENANT_ID_KEY, alertTask.getTenantId()), queryRequest);

    LOGGER.debug(
        "Starting rule evaluation for rule Id {} start {} & end time {}",
        alertTask.getEventConditionId(),
        Instant.ofEpochMilli(alertTask.getLastExecutionTime()),
        Instant.ofEpochMilli(alertTask.getCurrentExecutionTime()));

    // todo handle multiple violation conditions
    ViolationCondition violationCondition =
        metricAnomalyEventCondition.getViolationConditionList().get(0);

    if (violationCondition.hasStaticThresholdCondition()) {
      EvaluationResult evaluationResult = evaluateForStaticThreshold(iterator, violationCondition);
      LOGGER.debug(
          "Eval result {} {} {}",
          evaluationResult.isViolation(),
          evaluationResult.getDataCount(),
          evaluationResult.getViolationCount());
      if (evaluationResult.isViolation()) {
        return getNotificationEvent(
            alertTask,
            evaluationResult.dataCount,
            evaluationResult.violationCount,
            staticThresholdOperatorToOperatorConvertor(
                violationCondition.getStaticThresholdCondition().getOperator()));
      }
    }

    return Optional.empty();
  }

  EvaluationResult evaluateForStaticThreshold(
      Iterator<ResultSetChunk> iterator, ViolationCondition violationCondition) {
    int dataCount = 0, violationCount = 0;
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
        if (compareThreshold(value, violationCondition)) {
          violationCount++;
        }
      }
    }

    if (isViolation(dataCount, violationCount)) {
      LOGGER.debug("Rule violated. dataCount {}, violationCount {}", dataCount, violationCount);
    } else {
      LOGGER.debug("Rule normal. dataCount {} violationCount {}", dataCount, violationCount);
    }

    return EvaluationResult.builder()
        .dataCount(dataCount)
        .violationCount(violationCount)
        .isViolation(isViolation(dataCount, violationCount))
        .build();
  }

  private boolean isViolation(int dataCount, int violationCount) {
    return dataCount > 0 && (dataCount == violationCount);
  }

  @SuperBuilder
  @Getter
  private static class EvaluationResult {
    private final int violationCount;
    private final int dataCount;
    private final boolean isViolation;
  }

  boolean compareThreshold(Value value, ViolationCondition violationCondition) {
    StaticThresholdCondition thresholdCondition = violationCondition.getStaticThresholdCondition();
    double lhs = Double.parseDouble(value.getString());
    double rhs = thresholdCondition.getValue();
    boolean isViolation = evalOperator(thresholdCondition.getOperator(), lhs, rhs);

    if (isViolation) {
      violationSummaryMap.put(rhs, lhs);
    }

    return isViolation;
  }

  Iterator<ResultSetChunk> executeQuery(
      Map<String, String> requestHeaders, QueryRequest aggQueryRequest) {
    return queryServiceClient.executeQuery(aggQueryRequest, requestHeaders, qsRequestTimeout);
  }

  private QueryRequest getQueryRequest(
      MetricAnomalyEventCondition metricAnomalyEventCondition, AlertTask alertTask) {
    return metricQueryBuilder.buildMetricQueryRequest(
        metricAnomalyEventCondition.getMetricSelection(),
        alertTask.getLastExecutionTime(),
        alertTask.getCurrentExecutionTime(),
        alertTask.getTenantId());
  }

  private Optional<NotificationEvent> getNotificationEvent(
      AlertTask alertTask, int dataCount, int violationCount, Operator operator)
      throws IOException {

    List<ViolationSummary> violationSummaryList = new ArrayList<>();
    violationSummaryMap
        .asMap()
        .forEach(
            (key, collection) -> {
              violationSummaryList.add(
                  ViolationSummary.newBuilder()
                      .setLhs(new ArrayList<>(collection))
                      .setRhs(key)
                      .setDataCount(dataCount)
                      .setViolationCount(violationCount)
                      .setOperator(operator)
                      .build());
            });

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
            .setEventType(METRIC_ANOMALY_ACTION_EVENT_TYPE)
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

  private Operator staticThresholdOperatorToOperatorConvertor(
      org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdOperator
          operator) {
    switch (operator) {
      case STATIC_THRESHOLD_OPERATOR_GT:
        return Operator.STATIC_THRESHOLD_OPERATOR_GT;
      case STATIC_THRESHOLD_OPERATOR_LT:
        return Operator.STATIC_THRESHOLD_OPERATOR_LT;
      case STATIC_THRESHOLD_OPERATOR_GTE:
        return Operator.STATIC_THRESHOLD_OPERATOR_GTE;
      case STATIC_THRESHOLD_OPERATOR_LTE:
        return Operator.STATIC_THRESHOLD_OPERATOR_LTE;
      default:
        throw new UnsupportedOperationException(
            "Unsupported threshold condition operator: " + operator);
    }
  }
}
