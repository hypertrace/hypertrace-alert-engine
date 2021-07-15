package org.hypertrace.alert.engine.metric.anomaly.detector;

import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.Attribute;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.ViolationCondition;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.EventRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyNotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
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

class MetricAnomalyDetector {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricAnomalyDetector.class);

  private static final String METRIC_ANOMALY_ACTION_EVENT_TYPE = "MetricAnomalyViolation";
  private static final String QUERY_SERVICE_CONFIG_KEY = "query.service.config";
  private static final String REQUEST_TIMEOUT_CONFIG_KEY = "request.timeout";
  private static final int DEFAULT_REQUEST_TIMEOUT_MILLIS = 10000;
  private static final String METRIC_ANOMALY_EVENT_CONDITION = "MetricAnomalyEventCondition";
  static final String TENANT_ID_KEY = "x-tenant-id";

  private final MetricQueryBuilder metricQueryBuilder;
  private final QueryServiceClient queryServiceClient;
  private final int qsRequestTimeout;
  private final NotificationEventProducer eventProducer;

  MetricAnomalyDetector(Config appConfig, NotificationEventProducer notificationEventProducer) {
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

    this.eventProducer = notificationEventProducer;
  }

  void process(AlertTask alertTask) throws IOException {
    MetricAnomalyEventCondition metricAnomalyEventCondition;
    if (alertTask.getEventConditionType().equals(METRIC_ANOMALY_EVENT_CONDITION)) {
      try {
        metricAnomalyEventCondition =
            MetricAnomalyEventCondition.parseFrom(alertTask.getEventConditionValue());
      } catch (InvalidProtocolBufferException e) {
        LOGGER.error("Exception while parsing event condition", e);
        return;
      }
    } else {
      LOGGER.debug(
          "Not processing alert task of EventConditionType: {}", alertTask.getEventConditionType());
      return;
    }

    if (metricAnomalyEventCondition.getViolationConditionList().isEmpty()) {
      LOGGER.info(
          "Received rule with empty violation conditions. tenantId: {}, eventConditionId: {}",
          alertTask.getTenantId(),
          alertTask.getEventConditionId());
      return;
    }

    QueryRequest queryRequest =
        metricQueryBuilder.buildMetricQueryRequest(
            metricAnomalyEventCondition.getMetricSelection(),
            alertTask.getLastExecutionTime(),
            alertTask.getCurrentExecutionTime(),
            alertTask.getTenantId());

    // todo handle multiple violation conditions
    ViolationCondition violationCondition =
        metricAnomalyEventCondition.getViolationConditionList().get(0);

    Iterator<ResultSetChunk> iterator =
        executeQuery(Map.of(TENANT_ID_KEY, alertTask.getTenantId()), queryRequest);

    LOGGER.debug(
        "Starting rule evaluation for rule Id {} start {} & end time {}",
        alertTask.getEventConditionId(),
        Instant.ofEpochMilli(alertTask.getLastExecutionTime()),
        Instant.ofEpochMilli(alertTask.getCurrentExecutionTime()));

    if (violationCondition.hasStaticThresholdCondition()) {
      EvaluationResult evaluationResult =
          evaluateForStaticThreshold(
              iterator,
              violationCondition,
              metricAnomalyEventCondition.getMetricSelection().getMetricAttribute());
      if (evaluationResult.isViolation()) {

        MetricAnomalyNotificationEvent metricAnomalyNotificationEvent =
            MetricAnomalyNotificationEvent.newBuilder()
                .setViolationTimestamp(alertTask.getCurrentExecutionTime())
                .setChannelId(metricAnomalyEventCondition.getChannelId())
                .setEventConditionId(alertTask.getEventConditionId())
                .setEventConditionType(alertTask.getEventConditionType())
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
                .setActionEventMetadata(Map.of())
                .setEventTimeMillis(alertTask.getCurrentExecutionTime())
                .setEventRecord(eventRecord)
                .build();
        eventProducer.publish(notificationEvent);
      }
    }
  }

  EvaluationResult evaluateForStaticThreshold(
      Iterator<ResultSetChunk> iterator,
      ViolationCondition violationCondition,
      Attribute attribute) {
    int dataCount = 0, violationCount = 0;
    while (iterator.hasNext()) {
      ResultSetChunk resultSetChunk = iterator.next();
      int metricDataColumnIndex = -1;
      for (Row row : resultSetChunk.getRowList()) {

        if (metricDataColumnIndex == -1 && resultSetChunk.hasResultSetMetadata()) {
          for (int i = 0; i < resultSetChunk.getResultSetMetadata().getColumnMetadataCount(); i++) {
            if (resultSetChunk
                .getResultSetMetadata()
                .getColumnMetadata(i)
                .getColumnName()
                .equals(String.join(".", attribute.getScope(), attribute.getKey()))) {
              metricDataColumnIndex = i;
              break;
            }
          }
        }

        if (metricDataColumnIndex == -1) {
          LOGGER.warn("Couldn't find the requested metric data column in result");
          continue;
        }

        Value value = row.getColumn(metricDataColumnIndex);
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

    if (dataCount > 0 && violationCount == dataCount) {
      LOGGER.debug("Rule violated. dataCount {}, violationCount {}", dataCount, violationCount);
    } else {
      LOGGER.debug("Rule normal. dataCount {} violationCount {}", dataCount, violationCount);
    }

    return EvaluationResult.builder()
        .dataCount(dataCount)
        .violationCount(violationCount)
        .isViolation(dataCount == violationCount)
        .build();
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

    switch (thresholdCondition.getOperator()) {
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
            "Unsupported threshold condition operator: " + thresholdCondition.getOperator());
    }
  }

  Iterator<ResultSetChunk> executeQuery(
      Map<String, String> requestHeaders, QueryRequest aggQueryRequest) {
    return queryServiceClient.executeQuery(aggQueryRequest, requestHeaders, qsRequestTimeout);
  }
}
