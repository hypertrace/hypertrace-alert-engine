package org.hypertrace.alert.engine.metric.anomaly.detector;

import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Iterator;
import java.util.Map;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.ViolationCondition;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
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

  private static final String QUERY_SERVICE_CONFIG_KEY = "query.service.config";
  private static final String REQUEST_TIMEOUT_CONFIG_KEY = "request.timeout";
  private static final int DEFAULT_REQUEST_TIMEOUT_MILLIS = 10000;

  private final MetricQueryBuilder metricQueryBuilder;
  private final QueryServiceClient queryServiceClient;
  private final int qsRequestTimeout;

  public MetricAnomalyDetector(Config appConfig) {
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

  void process(AlertTask alertTask) {
    MetricAnomalyEventCondition metricAnomalyEventCondition;
    if (alertTask.getEventConditionType().equals("MetricAnomalyEventCondition")) {
      try {
        metricAnomalyEventCondition =
            MetricAnomalyEventCondition.parseFrom(alertTask.getEventConditionValue());
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
        return;
      }
    } else {
      LOGGER.info("EventConditionType:{}", alertTask.getEventConditionType());
      return;
    }
    QueryRequest queryRequest =
        metricQueryBuilder.buildMetricQueryRequest(
            metricAnomalyEventCondition.getMetricSelection(),
            alertTask.getLastExecutionTime(),
            alertTask.getCurrentExecutionTime());

    ViolationCondition violationCondition =
        metricAnomalyEventCondition.getViolationConditionList().get(0);

    boolean isViolation = true;
    Iterator<ResultSetChunk> iterator = executeQuery(Map.of("x-tenant-id", "default"), queryRequest);
    while (iterator.hasNext()) {
      ResultSetChunk resultSetChunk = iterator.next();
      for (Row row : resultSetChunk.getRowList()) {
        Value value = row.getColumn(1);
        if (value.getValueType() != ValueType.STRING) {
          throw new IllegalArgumentException("");
        }
        if (!compareThreshold(value, violationCondition)) {
          isViolation = false;
          break;
        }
      }
    }

    if (isViolation) {
      LOGGER.info("Rule violation");
    }
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
        return false;
    }
  }

  Iterator<ResultSetChunk> executeQuery(
      Map<String, String> requestHeaders, QueryRequest aggQueryRequest) {
    return queryServiceClient.executeQuery(aggQueryRequest, requestHeaders, qsRequestTimeout);
  }
}
