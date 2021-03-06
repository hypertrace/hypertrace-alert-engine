package org.hypertrace.alert.engine.metric.anomaly.detector.evaluator;

import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Iterator;
import java.util.Map;
import org.hypertrace.alert.engine.metric.anomaly.detector.MetricQueryBuilder;
import org.hypertrace.alerting.config.service.v1.MetricSelection;
import org.hypertrace.core.attribute.service.client.AttributeServiceClient;
import org.hypertrace.core.attribute.service.client.config.AttributeServiceClientConfig;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.core.query.service.client.QueryServiceConfig;

public class QueryRequestHandler {

  private static final String QUERY_SERVICE_CONFIG_KEY = "query.service.config";
  private static final String REQUEST_TIMEOUT_CONFIG_KEY = "request.timeout";
  private static final int DEFAULT_REQUEST_TIMEOUT_MILLIS = 10000;

  private final int qsRequestTimeout;
  private final QueryServiceClient queryServiceClient;
  private final MetricQueryBuilder metricQueryBuilder;

  public QueryRequestHandler(Config appConfig) {
    Config qsConfig = appConfig.getConfig(QUERY_SERVICE_CONFIG_KEY);
    queryServiceClient = new QueryServiceClient(new QueryServiceConfig(qsConfig));
    qsRequestTimeout =
        appConfig.hasPath(REQUEST_TIMEOUT_CONFIG_KEY)
            ? appConfig.getInt(REQUEST_TIMEOUT_CONFIG_KEY)
            : DEFAULT_REQUEST_TIMEOUT_MILLIS;

    AttributeServiceClientConfig asConfig = AttributeServiceClientConfig.from(appConfig);
    ManagedChannel attributeServiceChannel =
        ManagedChannelBuilder.forAddress(asConfig.getHost(), asConfig.getPort())
            .usePlaintext()
            .build();
    AttributeServiceClient asClient = new AttributeServiceClient(attributeServiceChannel);
    metricQueryBuilder = new MetricQueryBuilder(asClient);
  }

  public QueryRequestHandler(
      Config appConfig, QueryServiceClient queryServiceClient, AttributeServiceClient asClient) {
    this.queryServiceClient = queryServiceClient;
    this.metricQueryBuilder = new MetricQueryBuilder(asClient);
    this.qsRequestTimeout =
        appConfig.hasPath(REQUEST_TIMEOUT_CONFIG_KEY)
            ? appConfig.getInt(REQUEST_TIMEOUT_CONFIG_KEY)
            : DEFAULT_REQUEST_TIMEOUT_MILLIS;
  }

  Iterator<ResultSetChunk> executeQuery(
      Map<String, String> requestHeaders,
      MetricSelection metricSelection,
      String tenantId,
      long startTime,
      long endTime) {
    return queryServiceClient.executeQuery(
        metricQueryBuilder.buildMetricQueryRequest(metricSelection, startTime, endTime, tenantId),
        requestHeaders,
        qsRequestTimeout);
  }
}
