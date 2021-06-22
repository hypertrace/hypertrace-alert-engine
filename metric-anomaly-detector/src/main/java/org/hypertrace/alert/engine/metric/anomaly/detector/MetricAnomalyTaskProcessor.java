package org.hypertrace.alert.engine.metric.anomaly.detector;

import java.util.Iterator;
import java.util.Map;
import org.hypertrace.alert.engine.metric.anomaly.rule.config.service.v1.MetricAnomalyAlertRule;
import org.hypertrace.core.query.service.api.ResultSetChunk;

public class MetricAnomalyTaskProcessor {

  private final MetricDataFetcher metricDataFetcher;
  private final MetricQueryBuilder metricQueryBuilder;

  public MetricAnomalyTaskProcessor(
      MetricDataFetcher metricDataFetcher, MetricQueryBuilder metricQueryBuilder) {
    this.metricDataFetcher = metricDataFetcher;
    this.metricQueryBuilder = metricQueryBuilder;
  }

  void process(MetricAnomalyAlertRule metricAnomalyAlertRule) {
    Iterator<ResultSetChunk> iterator =
        metricDataFetcher.executeQuery(
            Map.of(),
            metricQueryBuilder.buildMetricQueryRequest(
                metricAnomalyAlertRule.getMetricSelection()));
  }
}
