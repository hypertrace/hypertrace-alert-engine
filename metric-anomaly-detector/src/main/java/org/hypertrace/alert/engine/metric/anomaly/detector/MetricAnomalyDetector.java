package org.hypertrace.alert.engine.metric.anomaly.detector;


import org.hypertrace.alert.engine.eventcondition.config.service.v1.AlertTask;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;

class MetricAnomalyDetector {

  private MetricQueryBuilder metricQueryBuilder;

  public MetricAnomalyDetector() {
  }

  void process(AlertTask alertTask) {
    metricQueryBuilder.buildMetricQueryRequest(alertTask.g, )
  }
}