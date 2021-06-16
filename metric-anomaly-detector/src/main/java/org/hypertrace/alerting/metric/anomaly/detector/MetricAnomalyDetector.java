package org.hypertrace.alerting.metric.anomaly.detector;

import org.hypertrace.core.serviceframework.PlatformService;

public class MetricAnomalyDetector extends PlatformService {

  @Override
  protected void doInit() {}

  @Override
  protected void doStart() {}

  @Override
  protected void doStop() {}

  @Override
  public boolean healthCheck() {
    return false;
  }
}
