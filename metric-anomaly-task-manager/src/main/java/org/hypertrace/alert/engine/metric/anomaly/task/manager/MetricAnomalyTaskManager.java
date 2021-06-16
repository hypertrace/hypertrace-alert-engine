package org.hypertrace.alert.engine.metric.anomaly.task.manager;

import org.hypertrace.core.serviceframework.PlatformService;

public class MetricAnomalyTaskManager extends PlatformService {

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
