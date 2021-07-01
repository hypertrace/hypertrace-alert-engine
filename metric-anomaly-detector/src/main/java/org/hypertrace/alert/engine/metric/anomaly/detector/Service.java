package org.hypertrace.alert.engine.metric.anomaly.detector;

import java.util.Optional;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.core.serviceframework.PlatformService;

public class Service extends PlatformService {

  AlertTaskConsumer alertTaskConsumer;
  MetricAnomalyDetector metricAnomalyDetector;

  @Override
  protected void doInit() {
    alertTaskConsumer = new AlertTaskConsumer(getAppConfig().getConfig("queue.config.kafka"));
    metricAnomalyDetector = new MetricAnomalyDetector(getAppConfig());
  }

  @Override
  protected void doStart() {
    while (true) {
      Optional<AlertTask> optionalAlertTask = alertTaskConsumer.consumeTask();
      if (optionalAlertTask.isPresent()) {
        metricAnomalyDetector.process(optionalAlertTask.get());
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  protected void doStop() {
    alertTaskConsumer.close();
  }

  @Override
  public boolean healthCheck() {
    return false;
  }
}
