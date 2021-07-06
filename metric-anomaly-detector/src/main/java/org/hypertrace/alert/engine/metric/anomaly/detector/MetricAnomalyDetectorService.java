package org.hypertrace.alert.engine.metric.anomaly.detector;

import java.io.IOException;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.queue.KafkaAlertTaskConsumer;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricAnomalyDetectorService extends PlatformService {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricAnomalyDetectorService.class);

  KafkaAlertTaskConsumer alertTaskConsumer;
  MetricAnomalyDetector metricAnomalyDetector;

  public MetricAnomalyDetectorService(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    alertTaskConsumer = new KafkaAlertTaskConsumer(getAppConfig().getConfig("queue.config.kafka"));
    metricAnomalyDetector = new MetricAnomalyDetector(getAppConfig());
  }

  @Override
  protected void doStart() {
    while (true) {
      try {
        AlertTask alertTask = alertTaskConsumer.dequeue();
        if (alertTask != null) {
          metricAnomalyDetector.process(alertTask);
        }
      } catch (IOException e) {
        LOGGER.error("Exception processing record", e);
      }
    }
  }

  @Override
  protected void doStop() {
    alertTaskConsumer.close();
  }

  @Override
  public boolean healthCheck() {
    return true;
  }
}
