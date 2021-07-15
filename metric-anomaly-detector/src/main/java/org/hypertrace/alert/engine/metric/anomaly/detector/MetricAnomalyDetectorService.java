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

  static final String KAFKA_QUEUE_CONFIG_KEY = "queue.config.kafka";

  private KafkaAlertTaskConsumer alertTaskConsumer;
  private MetricAnomalyDetector metricAnomalyDetector;
  private NotificationEventProducer notificationEventProducer;

  public MetricAnomalyDetectorService(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    alertTaskConsumer =
        new KafkaAlertTaskConsumer(getAppConfig().getConfig(KAFKA_QUEUE_CONFIG_KEY));
    notificationEventProducer =
        new NotificationEventProducer(getAppConfig().getConfig(KAFKA_QUEUE_CONFIG_KEY));
    metricAnomalyDetector = new MetricAnomalyDetector(getAppConfig(), notificationEventProducer);
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
    notificationEventProducer.close();
  }

  @Override
  public boolean healthCheck() {
    return true;
  }
}
