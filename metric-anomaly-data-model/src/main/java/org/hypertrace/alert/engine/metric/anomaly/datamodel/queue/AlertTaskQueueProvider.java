package org.hypertrace.alert.engine.metric.anomaly.datamodel.queue;

import com.typesafe.config.Config;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;

public class AlertTaskQueueProvider {
  private static final String QUEUE_SOURCE_TYPE = "type";
  private static final String QUEUE_SOURCE_TYPE_KAFKA = "kafka";

  public static Queue<AlertTask> getProducerQueue(Config queueConfig) {
    Queue<AlertTask> alertTaskQueue;
    String queueType = queueConfig.getString(QUEUE_SOURCE_TYPE);
    switch (queueType) {
      case QUEUE_SOURCE_TYPE_KAFKA:
        Config kafkaQueueConfig = queueConfig.getConfig(QUEUE_SOURCE_TYPE_KAFKA);
        alertTaskQueue = new KafkaQueueAlertTaskProducer();
        alertTaskQueue.init(kafkaQueueConfig);
        break;
      default:
        throw new RuntimeException(String.format("Invalid queue configuration: %s", queueType));
    }
    return alertTaskQueue;
  }
}
