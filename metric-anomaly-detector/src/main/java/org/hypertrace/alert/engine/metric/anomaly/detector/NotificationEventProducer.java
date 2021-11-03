package org.hypertrace.alert.engine.metric.anomaly.detector;

import com.typesafe.config.Config;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyNotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.queue.KafkaConfigReader;

class NotificationEventProducer {

  private final Producer<String, MetricAnomalyNotificationEvent> producer;
  private final Properties properties;

  NotificationEventProducer(Config kafkaQueueConfig) {
    KafkaConfigReader kafkaConfigReader = new KafkaConfigReader(kafkaQueueConfig);
    this.properties = new Properties();
    properties.putAll(kafkaConfigReader.getProducerConfig());
    producer = new KafkaProducer<>(properties);
  }

  public void publish(MetricAnomalyNotificationEvent notificationEvent) {
    producer.send(new ProducerRecord<>(properties.getProperty("topic"), null, notificationEvent));
  }

  public void close() {
    producer.close();
  }
}
