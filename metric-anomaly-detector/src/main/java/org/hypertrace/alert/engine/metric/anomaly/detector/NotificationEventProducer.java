package org.hypertrace.alert.engine.metric.anomaly.detector;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyNotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.queue.KafkaConfigReader;

class NotificationEventProducer {

  private final Producer<String, MetricAnomalyNotificationEvent> producer;
  private final KafkaConfigReader kafkaConfigReader;
  private final Properties properties;

  NotificationEventProducer(Config kafkaQueueConfig) {
    this.kafkaConfigReader = new KafkaConfigReader(kafkaQueueConfig);
    this.properties = new Properties();
    properties.putAll(kafkaConfigReader.getProducerConfig(createBaseProperties()));
    producer = new KafkaProducer<>(properties);
  }

  public void publish(MetricAnomalyNotificationEvent notificationEvent) {
    producer.send(new ProducerRecord<>(properties.getProperty("topic"), null, notificationEvent));
  }

  public void close() {
    producer.close();
  }

  private Map<String, Object> createBaseProperties() {
    Map<String, Object> baseProperties = new HashMap<>();
    baseProperties.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigReader.getBootstrapServer());
    baseProperties.put(ProducerConfig.ACKS_CONFIG, "all");
    baseProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    baseProperties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    baseProperties.putAll(kafkaConfigReader.getProducerSerdeProperties());
    return baseProperties;
  }
}
