package org.hypertrace.alert.engine.metric.anomaly.detector;

import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.queue.KafkaConfigReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NotificationEventProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricAnomalyDetector.class);

  private final Producer<String, ByteBuffer> producer;
  private final KafkaConfigReader kafkaConfigReader;
  private final Properties properties;

  NotificationEventProducer(Config kafkaQueueConfig) {
    this.kafkaConfigReader = new KafkaConfigReader(kafkaQueueConfig);
    this.properties = new Properties();
    properties.putAll(kafkaConfigReader.getProducerConfig(createBaseProperties()));
    producer = new KafkaProducer<String, ByteBuffer>(properties);
  }

  public void publish(NotificationEvent notificationEvent) {
    try {
      producer.send(
          new ProducerRecord<String, ByteBuffer>(
              properties.getProperty("topic"), null, notificationEvent.toByteBuffer()));
    } catch (IOException e) {
      LOGGER.error("Exception producing messages", e);
    }
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
    baseProperties.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    baseProperties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteBufferSerializer");
    return baseProperties;
  }
}
