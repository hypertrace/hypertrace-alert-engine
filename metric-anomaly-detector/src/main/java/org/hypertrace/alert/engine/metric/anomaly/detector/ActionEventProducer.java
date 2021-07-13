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
import org.hypertrace.alert.engine.metric.anomaly.datamodel.ActionEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyViolation;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.queue.KafkaConfigReader;

class ActionEventProducer {

  private final Producer<String, ByteBuffer> producer;
  private final KafkaConfigReader kafkaConfigReader;

  ActionEventProducer(Config kafkaQueueConfig) {
    this.kafkaConfigReader = new KafkaConfigReader(kafkaQueueConfig);
    Properties props = new Properties();
    props.putAll(kafkaConfigReader.getProducerConfig(createBaseProperties()));
    producer = new KafkaProducer<String, ByteBuffer>(props);
  }

  public void publish(ActionEvent actionEvent) throws IOException {
    producer.send(
        new ProducerRecord<String, ByteBuffer>(
            kafkaConfigReader.getTopicName(), null, actionEvent.toByteBuffer()));
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
