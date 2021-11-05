package org.hypertrace.alert.engine.metric.anomaly.datamodel.queue;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroDeserializer;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerializer;

public class KafkaConfigReader {
  static final String TOPIC_NAME_CONFIG = "topic";
  static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
  static final String PRODUCER_CONFIG = "producer";
  static final String CONSUMER_CONFIG = "consumer";

  private final Config kafkaQueueConfig;

  public KafkaConfigReader(Config kafkaQueueConfig) {
    this.kafkaQueueConfig = kafkaQueueConfig;
  }

  public String getConsumerTopicName() {
    return kafkaQueueConfig.getString(
        new StringJoiner(".").add(CONSUMER_CONFIG).add(TOPIC_NAME_CONFIG).toString());
  }

  public String getProducerTopicName() {
    return kafkaQueueConfig.getString(
        new StringJoiner(".").add(PRODUCER_CONFIG).add(TOPIC_NAME_CONFIG).toString());
  }

  public String getBootstrapServer() {
    return kafkaQueueConfig.getString(BOOTSTRAP_SERVERS_CONFIG);
  }

  public Map<String, Object> getProducerConfig() {
    return mergeProperties(
        createProducerBaseProperties(), getFlatMapConfig(kafkaQueueConfig, PRODUCER_CONFIG));
  }

  public Map<String, Object> getConsumerConfig() {
    return mergeProperties(
        createConsumerBaseProperties(), getFlatMapConfig(kafkaQueueConfig, CONSUMER_CONFIG));
  }

  private Map<String, Object> getFlatMapConfig(Config config, String path) {
    Map<String, Object> propertiesMap = new HashMap<>();
    Config subConfig = config.getConfig(path);
    subConfig
        .entrySet()
        .forEach(entry -> propertiesMap.put(entry.getKey(), subConfig.getString(entry.getKey())));
    return propertiesMap;
  }

  private Map<String, Object> mergeProperties(
      Map<String, Object> baseProps, Map<String, Object> props) {
    Objects.requireNonNull(baseProps);
    baseProps.putAll(props);
    return baseProps;
  }

  private Map<String, Object> createProducerBaseProperties() {
    Map<String, Object> baseProperties = new HashMap<>();
    baseProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServer());
    baseProperties.put(ProducerConfig.ACKS_CONFIG, "all");
    baseProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    baseProperties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    baseProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
    baseProperties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
    return baseProperties;
  }

  private Map<String, Object> createConsumerBaseProperties() {
    Map<String, Object> baseProperties = new HashMap<>();
    baseProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServer());
    baseProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "alert-task-consumer");
    baseProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    baseProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    baseProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    baseProperties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class.getName());
    baseProperties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class.getName());
    return baseProperties;
  }
}
