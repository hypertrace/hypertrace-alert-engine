package org.hypertrace.alert.engine.metric.anomaly.datamodel.queue;

import com.typesafe.config.Config;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
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
  static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

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
        new StringJoiner(".").add(CONSUMER_CONFIG).add(TOPIC_NAME_CONFIG).toString());
  }

  public String getBootstrapServer() {
    return kafkaQueueConfig.getString(BOOTSTRAP_SERVERS_CONFIG);
  }

  public Map<String, Object> getProducerConfig(Map<String, Object> baseProps) {
    return mergeProperties(baseProps, getFlatMapConfig(kafkaQueueConfig, PRODUCER_CONFIG));
  }

  public Map<String, Object> getConsumerConfig(Map<String, Object> baseProps) {
    return mergeProperties(baseProps, getFlatMapConfig(kafkaQueueConfig, CONSUMER_CONFIG));
  }

  public Map<String, Object> getProducerSerdeProperties() {
    String schemaRegistryUrl =
        new StringJoiner(".").add(CONSUMER_CONFIG).add(SCHEMA_REGISTRY_URL).toString();
    Map<String, Object> map = new HashMap<>();
    if (kafkaQueueConfig.hasPath(schemaRegistryUrl)) {
      map.put(
          KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
          kafkaQueueConfig.getConfig(schemaRegistryUrl));
      map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());
      map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());
      return map;
    }
    map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
    map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
    return map;
  }

  public Map<String, Object> getConsumerSerdeProperties() {
    String schemaRegistryUrl =
        new StringJoiner(".").add(CONSUMER_CONFIG).add(SCHEMA_REGISTRY_URL).toString();
    Map<String, Object> map = new HashMap<>();
    if (kafkaQueueConfig.hasPath(schemaRegistryUrl)) {
      map.put(
          KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
          kafkaQueueConfig.getConfig(schemaRegistryUrl));
      map.put(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer.class.getName());
      map.put(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer.class.getName());
      return map;
    }
    map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class.getName());
    map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class.getName());
    return map;
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
}
