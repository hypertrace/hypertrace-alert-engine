package org.hypertrace.alert.engine.metric.anomaly.datamodel.queue;

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

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
        new StringJoiner(".")
            .add(CONSUMER_CONFIG)
            .add(TOPIC_NAME_CONFIG).toString());
  }

  public String getProducerTopicName() {
    return kafkaQueueConfig.getString(
        new StringJoiner(".")
            .add(CONSUMER_CONFIG)
            .add(TOPIC_NAME_CONFIG).toString());
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

  private Map<String, Object> getFlatMapConfig(Config config, String path) {
    Map<String, Object> propertiesMap = new HashMap<>();
    Config subConfig = config.getConfig(path);
    subConfig.entrySet()
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
