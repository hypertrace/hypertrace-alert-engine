package org.hypertrace.alert.engine.metric.anomaly.datamodel.queue;

import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAlertTaskConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAlertTaskConsumer.class);
  private static final int CONSUMER_POLL_TIMEOUT_MS = 100;

  private final KafkaConfigReader kafkaConfigReader;
  private final KafkaConsumer<String, ByteBuffer> consumer;
  private final LinkedList<ConsumerRecord<String, ByteBuffer>> linkedList = new LinkedList<>();

  public KafkaAlertTaskConsumer(Config kafkaQueueConfig) {
    this.kafkaConfigReader = new KafkaConfigReader(kafkaQueueConfig);
    Properties props = new Properties();
    props.putAll(kafkaConfigReader.getConsumerConfig(createBaseProperties()));
    consumer = new KafkaConsumer<String, ByteBuffer>(props);
    consumer.subscribe(Collections.singletonList(kafkaConfigReader.getTopicName()));
  }

  public AlertTask dequeue() throws IOException {
    if (linkedList.isEmpty()) {
      ConsumerRecords<String, ByteBuffer> records =
          consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT_MS));
      records.forEach(record -> linkedList.addLast(record));
    }

    if (!linkedList.isEmpty()) {
      ConsumerRecord<String, ByteBuffer> record = linkedList.remove();
      LOGGER.info("offset = {}, key = {}, value = {}", record.offset(), record.key());
      return AlertTask.fromByteBuffer(record.value());
    }

    return null;
  }

  public void close() {
    consumer.close();
  }

  private Map<String, Object> createBaseProperties() {
    Map<String, Object> baseProperties = new HashMap<>();
    baseProperties.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigReader.getBootstrapServer());
    baseProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "alert-task-consumer");
    baseProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    baseProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    baseProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    baseProperties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    baseProperties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteBufferDeserializer");
    return baseProperties;
  }
}
