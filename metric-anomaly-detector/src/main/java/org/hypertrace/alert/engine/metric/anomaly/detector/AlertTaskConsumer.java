package org.hypertrace.alert.engine.metric.anomaly.detector;

import com.typesafe.config.Config;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertTaskConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlertTaskConsumer.class);

  private Config config;
  KafkaConsumer<String, ByteBuffer> consumer;
  String topicName;
  LinkedList<ConsumerRecord<String, ByteBuffer>> linkedList = new LinkedList<>();

  public AlertTaskConsumer(Config config) {
    this.config = config;
    topicName = config.getString("output.topic");
    Properties props = createBaseProperties();
    consumer = new KafkaConsumer<String, ByteBuffer>(props);
    consumer.subscribe(Arrays.asList(topicName));
  }

  public Optional<AlertTask> consumeTask() {
    Optional<AlertTask> optionalAlertTask = Optional.empty();
    if (linkedList.isEmpty()) {
      ConsumerRecords<String, ByteBuffer> records = consumer.poll(Duration.ofMillis(100));
      records.forEach(record -> linkedList.addLast(record));
    }

    if (!linkedList.isEmpty()) {
      ConsumerRecord<String, ByteBuffer> record = linkedList.remove();
      try {
        AlertTask alertTask = AlertTask.fromByteBuffer(record.value());
        LOGGER.info("offset = {}, key = {}, value = {}", record.offset(), record.key());
        return Optional.of(alertTask);
      } catch (Exception e) {
        LOGGER.error(
            "Failed parsing to alert task for offset = {}, key = {}, value = {}",
            record.offset(),
            record.key(),
            record.value());
      }
    }
    return optionalAlertTask;
  }

  public void close() {
    consumer.close();
  }

  private Properties createBaseProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", config.getString("bootstrap.servers"));
    props.put("group.id", "alert-task-consumer");
    props.put("enable.auto.commit", "false");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteBufferDeserializer");
    return props;
  }
}
