package org.hypertrace.alert.engine.metric.anomaly.detector;

import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Parser;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.AlertTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertTaskConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlertTaskConsumer.class);

  private Config config;
  KafkaConsumer<String, byte[]> consumer;
  String topicName;
  LinkedList<ConsumerRecord<String, byte[]>> linkedList = new LinkedList<>();

  private static final Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  public AlertTaskConsumer(Config config) {
    this.config = config;
    topicName = config.getString("output.topic");
    Properties props = createBaseProperties();
    consumer = new KafkaConsumer<String, byte[]>(props);
    consumer.subscribe(Arrays.asList(topicName));
  }

  public Optional<AlertTask> consumeTask() {
    Optional<AlertTask> optionalAlertTask = Optional.empty();
    if (linkedList.isEmpty()) {
      ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
      records.forEach(record -> linkedList.addLast(record));
    }

    if (!linkedList.isEmpty()) {
      ConsumerRecord<String, byte[]> record = linkedList.remove();
      try {
        AlertTask alertTask = AlertTask.parseFrom(record.value());
        LOGGER.info(
            "offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
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
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    return props;
  }
}
