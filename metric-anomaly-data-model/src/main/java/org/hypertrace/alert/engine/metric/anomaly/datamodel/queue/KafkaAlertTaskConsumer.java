package org.hypertrace.alert.engine.metric.anomaly.datamodel.queue;

import com.typesafe.config.Config;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAlertTaskConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAlertTaskConsumer.class);
  private static final int CONSUMER_POLL_TIMEOUT_MS = 100;

  private final KafkaConsumer<String, AlertTask> consumer;
  private final LinkedList<ConsumerRecord<String, AlertTask>> linkedList = new LinkedList<>();

  public KafkaAlertTaskConsumer(Config kafkaQueueConfig) {
    KafkaConfigReader kafkaConfigReader = new KafkaConfigReader(kafkaQueueConfig);
    Properties props = new Properties();
    props.putAll(kafkaConfigReader.getConsumerConfig());
    consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(kafkaConfigReader.getConsumerTopicName()));
  }

  public AlertTask dequeue() throws IOException {
    if (linkedList.isEmpty()) {
      ConsumerRecords<String, AlertTask> records =
          consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT_MS));
      records.forEach(linkedList::addLast);
    }

    if (!linkedList.isEmpty()) {
      ConsumerRecord<String, AlertTask> record = linkedList.remove();
      LOGGER.info("offset = {}, key = {}", record.offset(), record.key());
      return record.value();
    }

    return null;
  }

  public void close() {
    consumer.close();
  }
}
