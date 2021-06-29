package org.hypertrace.alert.engine.metric.anomaly.datamodel.queue;

import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaQueueAlertTaskProducer implements Queue<AlertTask> {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaQueueAlertTaskProducer.class);

  private Producer<String, ByteBuffer> producer;
  private KafkaQueueConfigReader kafkaQueueConfigReader;

  public void init(Config kafkaQueueConfig) {
    this.kafkaQueueConfigReader = new KafkaQueueConfigReader(kafkaQueueConfig);
    Properties props = new Properties();
    props.putAll(kafkaQueueConfigReader.getProducerConfig(createBaseProperties()));
    producer = new KafkaProducer<String, ByteBuffer>(props);
  }

  public void enqueue(AlertTask alertTask) throws IOException {
    producer.send(
        new ProducerRecord<String, ByteBuffer>(
            kafkaQueueConfigReader.getTopicName(), null, alertTask.toByteBuffer()));
  }

  public AlertTask dequeue() throws NotImplementedException {
    throw new NotImplementedException("For kafka based queue, user Consumer");
  }

  public void close() {
    producer.close();
  }

  private Map<String, Object> createBaseProperties() {
    Map<String, Object> baseProperties = new HashMap<>();
    baseProperties.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaQueueConfigReader.getBootstrapServer());
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
