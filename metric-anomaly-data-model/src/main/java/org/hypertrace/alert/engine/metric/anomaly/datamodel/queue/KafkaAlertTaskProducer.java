package org.hypertrace.alert.engine.metric.anomaly.datamodel.queue;

import com.typesafe.config.Config;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;

public class KafkaAlertTaskProducer {

  private final Producer<String, AlertTask> producer;
  private final KafkaConfigReader kafkaConfigReader;

  public KafkaAlertTaskProducer(Config kafkaQueueConfig) {
    this.kafkaConfigReader = new KafkaConfigReader(kafkaQueueConfig);
    Properties props = new Properties();
    props.putAll(kafkaConfigReader.getProducerConfig());
    producer = new KafkaProducer<>(props);
  }

  public void enqueue(AlertTask alertTask) throws IOException {
    producer.send(new ProducerRecord<>(kafkaConfigReader.getProducerTopicName(), null, alertTask));
  }

  public void close() {
    producer.close();
  }
}
