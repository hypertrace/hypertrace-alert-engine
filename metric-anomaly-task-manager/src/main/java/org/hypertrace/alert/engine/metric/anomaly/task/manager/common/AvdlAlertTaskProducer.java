package org.hypertrace.alert.engine.metric.anomaly.task.manager.common;

import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvdlAlertTaskProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvdlAlertTaskProducer.class);

  private Config config;
  Producer<String, ByteBuffer> producer;
  String topicName;

  public AvdlAlertTaskProducer(Config config) {
    this.config = config;
    topicName = config.getString("output.topic");
    Properties props = createBaseProperties();
    producer = new KafkaProducer<String, ByteBuffer>(props);
  }

  public void produceTask(AlertTask alertTask) {
    try {
      producer.send(
          new ProducerRecord<String, ByteBuffer>(topicName, null, alertTask.toByteBuffer()));
      LOGGER.info("task produced");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void close() {
    producer.close();
  }

  private Properties createBaseProperties() {
    // create instance for properties to access producer configs
    Properties props = new Properties();

    // Assign localhost id
    props.put("bootstrap.servers", config.getString("bootstrap.servers"));

    // Set acknowledgements for producer requests.
    props.put("acks", "all");

    // If the request fails, the producer can automatically retry,
    props.put("retries", 0);

    // Specify buffer size in config
    props.put("batch.size", 16384);

    // Reduce the no of requests less than 0
    props.put("linger.ms", 1);

    // The buffer.memory controls the total amount of memory available to the producer for
    // buffering.
    props.put("buffer.memory", 33554432);

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteBufferSerializer");

    return props;
  }
}
