package org.hypertrace.alert.engine.notification.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyNotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.queue.KafkaConfigReader;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationEventProcessorService extends PlatformService {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NotificationEventProcessorService.class);

  private static final int CONSUMER_POLL_TIMEOUT_MS = 100;

  private final KafkaConfigReader kafkaConfigReader;
  private final KafkaConsumer<String, ByteBuffer> consumer;
  private final NotificationEventProcessor notificationEventProcessor;

  public NotificationEventProcessorService(ConfigClient configClient) {
    super(configClient);
    notificationEventProcessor =
        new NotificationEventProcessor(
            getAppConfig().getConfig(NotificationChannelsReader.NOTIIFICATION_CHANNELS_SOURCE),
            getLifecycle());
    this.kafkaConfigReader = new KafkaConfigReader(getAppConfig().getConfig("queue.config.kafka"));
    Properties props = new Properties();
    props.putAll(kafkaConfigReader.getConsumerConfig(createBaseProperties()));
    consumer = new KafkaConsumer<String, ByteBuffer>(props);
    consumer.subscribe(Collections.singletonList(kafkaConfigReader.getTopicName()));
  }

  @Override
  protected void doInit() {}

  @Override
  protected void doStart() {
    while (true) {
      try {
        ConsumerRecords<String, ByteBuffer> records =
            consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT_MS));
        for (ConsumerRecord<String, ByteBuffer> record : records) {
          notificationEventProcessor.process(
              MetricAnomalyNotificationEvent.fromByteBuffer(record.value()));
        }
      } catch (IOException e) {
        LOGGER.error("Exception processing record", e);
      }
    }
  }

  @Override
  protected void doStop() {
    consumer.close();
  }

  @Override
  public boolean healthCheck() {
    return true;
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
