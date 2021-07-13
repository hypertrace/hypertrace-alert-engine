package org.hypertrace.alert.engine.anomaly.event.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.hypertrace.alert.engine.anomaly.event.processor.NotificationChannel.NotificationChannelConfig;
import org.hypertrace.alert.engine.anomaly.event.processor.NotificationChannel.WebFormatNotificationChannelConfig;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.ActionEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyViolation;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.queue.KafkaConfigReader;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnomalyEventProcessorService extends PlatformService {

  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyEventProcessorService.class);

  private static final int CONSUMER_POLL_TIMEOUT_MS = 100;
  private static final String PATH_CONFIG = "rules.path";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final KafkaConfigReader kafkaConfigReader;
  private final KafkaConsumer<String, ByteBuffer> consumer;
  private final AnomalyEventProcessor anomalyEventProcessor;

  public AnomalyEventProcessorService(ConfigClient configClient) throws IOException {
    super(configClient);
    List<NotificationChannel> notificationChannels = getAllRules(getAppConfig());
    anomalyEventProcessor = new AnomalyEventProcessor(notificationChannels);
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
          anomalyEventProcessor.process(ActionEvent.fromByteBuffer(record.value()));
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

  List<NotificationChannel> getAllRules(Config config) throws IOException {
    String fsPath = config.getString(PATH_CONFIG);
    LOGGER.debug("Reading rules from file path:{}", fsPath);

    JsonNode jsonNode = OBJECT_MAPPER.readTree(new File(fsPath).getAbsoluteFile());
    if (!jsonNode.isArray()) {
      throw new IOException("File should contain an array of notification rules");
    }

    List<JsonNode> nodes =
        StreamSupport.stream(jsonNode.spliterator(), false)
            .collect(Collectors.toUnmodifiableList());

    return nodes.stream()
        .map(
            node ->
                NotificationChannel.builder()
                    .channelId(node.get("channelId").asText())
                    .channelName(node.get("channelName").asText())
                    .notificationChannelConfig(getChannelConfigs(node))
                    .build())
        .collect(Collectors.toList());
  }

  List<NotificationChannelConfig> getChannelConfigs(JsonNode node) {
    return StreamSupport.stream(node.get("channelConfig").spliterator(), false)
        .filter(
            channelConfigNode ->
                channelConfigNode.get("channelConfigType").asText().equals("WEBHOOK"))
        .map(
            webFormatChannelConfig ->
                WebFormatNotificationChannelConfig.builder()
                    .url(webFormatChannelConfig.get("url").asText())
                    .webhookFormat(webFormatChannelConfig.get("webhookFormat").asText())
                    .channelConfigType(webFormatChannelConfig.get("channelConfigType").asText())
                    .build())
        .collect(Collectors.toList());
  }
}
