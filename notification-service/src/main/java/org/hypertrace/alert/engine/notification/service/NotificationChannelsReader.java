package org.hypertrace.alert.engine.notification.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hypertrace.alert.engine.notification.service.NotificationChannel.NotificationChannelConfig;
import org.hypertrace.alert.engine.notification.service.NotificationChannel.WebFormatNotificationChannelConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationChannelsReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(NotificationChannelsReader.class);
  private static final String PATH_CONFIG = "notification.channels.path";
  private static final String CHANNEL_ID = "channelId";
  private static final String CHANNEL_NAME = "channelName";
  private static final String CHANNEL_CONFIG = "channelConfig";
  private static final String CHANNEL_CONFIG_TYPE = "channelConfigType";
  private static final String WEBFORMAT_CHANNEL_CONFIG_URL = "url";
  private static final String WEBFORMAT_CHANNEL_CONFIG_WEBHOOK_FORMAT = "webhookFormat";
  public static final String CHANNEL_CONFIG_TYPE_WEBHOOK = "WEBHOOK";
  public static final String WEBHOOK_FORMAT_SLACK = "WEBHOOK_FORMAT_SLACK";
  public static final String WEBHOOK_FORMAT_JSON = "WEBHOOK_FORMAT_JSON";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static List<NotificationChannel> readNotificationChannels(Config config)
      throws IOException {
    return getJsonNodes(config, PATH_CONFIG, LOGGER).stream()
        .map(
            node ->
                NotificationChannel.builder()
                    .channelId(node.get(CHANNEL_ID).asText())
                    .channelName(node.get(CHANNEL_NAME).asText())
                    .notificationChannelConfig(getChannelConfigs(node))
                    .build())
        .collect(Collectors.toList());
  }

  private static List<NotificationChannelConfig> getChannelConfigs(JsonNode node) {
    return StreamSupport.stream(node.get(CHANNEL_CONFIG).spliterator(), false)
        .filter(
            channelConfigNode ->
                channelConfigNode
                    .get(CHANNEL_CONFIG_TYPE)
                    .asText()
                    .equals(CHANNEL_CONFIG_TYPE_WEBHOOK))
        .map(
            webFormatChannelConfig ->
                WebFormatNotificationChannelConfig.builder()
                    .url(webFormatChannelConfig.get(WEBFORMAT_CHANNEL_CONFIG_URL).asText())
                    .webhookFormat(
                        webFormatChannelConfig
                            .get(WEBFORMAT_CHANNEL_CONFIG_WEBHOOK_FORMAT)
                            .asText())
                    .channelConfigType(webFormatChannelConfig.get(CHANNEL_CONFIG_TYPE).asText())
                    .build())
        .collect(Collectors.toList());
  }

  public static List<JsonNode> getJsonNodes(Config config, String pathConfig, Logger logger)
      throws IOException {
    String fsPath = config.getString(pathConfig);
    logger.debug("Reading rules from file path:{}", fsPath);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(new File(fsPath).getAbsoluteFile());
    if (!jsonNode.isArray()) {
      throw new IOException("File should contain an array of notification rules");
    }

    logger.info("Reading rules {}", jsonNode.toPrettyString());
    return StreamSupport.stream(jsonNode.spliterator(), false)
        .collect(Collectors.toUnmodifiableList());
  }
}
