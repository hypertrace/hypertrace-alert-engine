package org.hypertrace.alert.engine.notification.service;

import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.NOTIIFICATION_CHANNELS_SOURCE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source.RuleSource;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source.RuleSourceProvider;
import org.hypertrace.alert.engine.notification.service.NotificationChannel.NotificationChannelConfig;
import org.hypertrace.alert.engine.notification.service.NotificationChannel.WebFormatNotificationChannelConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationChannelsReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(NotificationChannelsReader.class);
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
    RuleSource ruleSource =
        RuleSourceProvider.getProvider(config.getConfig(NOTIIFICATION_CHANNELS_SOURCE));
    return ruleSource.getAllRules(jsonNode -> true).stream()
        .map(
            document -> {
              try {
                return OBJECT_MAPPER.readTree(document.toJson());
              } catch (JsonProcessingException e) {
                LOGGER.error("Error converting document to Json node.");
              }
              return null;
            })
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
}
