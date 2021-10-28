package org.hypertrace.alert.engine.notification.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source.FSRuleSource;
import org.hypertrace.alert.engine.notification.service.NotificationChannel.NotificationChannelConfig;
import org.hypertrace.alert.engine.notification.service.NotificationChannel.WebFormatNotificationChannelConfig;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationChannelsReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(NotificationChannelsReader.class);
  public static final String NOTIIFICATION_CHANNELS_SOURCE = "notificationChannelsSource";
  private static final String TENANT_ID = "tenantId";
  private static final String CHANNEL_ID = "channelId";
  private static final String CHANNEL_NAME = "channelName";
  private static final String CHANNEL_CONFIG = "channelConfig";
  private static final String CHANNEL_CONFIG_TYPE = "channelConfigType";
  private static final String WEBFORMAT_CHANNEL_CONFIG_URL = "url";
  private static final String WEBFORMAT_CHANNEL_CONFIG_WEBHOOK_FORMAT = "webhookFormat";
  public static final String CHANNEL_CONFIG_TYPE_WEBHOOK = "WEBHOOK";
  public static final String WEBHOOK_FORMAT_SLACK = "WEBHOOK_FORMAT_SLACK";
  public static final String WEBHOOK_FORMAT_JSON = "WEBHOOK_FORMAT_JSON";
  private static final String RULE_SOURCE_TYPE = "type";
  private static final String RULE_SOURCE_TYPE_FS = "fs";
  private static final String RULE_SOURCE_TYPE_DATASTORE = "dataStore";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private NotificationChannelDbRuleSource dbRuleSource;
  private FSRuleSource fsRuleSource;
  private final boolean ruleSourceTypeFs;

  public NotificationChannelsReader(
      Config ruleSourceConfig, PlatformServiceLifecycle platformServiceLifecycle) {
    String ruleSourceType = ruleSourceConfig.getString(RULE_SOURCE_TYPE);
    switch (ruleSourceType) {
      case RULE_SOURCE_TYPE_FS:
        ruleSourceTypeFs = true;
        fsRuleSource = new FSRuleSource(ruleSourceConfig.getConfig(RULE_SOURCE_TYPE_FS));
        break;
      case RULE_SOURCE_TYPE_DATASTORE:
        ruleSourceTypeFs = false;
        dbRuleSource =
            new NotificationChannelDbRuleSource(
                ruleSourceConfig.getConfig(RULE_SOURCE_TYPE_DATASTORE), platformServiceLifecycle);
        break;
      default:
        throw new RuntimeException(String.format("Invalid rule source type:%s", ruleSourceType));
    }
  }

  public List<NotificationChannel> readAllNotificationChannelsForTenant(String tenantId) {
    if (ruleSourceTypeFs) {
      return readNotificationChannelsFromFs(tenantId);
    }
    return readNotificationChannelsFromDb(tenantId);
  }

  private List<NotificationChannel> readNotificationChannelsFromDb(String tenantId) {
    List<org.hypertrace.notification.config.service.v1.NotificationChannel> notificationChannels =
        dbRuleSource.getNotificationChannelsForTenant(tenantId);
    return notificationChannels.stream()
        .filter(
            notificationChannel ->
                notificationChannel
                        .getNotificationChannelMutableData()
                        .getWebhookChannelConfigCount()
                    > 0)
        .map(
            notificationChannel -> {
              List<NotificationChannelConfig> webhookChannelConfigs =
                  notificationChannel
                      .getNotificationChannelMutableData()
                      .getWebhookChannelConfigList()
                      .stream()
                      .map(
                          webhookChannelConfig ->
                              WebFormatNotificationChannelConfig.builder()
                                  .url(webhookChannelConfig.getUrl())
                                  .webhookFormat(webhookChannelConfig.getFormat().name())
                                  .channelConfigType(CHANNEL_CONFIG_TYPE_WEBHOOK)
                                  .build())
                      .collect(Collectors.toUnmodifiableList());

              return NotificationChannel.builder()
                  .tenantId(tenantId)
                  .channelId(notificationChannel.getId())
                  .channelName(
                      notificationChannel.getNotificationChannelMutableData().getChannelName())
                  .notificationChannelConfig(webhookChannelConfigs)
                  .build();
            })
        .collect(Collectors.toUnmodifiableList());
  }

  private List<NotificationChannel> readNotificationChannelsFromFs(String tenantId) {
    try {
      return fsRuleSource.getAllRules(jsonNode -> true).stream()
          .map(
              document -> {
                try {
                  return OBJECT_MAPPER.readTree(document.toJson());
                } catch (JsonProcessingException e) {
                  LOGGER.error("Error converting document to Json node.");
                }
                return null;
              })
          .filter(node -> node != null && tenantId.equals(node.get(TENANT_ID).asText()))
          .map(
              node ->
                  NotificationChannel.builder()
                      .channelId(node.get(CHANNEL_ID).asText())
                      .channelName(node.get(CHANNEL_NAME).asText())
                      .notificationChannelConfig(getChannelConfigs(node))
                      .build())
          .collect(Collectors.toList());
    } catch (IOException e) {
      return Collections.emptyList();
    }
  }

  private List<NotificationChannelConfig> getChannelConfigs(JsonNode node) {
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
