package org.hypertrace.alert.engine.notification.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.Config;
import java.io.IOException;
import java.util.List;
import org.hypertrace.alert.engine.notification.service.NotificationChannel.WebFormatNotificationChannelConfig;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class NotificationChannelsReaderTest {

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "notification-service")
  void testReadNotificationChannels() throws IOException {
    Config config = ConfigClientFactory.getClient().getConfig().getConfig("ruleSource");

    List<NotificationChannel> notificationChannels =
        NotificationChannelsReader.readNotificationChannels(config);

    assertEquals(1, notificationChannels.size());
    assertEquals("channel-1", notificationChannels.get(0).getChannelName());
    assertEquals("channel-id-1", notificationChannels.get(0).getChannelId());
    assertEquals(
        NotificationChannelsReader.CHANNEL_CONFIG_TYPE_WEBHOOK,
        notificationChannels.get(0).getNotificationChannelConfig().get(0).getChannelConfigType());
    assertEquals(
        NotificationChannelsReader.WEBHOOK_FORMAT_SLACK,
        ((WebFormatNotificationChannelConfig)
                notificationChannels.get(0).getNotificationChannelConfig().get(0))
            .getWebhookFormat());
    assertEquals(
        "https://hooks.slack.com/services/abc",
        ((WebFormatNotificationChannelConfig)
                notificationChannels.get(0).getNotificationChannelConfig().get(0))
            .getUrl());
  }
}
