package org.hypertrace.alert.engine.anomaly.event.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.Config;
import java.io.IOException;
import java.util.List;
import org.hypertrace.alert.notification.service.NotificationChannel;
import org.hypertrace.alert.notification.service.NotificationChannel.WebFormatNotificationChannelConfig;
import org.hypertrace.alert.notification.service.NotificationChannelsReader;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class NotificationChannelsReaderTest {

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "notification-service")
  void testReadNotificationChannels() throws IOException {
    Config config = ConfigClientFactory.getClient().getConfig();

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
        "https://hooks.slack.com/services/abcde",
        ((WebFormatNotificationChannelConfig)
                notificationChannels.get(0).getNotificationChannelConfig().get(0))
            .getUrl());
  }
}
