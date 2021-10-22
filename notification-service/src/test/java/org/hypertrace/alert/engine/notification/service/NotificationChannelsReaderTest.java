package org.hypertrace.alert.engine.notification.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.Config;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.hypertrace.alert.engine.notification.service.NotificationChannel.WebFormatNotificationChannelConfig;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class NotificationChannelsReaderTest {

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "notification-service")
  void testReadNotificationChannels() {
    Config config = ConfigClientFactory.getClient().getConfig();

    List<NotificationChannel> notificationChannels =
        new NotificationChannelsReader(
                config.getConfig("notificationChannelsSource"),
                new PlatformServiceLifecycle() {
                  @Override
                  public CompletionStage<Void> shutdownComplete() {
                    return new CompletableFuture().minimalCompletionStage();
                  }

                  @Override
                  public State getState() {
                    return null;
                  }
                })
            .readNotificationChannels();

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
