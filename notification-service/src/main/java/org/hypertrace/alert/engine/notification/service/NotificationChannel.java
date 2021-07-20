package org.hypertrace.alert.engine.notification.service;

import java.util.List;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
public class NotificationChannel {

  private final String channelName;
  private final String channelId;
  private final List<NotificationChannelConfig> notificationChannelConfig;

  @SuperBuilder
  @Getter
  public abstract static class NotificationChannelConfig {
    private final String channelConfigType;
  }

  @SuperBuilder
  @Getter
  public static class WebFormatNotificationChannelConfig extends NotificationChannelConfig {
    private final String url;
    private final String webhookFormat;
  }
}
