package org.hypertrace.alert.engine.anomaly.event.processor;

import java.util.List;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
public class NotificationChannel {

  public static final String WEBHOOK_CHANNEL_CONFIG_TYPE = "WEBHOOK";
  public static final String WEBHOOK_FORMAT_SLACK = "WEBHOOK_FORMAT_SLACK";
  public static final String WEBHOOK_FORMAT_JSON = "WEBHOOK_FORMAT_JSON";

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
