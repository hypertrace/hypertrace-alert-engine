package org.hypertrace.alert.engine.notification.transport;

import com.typesafe.config.Config;

public class NotificationSenderConfig {
  private static final String NOTIFICATION_CONFIG = "notification.config";
  private static final String TRACEABLE_URL = "traceable.url";

  private final Config rawConfig;
  private final String traceableUrl;

  public static NotificationSenderConfig from(Config config) {
    return new NotificationSenderConfig(config.getConfig(NOTIFICATION_CONFIG));
  }

  private NotificationSenderConfig(Config notificationConfig) {
    this.rawConfig = notificationConfig;
    this.traceableUrl = notificationConfig.getString(TRACEABLE_URL);
  }

  public String getTraceableUrl() {
    return traceableUrl;
  }

  public Config getRawConfig() {
    return rawConfig;
  }
}
