package org.hypertrace.alert.engine.notification.service.notification;

import java.time.Instant;
import org.hypertrace.alert.engine.notification.service.NotificationChannel;
import org.hypertrace.alert.engine.notification.service.NotificationChannel.NotificationChannelConfig;
import org.hypertrace.alert.engine.notification.service.NotificationChannel.WebFormatNotificationChannelConfig;
import org.hypertrace.alert.engine.notification.service.NotificationChannelsReader;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyNotificationEvent;
import org.hypertrace.alert.engine.notification.transport.webhook.WebhookSender;

public class WebhookNotifier {

  private final WebhookSender webhookSender;

  public WebhookNotifier(WebhookSender webhookSender) {
    this.webhookSender = webhookSender;
  }

  public void notify(
      MetricAnomalyNotificationEvent metricAnomalyNotificationEvent,
      NotificationChannel notificationChannel) {

    if (!notificationChannel.getNotificationChannelConfig().isEmpty()) {

      MetricAnomalyWebhookEvent message = convert(metricAnomalyNotificationEvent);
      SlackMessage slackMessage = convert(message);
      for (NotificationChannelConfig channelConfig :
          notificationChannel.getNotificationChannelConfig()) {
        if (!channelConfig
            .getChannelConfigType()
            .equals(NotificationChannelsReader.CHANNEL_CONFIG_TYPE_WEBHOOK)) {
          continue;
        }
        WebFormatNotificationChannelConfig webFormatNotificationChannelConfig =
            (WebFormatNotificationChannelConfig) channelConfig;
        if (webFormatNotificationChannelConfig
            .getWebhookFormat()
            .equals(NotificationChannelsReader.WEBHOOK_FORMAT_SLACK)) {
          webhookSender.send(webFormatNotificationChannelConfig.getUrl(), slackMessage);
        } else {
          webhookSender.send(webFormatNotificationChannelConfig.getUrl(), message);
        }
      }
    }
  }

  private MetricAnomalyWebhookEvent convert(
      MetricAnomalyNotificationEvent metricAnomalyNotificationEvent) {
    return MetricAnomalyWebhookEvent.builder()
        .eventConditionId(metricAnomalyNotificationEvent.getEventConditionId())
        .eventConditionType(metricAnomalyNotificationEvent.getEventConditionType())
        .eventTimeStamp(Instant.now())
        .build();
  }

  private MetricAnomalySlackEvent convert(MetricAnomalyWebhookEvent metricAnomalyWebhookEvent) {
    return MetricAnomalySlackEvent.getMessage(metricAnomalyWebhookEvent);
  }
}
