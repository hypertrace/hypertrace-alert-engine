package org.hypertrace.alert.engine.anomaly.event.processor.notification;

import java.time.Instant;
import org.hypertrace.alert.engine.anomaly.event.processor.NotificationChannel;
import org.hypertrace.alert.engine.anomaly.event.processor.NotificationChannel.NotificationChannelConfig;
import org.hypertrace.alert.engine.anomaly.event.processor.NotificationChannel.WebFormatNotificationChannelConfig;
import org.hypertrace.alert.engine.anomaly.event.processor.NotificationChannelsReader;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyViolation;
import org.hypertrace.alert.engine.notification.transport.webhook.WebhookSender;

public class WebhookNotifier {

  private final WebhookSender webhookSender;

  public WebhookNotifier(WebhookSender webhookSender) {
    this.webhookSender = webhookSender;
  }

  public void notify(
      MetricAnomalyViolation metricAnomalyViolation, NotificationChannel notificationChannel) {

    if (!notificationChannel.getNotificationChannelConfig().isEmpty()) {

      MetricAnomalyWebhookEvent message = convert(metricAnomalyViolation);
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

  private MetricAnomalyWebhookEvent convert(MetricAnomalyViolation metricAnomalyViolation) {
    return MetricAnomalyWebhookEvent.builder()
        .eventConditionId(metricAnomalyViolation.getEventConditionId())
        .eventConditionType(metricAnomalyViolation.getEventConditionType())
        .eventTimeStamp(Instant.now())
        .build();
  }

  private MetricAnomalySlackEvent convert(MetricAnomalyWebhookEvent metricAnomalyWebhookEvent) {
    return MetricAnomalySlackEvent.getMessage(metricAnomalyWebhookEvent);
  }
}
