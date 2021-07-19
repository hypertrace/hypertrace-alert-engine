package org.hypertrace.alert.engine.notification.service.notification;

import java.time.Instant;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
class MetricAnomalyWebhookEvent {
  Instant eventTimeStamp;
  String eventConditionId;
  String eventConditionType;
}
