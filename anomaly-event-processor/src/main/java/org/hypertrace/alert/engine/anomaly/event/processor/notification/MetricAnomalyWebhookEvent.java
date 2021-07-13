package org.hypertrace.alert.engine.anomaly.event.processor.notification;

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
