package org.hypertrace.alert.engine.notification.service.notification;

import java.time.Instant;
import java.util.List;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.ViolationSummary;

@SuperBuilder
@Getter
class MetricAnomalyWebhookEvent {
  Instant eventTimestamp;
  Instant violationTimestamp;
  String eventConditionId;
  String eventConditionType;
  List<ViolationSummary> violationSummaryList;
}
