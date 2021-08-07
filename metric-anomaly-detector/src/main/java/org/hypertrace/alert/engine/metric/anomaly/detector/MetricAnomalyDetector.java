package org.hypertrace.alert.engine.metric.anomaly.detector;

import com.typesafe.config.Config;
import java.io.IOException;
import java.util.Optional;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.detector.evaluator.AlertRuleEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MetricAnomalyDetector {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricAnomalyDetector.class);

  private final AlertRuleEvaluator alertRuleEvaluator;
  private final NotificationEventProducer eventProducer;

  MetricAnomalyDetector(Config appConfig, NotificationEventProducer notificationEventProducer) {
    this.alertRuleEvaluator = new AlertRuleEvaluator(appConfig);
    this.eventProducer = notificationEventProducer;
  }

  void process(AlertTask alertTask) throws IOException {
    Optional<NotificationEvent> optionalNotificationEvent = alertRuleEvaluator.process(alertTask);
    optionalNotificationEvent.ifPresent(eventProducer::publish);
  }
}
