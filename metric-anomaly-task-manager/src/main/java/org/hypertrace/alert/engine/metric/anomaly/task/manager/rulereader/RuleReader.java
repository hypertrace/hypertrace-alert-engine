package org.hypertrace.alert.engine.metric.anomaly.task.manager.rulereader;

import org.hypertrace.alert.engine.metric.anomlay.rule.config.service.v1.MetricAnomalyAlertRule;

public interface RuleReader {
  MetricAnomalyAlertRule readRule();
}
