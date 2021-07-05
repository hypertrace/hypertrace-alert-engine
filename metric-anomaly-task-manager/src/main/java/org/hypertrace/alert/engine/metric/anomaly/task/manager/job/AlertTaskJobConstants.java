package org.hypertrace.alert.engine.metric.anomaly.task.manager.job;

public class AlertTaskJobConstants {
  static final String JOB_DATA_MAP_RULE_SOURCE = "ruleSource";
  static final String JOB_DATA_MAP_PRODUCER_QUEUE = "producerQueue";
  static final String JOB_DATA_MAP_TASK_CONVERTER = "taskConverter";
  static final String JOB_DATA_MAP_JOB_CONFIG = "jobConfig";

  static final String JOB_NAME = "alert-task";
  static final String JOB_GROUP = "alerting";
  static final String JOB_TRIGGER_NAME = "alert-task-trigger";
  static final String CRON_EXPRESSION = "0 * * * * ?";

  static final String METRIC_ANOMALY_EVENT_CONDITION = "MetricAnomalyEventCondition";
}
