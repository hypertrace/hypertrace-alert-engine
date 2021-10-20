package org.hypertrace.alert.engine.metric.anomaly.task.manager.job;

public class AlertTaskJobConstants {
  public static final String JOB_DATA_MAP_RULE_SOURCE = "ruleSource";
  public static final String JOB_DATA_MAP_PRODUCER_QUEUE = "producerQueue";
  public static final String JOB_DATA_MAP_TASK_CONVERTER = "taskConverter";
  public static final String JOB_DATA_MAP_JOB_CONFIG = "jobConfig";
  public static final String ALERT_RULE_SOURCE = "alertRuleSource";
  public static final String RULE_SOURCE_TYPE = "type";
  public static final String RULE_SOURCE_TYPE_FS = "fs";
  public static final String RULE_SOURCE_TYPE_DATASTORE = "dataStore";

  public static final String JOB_NAME = "alert-task";
  public static final String JOB_GROUP = "alerting";
  public static final String JOB_TRIGGER_NAME = "alert-task-trigger";
  public static final String CRON_EXPRESSION = "0 * * * * ?";

  public static final String KAFKA_QUEUE_CONFIG = "queue.config.kafka";
  public static final String JOB_CONFIG = "job.config";
  public static final String JOB_CONFIG_CRON_EXPRESSION = "cronExpression";

  public static final String METRIC_ANOMALY_EVENT_CONDITION = "metricAnomalyEventCondition";
}
