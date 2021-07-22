package org.hypertrace.alert.engine;

import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.CRON_EXPRESSION;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_CONFIG;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_CONFIG_CRON_EXPRESSION;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_GROUP;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_NAME;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_TRIGGER_NAME;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.METRIC_ANOMALY_EVENT_CONDITION;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.detector.AlertRuleEvaluator;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskConverter;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.JobManager;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.rule.source.RuleSource;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.rule.source.RuleSourceProvider;
import org.hypertrace.alert.engine.notification.service.NotificationChannel;
import org.hypertrace.alert.engine.notification.service.NotificationChannelsReader;
import org.hypertrace.alert.engine.notification.service.NotificationEventProcessor;
import org.hypertrace.core.documentstore.Document;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuleEvaluationJobManager implements JobManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(RuleEvaluationJobManager.class);

  static final String ALERT_TASKS = "ALERT_TASKS";
  static final String ALERT_RULE_EVALUATOR = "ALERT_RULE_EVALUATOR";
  static final String NOTIFICATION_PROCESSOR = "NOTIFICATION_PROCESSOR";

  private JobKey jobKey;
  private JobDetail jobDetail;
  private Trigger jobTrigger;

  public void initJob(Config appConfig) {

    Config jobConfig =
        appConfig.hasPath(JOB_CONFIG)
            ? appConfig.getConfig(JOB_CONFIG)
            : ConfigFactory.parseMap(Map.of());

    jobKey = JobKey.jobKey(JOB_NAME, JOB_GROUP);

    JobDataMap jobDataMap = new JobDataMap();
    jobDataMap.put(ALERT_TASKS, getAlertTasks(appConfig));

    AlertRuleEvaluator alertRuleEvaluator = new AlertRuleEvaluator(appConfig);
    jobDataMap.put(ALERT_RULE_EVALUATOR, alertRuleEvaluator);

    try {
      List<NotificationChannel> notificationChannels =
          NotificationChannelsReader.readNotificationChannels(appConfig);
      jobDataMap.put(NOTIFICATION_PROCESSOR, new NotificationEventProcessor(notificationChannels));
    } catch (IOException e) {
      throw new RuntimeException();
    }

    jobDetail =
        JobBuilder.newJob(RuleEvaluationJob.class)
            .withIdentity(jobKey)
            .usingJobData(jobDataMap)
            .build();

    String cronExpression =
        jobConfig.hasPath(JOB_CONFIG_CRON_EXPRESSION)
            ? jobConfig.getString(JOB_CONFIG_CRON_EXPRESSION)
            : CRON_EXPRESSION;
    jobTrigger =
        TriggerBuilder.newTrigger()
            .withIdentity(JOB_TRIGGER_NAME, JOB_GROUP)
            .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
            .build();
  }

  public void startJob(Scheduler scheduler) throws SchedulerException {
    LOGGER.info("Schedule a job:{} with Trigger:{}", jobKey, jobTrigger);
    scheduler.scheduleJob(jobDetail, jobTrigger);
  }

  public void stopJob(Scheduler scheduler) throws SchedulerException {
    if (scheduler.checkExists(jobKey)) {
      scheduler.deleteJob(jobKey);
    }
  }

  private List<AlertTask> getAlertTasks(Config config) {
    RuleSource ruleSource =
        RuleSourceProvider.getProvider(
            config.getConfig(AlertTaskJobConstants.JOB_DATA_MAP_RULE_SOURCE));
    Config jobConfig =
        config.hasPath(AlertTaskJobConstants.JOB_DATA_MAP_JOB_CONFIG)
            ? config.getConfig(AlertTaskJobConstants.JOB_DATA_MAP_JOB_CONFIG)
            : ConfigFactory.parseMap(Map.of());

    AlertTaskConverter alertTaskConverter = new AlertTaskConverter(jobConfig);
    List<AlertTask> alertTasks = new ArrayList<>();
    try {
      List<Document> documents = ruleSource.getAllEventConditions(METRIC_ANOMALY_EVENT_CONDITION);
      documents.forEach(
          document -> {
            try {
              AlertTask task = alertTaskConverter.toAlertTask(document);
              alertTasks.add(task);
            } catch (Exception e) {
              LOGGER.error(
                  "Failed to convert alert task for document:{} with exception:{}",
                  document.toJson(),
                  e);
            }
          });
    } catch (IOException e) {
      LOGGER.error("AlertTask conversion failed with an exception", e);
    }
    return alertTasks;
  }
}
