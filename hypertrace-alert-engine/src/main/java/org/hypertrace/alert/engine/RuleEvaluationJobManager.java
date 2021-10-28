package org.hypertrace.alert.engine;

import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.ALERT_RULE_SOURCE;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.CRON_EXPRESSION;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_CONFIG;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_CONFIG_CRON_EXPRESSION;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_RULE_SOURCE;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_TASK_CONVERTER;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_GROUP;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_NAME;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_TRIGGER_NAME;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.RULE_SOURCE_TYPE;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.RULE_SOURCE_TYPE_DATASTORE;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.RULE_SOURCE_TYPE_FS;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import java.util.StringJoiner;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source.FSRuleSource;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source.RuleSource;
import org.hypertrace.alert.engine.metric.anomaly.detector.evaluator.AlertRuleEvaluator;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskConverter;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.DbRuleSource;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.JobManager;
import org.hypertrace.alert.engine.notification.service.NotificationChannelsReader;
import org.hypertrace.alert.engine.notification.service.NotificationEventProcessor;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;
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

  static final String ALERT_RULE_EVALUATOR = "ALERT_RULE_EVALUATOR";
  static final String NOTIFICATION_PROCESSOR = "NOTIFICATION_PROCESSOR";
  static final String JOB_SUFFIX = "jobSuffix";

  private JobKey jobKey;
  private JobDetail jobDetail;
  private Trigger jobTrigger;
  private final PlatformServiceLifecycle lifecycle;

  public RuleEvaluationJobManager(PlatformServiceLifecycle lifecycle) {
    this.lifecycle = lifecycle;
  }

  public void initJob(Config appConfig) {
    Config jobConfig =
        appConfig.hasPath(JOB_CONFIG)
            ? appConfig.getConfig(JOB_CONFIG)
            : ConfigFactory.parseMap(Map.of());

    LOGGER.info("Application Config {}, job Config {}", appConfig, jobConfig);

    String jobGroup =
        new StringJoiner(".").add(JOB_GROUP).add(jobConfig.getString(JOB_SUFFIX)).toString();

    jobKey = JobKey.jobKey(JOB_NAME, jobGroup);

    JobDataMap jobDataMap = new JobDataMap();

    addAlertTasksToJobData(jobDataMap, appConfig);

    addEvaluatorToJobData(jobDataMap, appConfig);

    addNotificationProcessorToJobData(jobDataMap, appConfig, lifecycle);

    addJobConfigToJobData(jobDataMap, appConfig);

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
            .withIdentity(JOB_TRIGGER_NAME, jobGroup)
            .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
            .startNow()
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

  private void addAlertTasksToJobData(JobDataMap jobDataMap, Config appConfig) {
    RuleSource ruleSource = getAlertRuleSource(appConfig.getConfig(ALERT_RULE_SOURCE));
    Config jobConfig = getJobConfig(appConfig);

    AlertTaskConverter alertTaskConverter = new AlertTaskConverter(jobConfig);

    jobDataMap.put(JOB_DATA_MAP_RULE_SOURCE, ruleSource);
    jobDataMap.put(JOB_DATA_MAP_TASK_CONVERTER, alertTaskConverter);
  }

  private RuleSource getAlertRuleSource(Config ruleSourceConfig) {
    String ruleSourceType = ruleSourceConfig.getString(RULE_SOURCE_TYPE);
    switch (ruleSourceType) {
      case RULE_SOURCE_TYPE_FS:
        return new FSRuleSource(ruleSourceConfig.getConfig(RULE_SOURCE_TYPE_FS));
      case RULE_SOURCE_TYPE_DATASTORE:
        return new DbRuleSource(ruleSourceConfig.getConfig(RULE_SOURCE_TYPE_DATASTORE), lifecycle);
      default:
        throw new RuntimeException(String.format("Invalid rule source type:%s", ruleSourceType));
    }
  }

  private void addEvaluatorToJobData(JobDataMap jobDataMap, Config appConfig) {
    AlertRuleEvaluator alertRuleEvaluator = new AlertRuleEvaluator(appConfig);
    jobDataMap.put(ALERT_RULE_EVALUATOR, alertRuleEvaluator);
  }

  private void addJobConfigToJobData(JobDataMap jobDataMap, Config appConfig) {
    jobDataMap.put(AlertTaskJobConstants.JOB_DATA_MAP_JOB_CONFIG, getJobConfig(appConfig));
  }

  private Config getJobConfig(Config appConfig) {
    return appConfig.hasPath(JOB_CONFIG)
        ? appConfig.getConfig(JOB_CONFIG)
        : ConfigFactory.parseMap(Map.of());
  }

  private void addNotificationProcessorToJobData(
      JobDataMap jobDataMap, Config appConfig, PlatformServiceLifecycle platformServiceLifecycle) {

    jobDataMap.put(
        NOTIFICATION_PROCESSOR,
        new NotificationEventProcessor(
            appConfig.getConfig(NotificationChannelsReader.NOTIIFICATION_CHANNELS_SOURCE),
            platformServiceLifecycle));
  }
}
