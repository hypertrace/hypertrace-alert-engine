package org.hypertrace.alert.engine.metric.anomaly.task.manager.job;

import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.ALERT_RULE_SOURCE;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.CRON_EXPRESSION;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_CONFIG;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_CONFIG_CRON_EXPRESSION;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_PRODUCER_QUEUE;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_RULE_SOURCE;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_TASK_CONVERTER;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_GROUP;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_NAME;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_TRIGGER_NAME;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.KAFKA_QUEUE_CONFIG;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.queue.KafkaAlertTaskProducer;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source.RuleSource;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source.RuleSourceProvider;
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

public class AlertTaskJobManager implements JobManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlertTaskJobManager.class);

  private JobKey jobKey;
  private JobDetail jobDetail;
  private Trigger jobTrigger;

  public void initJob(Config appConfig) {
    Config jobConfig =
        appConfig.hasPath(JOB_CONFIG)
            ? appConfig.getConfig(JOB_CONFIG)
            : ConfigFactory.parseMap(Map.of());

    RuleSource ruleSource = RuleSourceProvider.getProvider(appConfig.getConfig(ALERT_RULE_SOURCE));
    KafkaAlertTaskProducer kafkaAlertTaskProducer =
        new KafkaAlertTaskProducer(appConfig.getConfig(KAFKA_QUEUE_CONFIG));
    AlertTaskConverter alertTaskConverter = new AlertTaskConverter(jobConfig);

    jobKey = JobKey.jobKey(JOB_NAME, JOB_GROUP);

    JobDataMap jobDataMap = new JobDataMap();
    jobDataMap.put(JOB_DATA_MAP_RULE_SOURCE, ruleSource);
    jobDataMap.put(JOB_DATA_MAP_PRODUCER_QUEUE, kafkaAlertTaskProducer);
    jobDataMap.put(JOB_DATA_MAP_TASK_CONVERTER, alertTaskConverter);

    jobDetail =
        JobBuilder.newJob(MetricAnomalyAlertTaskJob.class)
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
}
