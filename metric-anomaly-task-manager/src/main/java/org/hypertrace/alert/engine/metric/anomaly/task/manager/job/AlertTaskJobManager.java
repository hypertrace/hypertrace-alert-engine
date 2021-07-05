package org.hypertrace.alert.engine.metric.anomaly.task.manager.job;

import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.CRON_EXPRESSION;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_JOB_CONFIG;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_PRODUCER_QUEUE;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_RULE_SOURCE;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_GROUP;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_NAME;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_TRIGGER_NAME;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.queue.KafkaAlertTaskProducer;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.rule.source.RuleSource;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.rule.source.RuleSourceProvider;
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
  private static final String RULE_SOURCE_CONFIG = "ruleSource";
  private static final String KAFKA_QUEUE_CONFIG = "queue.config.kafka";
  private static final String JOB_CONFIG = "job.config";
  private static final String JOB_CONFIG_CRON_EXPRESSION = "cronExpression";

  private JobKey jobKey;
  private JobDetail jobDetail;
  private Trigger jobTrigger;
  private RuleSource ruleSource;
  private KafkaAlertTaskProducer kafkaAlertTaskProducer;

  public void initJob(Config appConfig) {
    ruleSource = RuleSourceProvider.getProvider(appConfig.getConfig(RULE_SOURCE_CONFIG));
    kafkaAlertTaskProducer = new KafkaAlertTaskProducer(appConfig.getConfig(KAFKA_QUEUE_CONFIG));
    Config jobConfig =
        appConfig.hasPath(JOB_CONFIG)
            ? appConfig.getConfig(JOB_CONFIG)
            : ConfigFactory.parseMap(Map.of());

    jobKey = JobKey.jobKey(JOB_NAME, JOB_GROUP);

    JobDataMap jobDataMap = new JobDataMap();
    jobDataMap.put(JOB_DATA_MAP_JOB_CONFIG, jobConfig);
    jobDataMap.put(JOB_DATA_MAP_RULE_SOURCE, ruleSource);
    jobDataMap.put(JOB_DATA_MAP_PRODUCER_QUEUE, kafkaAlertTaskProducer);

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
