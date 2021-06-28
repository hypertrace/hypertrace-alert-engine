package org.hypertrace.alert.engine.metric.anomaly.task.manager;

import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.Config;
import java.util.Optional;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.common.AvdlAlertTaskConsumer;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.common.AvdlAlertTaskJob;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvdlBasedTaskManager implements TaskManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBasedTaskManager.class);
  private AvdlAlertTaskConsumer avdlAlertTaskConsumer;
  private JobKey jobKey;

  public void init(Scheduler scheduler, Config appConfig) {
    try {
      JobDataMap jobDataMap = new JobDataMap();
      jobDataMap.put("config", appConfig);
      JobDetail jobDetail =
          JobBuilder.newJob(AvdlAlertTaskJob.class)
              .withIdentity("avdl-alert-task", "alerting")
              .usingJobData(jobDataMap)
              .build();
      jobKey = jobDetail.getKey();
      CronTrigger trigger =
          TriggerBuilder.newTrigger()
              .withIdentity("avdl-alert-task-trigger", "alerting")
              .withSchedule(CronScheduleBuilder.cronSchedule("0 * * * * ?"))
              .build();
      scheduler.scheduleJob(jobDetail, trigger);

    } catch (SchedulerException e) {
      throw new RuntimeException(e);
    }
    avdlAlertTaskConsumer = new AvdlAlertTaskConsumer(appConfig.getConfig("queue.config.kafka"));
  }

  public void runConsumerLoop() {
    while (true) {
      Optional<AlertTask> optionalAlertTask = avdlAlertTaskConsumer.consumeTask();
      if (optionalAlertTask.isPresent()) {
        AlertTask alertTask = optionalAlertTask.get();
        LOGGER.info("AlertTask:{}", alertTask);
        if (alertTask.getEventConditionType().equals("MetricAnomalyEventCondition")) {
          try {
            MetricAnomalyEventCondition metricAnomalyEventCondition =
                MetricAnomalyEventCondition.parseFrom(alertTask.getEventConditionValue());
            LOGGER.info("MetricAnomalyEventCondition:{}", metricAnomalyEventCondition.toString());
          } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
          }
        } else {
          LOGGER.info("EventConditionType:{}", alertTask.getEventConditionType());
        }
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void stop(Scheduler scheduler) {
    try {
      if (scheduler.checkExists(jobKey)) {
        scheduler.deleteJob(jobKey);
      }
    } catch (SchedulerException e) {
      e.printStackTrace();
    }
    avdlAlertTaskConsumer.close();
  }
}
