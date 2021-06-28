package org.hypertrace.alert.engine.metric.anomaly.task.manager;

import com.typesafe.config.Config;
import java.util.Optional;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.common.AlertTaskConsumer;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.common.AlertTaskJob;
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

public class ProtoBasedTaskManager implements TaskManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBasedTaskManager.class);
  private AlertTaskConsumer alertTaskConsumer;
  private JobKey jobKey;

  public void init(Scheduler scheduler, Config appConfig) {
    try {
      JobDataMap jobDataMap = new JobDataMap();
      jobDataMap.put("config", appConfig);
      JobDetail jobDetail =
          JobBuilder.newJob(AlertTaskJob.class)
              .withIdentity("proto-alert-task", "alerting")
              .usingJobData(jobDataMap)
              .build();
      jobKey = jobDetail.getKey();
      CronTrigger trigger =
          TriggerBuilder.newTrigger()
              .withIdentity("prot-alert-task-trigger", "alerting")
              .withSchedule(CronScheduleBuilder.cronSchedule("0 * * * * ?"))
              .build();
      scheduler.scheduleJob(jobDetail, trigger);

    } catch (SchedulerException e) {
      throw new RuntimeException(e);
    }
    alertTaskConsumer = new AlertTaskConsumer(appConfig.getConfig("queue.config.kafka"));
  }

  public void runConsumerLoop() {
    while (true) {
      Optional<AlertTask> optionalAlertTask = alertTaskConsumer.consumeTask();
      if (optionalAlertTask.isPresent()) {
        LOGGER.info("AlertTask:{}", optionalAlertTask.get().toString());
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
    alertTaskConsumer.close();
  }
}
