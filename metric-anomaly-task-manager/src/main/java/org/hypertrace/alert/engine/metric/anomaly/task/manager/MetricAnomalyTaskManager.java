package org.hypertrace.alert.engine.metric.anomaly.task.manager;

import java.util.Optional;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.common.AlertTaskConsumer;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.common.AlertTaskJob;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricAnomalyTaskManager extends PlatformService {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricAnomalyTaskManager.class);

  //  private static final String SERVICE_NAME_CONFIG = "service.name";
  //  private static final String SERVICE_PORT_CONFIG = "service.port";

  private Scheduler scheduler;
  AlertTaskConsumer alertTaskConsumer;

  public MetricAnomalyTaskManager(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    try {
      SchedulerFactory schedulerFactory = new StdSchedulerFactory();
      scheduler = schedulerFactory.getScheduler();
      JobDataMap jobDataMap = new JobDataMap();
      jobDataMap.put("config", getAppConfig());
      JobDetail jobDetail =
          JobBuilder.newJob(AlertTaskJob.class)
              .withIdentity("alert-task", "alerting")
              .usingJobData(jobDataMap)
              .build();

      CronTrigger trigger =
          TriggerBuilder.newTrigger()
              .withIdentity("trigger3", "group1")
              .withSchedule(CronScheduleBuilder.cronSchedule("0 * * * * ?"))
              .build();
      scheduler.scheduleJob(jobDetail, trigger);

    } catch (SchedulerException e) {
      throw new RuntimeException(e);
    }
    alertTaskConsumer = new AlertTaskConsumer(getAppConfig().getConfig("queue.config.kafka"));
  }

  @Override
  protected void doStart() {
    try {
      scheduler.start();
    } catch (SchedulerException e) {
      throw new RuntimeException(e);
    }

    // consume task
    // AlertTaskProducer Job
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

  @Override
  protected void doStop() {
    try {
      alertTaskConsumer.close();
      scheduler.shutdown();
    } catch (SchedulerException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean healthCheck() {
    return true;
  }
}
