package org.hypertrace.alert.engine.metric.anomaly.task.manager;

import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricAnomalyTaskManager extends PlatformService {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricAnomalyTaskManager.class);

  //  private static final String SERVICE_NAME_CONFIG = "service.name";
  //  private static final String SERVICE_PORT_CONFIG = "service.port";

  private Scheduler scheduler;
  TaskManager taskManager;

  public MetricAnomalyTaskManager(ConfigClient configClient) {
    super(configClient);
  }

  private TaskManager getTaskManager(String type) {
    if (type.equals("avdl")) {
      return new AvdlBasedTaskManager();
    } else {
      return new ProtoBasedTaskManager();
    }
  }

  @Override
  protected void doInit() {
    try {
      SchedulerFactory schedulerFactory = new StdSchedulerFactory();
      scheduler = schedulerFactory.getScheduler();
      taskManager = getTaskManager(getAppConfig().getString("jobType"));
      taskManager.init(scheduler, getAppConfig());
    } catch (SchedulerException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doStart() {
    try {
      scheduler.start();
    } catch (SchedulerException e) {
      throw new RuntimeException(e);
    }
    taskManager.runConsumerLoop();
  }

  @Override
  protected void doStop() {
    try {
      taskManager.stop(scheduler);
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
