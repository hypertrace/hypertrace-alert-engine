package org.hypertrace.alert.engine.metric.anomaly.task.manager;

import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobManager;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.JobManager;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

public class MetricAnomalyTaskManager extends PlatformService {
  private Scheduler scheduler;
  private JobManager jobManager;

  public MetricAnomalyTaskManager(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    try {
      SchedulerFactory schedulerFactory = new StdSchedulerFactory();
      scheduler = schedulerFactory.getScheduler();
      jobManager = new AlertTaskJobManager(getLifecycle());
      jobManager.initJob(getAppConfig());
    } catch (SchedulerException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doStart() {
    try {
      jobManager.startJob(scheduler);
      scheduler.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doStop() {
    try {
      jobManager.stopJob(scheduler);
      scheduler.shutdown();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean healthCheck() {
    return true;
  }
}
