package org.hypertrace.alert.engine;

import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.JobManager;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

public class HypertraceAlertEngineService extends PlatformService {

  private Scheduler scheduler;
  private JobManager jobManager;

  public HypertraceAlertEngineService(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    try {
      SchedulerFactory schedulerFactory = new StdSchedulerFactory();
      scheduler = schedulerFactory.getScheduler();
      jobManager = new RuleEvaluationJobManager();
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