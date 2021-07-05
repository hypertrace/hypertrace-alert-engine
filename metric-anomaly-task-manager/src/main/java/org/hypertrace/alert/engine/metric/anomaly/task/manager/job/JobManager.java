package org.hypertrace.alert.engine.metric.anomaly.task.manager.job;

import com.typesafe.config.Config;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

public interface JobManager {
  void initJob(Config config);

  void startJob(Scheduler scheduler) throws SchedulerException;

  void stopJob(Scheduler scheduler) throws SchedulerException;
}
