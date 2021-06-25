package org.hypertrace.alert.engine.metric.anomaly.task.manager;

import com.typesafe.config.Config;
import org.quartz.Scheduler;

public interface TaskManager {
  void init(Scheduler scheduler, Config config);

  void runConsumerLoop();

  void stop(Scheduler scheduler);
}
