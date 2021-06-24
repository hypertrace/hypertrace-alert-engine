package org.hypertrace.alert.engine.metric.anomaly.task.manager.common;

import com.typesafe.config.Config;

public interface DataSource {
  void init(Config config);
}


