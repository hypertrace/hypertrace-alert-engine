package org.hypertrace.alert.engine.metric.anomaly.datamodel.queue;

import com.typesafe.config.Config;
import java.io.IOException;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;

public interface Queue<T> {
  void init(Config queueConfig);

  void enqueue(T object) throws NotImplementedException, IOException;

  T dequeue() throws NotImplementedException, IOException;

  void close();
}
