package org.hypertrace.alert.engine.metric.anomaly.detector;

import java.util.Optional;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.AlertTask;
import org.hypertrace.core.serviceframework.PlatformService;

public class Service  extends PlatformService {

  AlertTaskConsumer alertTaskConsumer;

  @Override
  protected void doInit() {
    alertTaskConsumer = new AlertTaskConsumer(getAppConfig().getConfig("queue.config.kafka"));
  }

  @Override
  protected void doStart() {
    while (true) {
      Optional<AlertTask> optionalAlertTask = alertTaskConsumer.consumeTask();
      if (optionalAlertTask.isPresent()) {
        //LOGGER.info("AlertTask:{}", optionalAlertTask.get().toString());
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
    alertTaskConsumer.close();
  }

  @Override
  public boolean healthCheck() {
    return false;
  }
}