package org.hypertrace.alert.engine.anomaly.event.processor;

import java.io.IOException;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle.State;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class AnomalyEventProcessorServiceTest {

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "anomaly-event-processor")
  public void testAlertTask() throws IOException {

    AnomalyEventProcessorService anomalyEventProcessorService =
        new AnomalyEventProcessorService(ConfigClientFactory.getClient());

    Assertions.assertEquals(State.STARTED, anomalyEventProcessorService.getLifecycle().getState());
  }
}
