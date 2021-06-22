package org.hypertrace.alert.engine.metric.anomaly.detector;

import org.hypertrace.core.serviceframework.PlatformService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricAnomalyDetector extends PlatformService {

  private static final Logger LOG = LoggerFactory.getLogger(MetricAnomalyDetector.class);
  private static final String SERVICE_NAME_CONFIG = "service.name";
  private static final String SERVICE_PORT_CONFIG = "service.port";
  private static final int DEFAULT_PORT = 50071;

  private String serviceName;

  @Override
  protected void doInit() {
    serviceName = getAppConfig().getString(SERVICE_NAME_CONFIG);
    int port =
        getAppConfig().hasPath(SERVICE_PORT_CONFIG)
            ? getAppConfig().getInt(SERVICE_PORT_CONFIG)
            : DEFAULT_PORT;
    // init the kafka consumer threads
  }

  @Override
  protected void doStart() {
    // start the kafka consumer threads
  }

  @Override
  protected void doStop() {
    // stop the threads
  }

  @Override
  public boolean healthCheck() {
    return true;
  }
}
