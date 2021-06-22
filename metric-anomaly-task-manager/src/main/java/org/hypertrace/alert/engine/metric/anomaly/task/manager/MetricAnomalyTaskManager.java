package org.hypertrace.alert.engine.metric.anomaly.task.manager;

import com.typesafe.config.Config;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.common.DataSource;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.common.DataSourceProvider;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;

public class MetricAnomalyTaskManager extends PlatformService {
  private static final String SERVICE_NAME_CONFIG = "service.name";
  private static final String SERVICE_PORT_CONFIG = "service.port";
  private static final String DATA_SOURCE_CONFIG = "dataSource";

  private DataSource dataSource;

  public MetricAnomalyTaskManager(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    Config dataSourceConfig = getAppConfig().getConfig(DATA_SOURCE_CONFIG);
    this.dataSource =
        DataSourceProvider.getDataSource(dataSourceConfig);

  }

  @Override
  protected void doStart() {}

  @Override
  protected void doStop() {}

  @Override
  public boolean healthCheck() {
    return false;
  }
}
