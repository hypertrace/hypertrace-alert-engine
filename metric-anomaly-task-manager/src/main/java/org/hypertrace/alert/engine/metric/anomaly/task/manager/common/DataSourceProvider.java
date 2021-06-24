package org.hypertrace.alert.engine.metric.anomaly.task.manager.common;

import com.typesafe.config.Config;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataSourceProvider {
  private static Map<String, Class<? extends DataSource>> registry = new ConcurrentHashMap();

  public static DataSource getDataSource(Config dataSourceConfig) {
    String dataSourceType = dataSourceConfig.getString("type").toLowerCase();
    Class clazz = (Class)registry.get(dataSourceType);

    try {
      Constructor<? extends DataSource> constructor = clazz.getConstructor();
      DataSource instance = (DataSource)constructor.newInstance();
      instance.init(dataSourceConfig.getConfig(dataSourceType));
      return instance;
    } catch (Exception var5) {
      throw new IllegalArgumentException("Exception creating DataSource", var5);
    }
  }

  public static void register(String type, Class<? extends DataSource> clazz) {
    registry.put(type.toLowerCase(), clazz);
  }

  static {
    register("file", FileDataSource.class);
  }
}
