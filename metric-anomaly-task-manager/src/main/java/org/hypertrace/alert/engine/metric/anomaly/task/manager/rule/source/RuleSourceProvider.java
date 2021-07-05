package org.hypertrace.alert.engine.metric.anomaly.task.manager.rule.source;

import com.typesafe.config.Config;

public class RuleSourceProvider {
  private static final String RULE_SOURCE_TYPE = "type";
  private static final String RULE_SOURCE_TYPE_FS = "fs";

  public static RuleSource getProvider(Config ruleSourceConfig) {
    RuleSource ruleSource;
    String dataSourceType = ruleSourceConfig.getString(RULE_SOURCE_TYPE);
    switch (dataSourceType) {
      case RULE_SOURCE_TYPE_FS:
        Config fileSystemConfig = ruleSourceConfig.getConfig(RULE_SOURCE_TYPE_FS);
        ruleSource = new FSRuleSource(fileSystemConfig);
        break;
      default:
        throw new RuntimeException(
            String.format("Invalid datasource configuration: %s", dataSourceType));
    }
    return ruleSource;
  }
}
