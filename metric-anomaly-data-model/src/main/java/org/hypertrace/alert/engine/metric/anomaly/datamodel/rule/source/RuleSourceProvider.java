package org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source;

import com.typesafe.config.Config;

public class RuleSourceProvider {
  private static final String RULE_SOURCE_TYPE = "type";
  private static final String RULE_SOURCE_TYPE_FS = "fs";

  public static RuleSource getProvider(Config ruleSourceConfig) {
    RuleSource ruleSource;
    String ruleSourceType = ruleSourceConfig.getString(RULE_SOURCE_TYPE);
    switch (ruleSourceType) {
      case RULE_SOURCE_TYPE_FS:
        Config ruleSourceFsConfig = ruleSourceConfig.getConfig(RULE_SOURCE_TYPE_FS);
        ruleSource = new FSRuleSource(ruleSourceFsConfig);
        break;
      default:
        throw new RuntimeException(String.format("Invalid rule source type:%s", ruleSourceType));
    }
    return ruleSource;
  }
}
