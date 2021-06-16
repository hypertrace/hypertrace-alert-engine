package org.hypertrace.alerting.rule.manager;

import org.hypertrace.core.serviceframework.PlatformService;

public class AlertingRuleManager extends PlatformService {

  @Override
  protected void doInit() {}

  @Override
  protected void doStart() {}

  @Override
  protected void doStop() {}

  @Override
  public boolean healthCheck() {
    return false;
  }
}
