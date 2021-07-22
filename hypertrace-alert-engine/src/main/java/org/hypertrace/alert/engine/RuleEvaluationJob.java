package org.hypertrace.alert.engine;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.detector.AlertRuleEvaluator;
import org.hypertrace.alert.engine.notification.service.NotificationEventProcessor;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RuleEvaluationJob implements Job {
  private static final Logger LOGGER = LoggerFactory.getLogger(RuleEvaluationJob.class);

  public void execute(JobExecutionContext jobExecutionContext) {
    JobDetail jobDetail = jobExecutionContext.getJobDetail();
    LOGGER.debug("Starting Hypertrace Alert Engine: {}", jobDetail.getKey());

    JobDataMap jobDataMap = jobDetail.getJobDataMap();
    List<AlertTask> alertTasks =
        (List<AlertTask>) jobDataMap.get(RuleEvaluationJobManager.ALERT_TASKS);

    AlertRuleEvaluator alertRuleEvaluator =
        (AlertRuleEvaluator) jobDataMap.get(RuleEvaluationJobManager.ALERT_RULE_EVALUATOR);
    NotificationEventProcessor notificationEventProcessor =
        (NotificationEventProcessor)
            jobDataMap.get(RuleEvaluationJobManager.NOTIFICATION_PROCESSOR);

    for (AlertTask alertTask : alertTasks) {
      try {
        Optional<NotificationEvent> notificationEventOptional =
            alertRuleEvaluator.process(alertTask);
        notificationEventOptional.ifPresent(notificationEventProcessor::process);
      } catch (IOException e) {
        LOGGER.error("Exception processing alertTask {}", alertTask, e);
      }
    }
  }
}
