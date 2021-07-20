package org.hypertrace.alert.engine;

import java.util.List;
import java.util.Optional;
import org.hypertrace.alert.engine.anomaly.event.processor.NotificationEventProcessor;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.detector.AlertRuleEvaluator;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RuleEvaluationJob {
  private static final Logger LOGGER = LoggerFactory.getLogger(RuleEvaluationJob.class);

  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    JobDetail jobDetail = jobExecutionContext.getJobDetail();
    LOGGER.debug("Starting Metric Anomaly alert task Job:", jobDetail.getKey());

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
      } catch (Exception e) {
        //
      }
    }
  }
}
