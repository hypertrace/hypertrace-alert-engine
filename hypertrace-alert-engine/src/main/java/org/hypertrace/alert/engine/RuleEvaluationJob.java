package org.hypertrace.alert.engine;

import com.typesafe.config.Config;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.detector.AlertRuleEvaluator;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskConverter;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants;
import org.hypertrace.alert.engine.notification.service.NotificationEventProcessor;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuleEvaluationJob implements Job {
  private static final Logger LOGGER = LoggerFactory.getLogger(RuleEvaluationJob.class);

  public void execute(JobExecutionContext jobExecutionContext) {
    JobDetail jobDetail = jobExecutionContext.getJobDetail();
    LOGGER.debug("Starting Hypertrace Alert Engine: {}", jobDetail.getKey());

    JobDataMap jobDataMap = jobDetail.getJobDataMap();
    List<AlertTask.Builder> alertTasks =
        (List<AlertTask.Builder>) jobDataMap.get(RuleEvaluationJobManager.ALERT_TASKS);

    AlertRuleEvaluator alertRuleEvaluator =
        (AlertRuleEvaluator) jobDataMap.get(RuleEvaluationJobManager.ALERT_RULE_EVALUATOR);
    NotificationEventProcessor notificationEventProcessor =
        (NotificationEventProcessor)
            jobDataMap.get(RuleEvaluationJobManager.NOTIFICATION_PROCESSOR);

    Config jobConfig = (Config) jobDataMap.get(AlertTaskJobConstants.JOB_DATA_MAP_JOB_CONFIG);

    for (AlertTask.Builder alertTaskBuilder : alertTasks) {
      Instant now = AlertTaskConverter.getCurrent(jobConfig);
      AlertTaskConverter.setCurrentExecutionTime(alertTaskBuilder, now);
      AlertTaskConverter.setLastExecutionTime(alertTaskBuilder, now, jobConfig);
      try {
        Optional<NotificationEvent> notificationEventOptional =
            alertRuleEvaluator.process(alertTaskBuilder.build());
        notificationEventOptional.ifPresent(notificationEventProcessor::process);
      } catch (IOException e) {
        LOGGER.error("Exception processing alertTask {}", alertTaskBuilder, e);
      }
    }
  }
}
