package org.hypertrace.alert.engine;

import com.typesafe.config.Config;
import io.micrometer.core.instrument.Counter;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.detector.evaluator.AlertRuleEvaluator;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskConverter;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants;
import org.hypertrace.alert.engine.notification.service.NotificationEventProcessor;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuleEvaluationJob implements Job {
  private static final Logger LOGGER = LoggerFactory.getLogger(RuleEvaluationJob.class);
  private static final ConcurrentMap<String, Counter> alertProcessingErrorCounter =
      new ConcurrentHashMap<>();
  private static final String ALERT_PROCESS_ERROR_COUNTER = "hypertrace.alert.engine.alert.task.processing.error";

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
      LOGGER.debug(
          "Current and last time {} {}",
          Instant.ofEpochMilli(alertTaskBuilder.getCurrentExecutionTime()),
          Instant.ofEpochMilli(alertTaskBuilder.getLastExecutionTime()));
      try {
        Optional<NotificationEvent> notificationEventOptional =
            alertRuleEvaluator.process(alertTaskBuilder.build());
        notificationEventOptional.ifPresent(notificationEventProcessor::process);
      } catch (IOException e) {
        alertProcessingErrorCounter
            .computeIfAbsent(
                alertTaskBuilder.getTenantId(),
                k ->
                    PlatformMetricsRegistry.registerCounter(
                        ALERT_PROCESS_ERROR_COUNTER, Map.of("tenantId", k)))
            .increment();
        LOGGER.error("Exception processing alertTask {}", alertTaskBuilder, e);
      }
    }
  }
}
