package org.hypertrace.alert.engine.metric.anomaly.task.manager.job;

import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_PRODUCER_QUEUE;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_RULE_SOURCE;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_TASK_CONVERTER;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.METRIC_ANOMALY_EVENT_CONDITION;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.queue.KafkaAlertTaskProducer;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source.RuleSource;
import org.hypertrace.core.documentstore.Document;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricAnomalyAlertTaskJob implements Job {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricAnomalyAlertTaskJob.class);
  private static final String EVENT_CONDITION_TYPE_KEY = "eventConditionType";
  private static final Predicate<JsonNode> PREDICATE =
      node ->
          (node.get(EVENT_CONDITION_TYPE_KEY).textValue().equals(METRIC_ANOMALY_EVENT_CONDITION));

  public void execute(JobExecutionContext jobExecutionContext) {
    JobDetail jobDetail = jobExecutionContext.getJobDetail();
    LOGGER.debug("Starting Metric Anomaly alert task Job: {}", jobDetail.getKey());

    JobDataMap jobDataMap = jobDetail.getJobDataMap();
    RuleSource ruleSource = (RuleSource) jobDataMap.get(JOB_DATA_MAP_RULE_SOURCE);
    KafkaAlertTaskProducer kafkaAlertTaskProducer =
        (KafkaAlertTaskProducer) jobDataMap.get(JOB_DATA_MAP_PRODUCER_QUEUE);
    AlertTaskConverter alertTaskConverter =
        (AlertTaskConverter) jobDataMap.get(JOB_DATA_MAP_TASK_CONVERTER);

    List<AlertTask.Builder> alertTasks = getAlertTasks(alertTaskConverter, ruleSource);
    LOGGER.debug("Number of task to execute as part of this run: {}", alertTasks.size());
    alertTasks.forEach(
        alertTask -> {
          try {
            kafkaAlertTaskProducer.enqueue(alertTask.build());
          } catch (IOException e) {
            LOGGER.debug("Failed execute alert task for task: {} with exception:{}", alertTask, e);
          }
        });
    LOGGER.debug("job finished");
  }

  public static List<AlertTask.Builder> getAlertTasks(
      AlertTaskConverter alertTaskConverter, RuleSource ruleSource) {
    List<AlertTask.Builder> alertTasks = new ArrayList<>();
    try {
      List<Document> documents = ruleSource.getAllRules(PREDICATE);
      documents.forEach(
          document -> {
            try {
              AlertTask.Builder alertTaskBuilder = alertTaskConverter.toAlertTaskBuilder(document);
              alertTasks.add(alertTaskBuilder);
            } catch (Exception e) {
              LOGGER.error(
                  "Failed to convert alert task for document:{} with exception:{}",
                  document.toJson(),
                  e);
            }
          });
    } catch (IOException e) {
      LOGGER.error("Job failed with exception", e);
    }
    return alertTasks;
  }
}
