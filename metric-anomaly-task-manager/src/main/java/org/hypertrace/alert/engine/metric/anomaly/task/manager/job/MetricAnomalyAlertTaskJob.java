package org.hypertrace.alert.engine.metric.anomaly.task.manager.job;

import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.DELAYED_IN_MINUTES;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_JOB_CONFIG;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_PRODUCER_QUEUE;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_RULE_SOURCE;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.METRIC_ANOMALY_EVENT_CONDITION;

import com.typesafe.config.Config;
import java.io.IOException;
import java.util.List;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.queue.KafkaAlertTaskProducer;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.rule.source.RuleSource;
import org.hypertrace.core.documentstore.Document;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricAnomalyAlertTaskJob implements Job {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricAnomalyAlertTaskJob.class);
  private static final String DELAY_IN_MINUTES_CONFIG = "delayInMinutes";

  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    JobDetail jobDetail = jobExecutionContext.getJobDetail();
    LOGGER.debug("Starting Alert task Job:", jobDetail.getKey());

    JobDataMap jobDataMap = jobDetail.getJobDataMap();
    Config jobConfig = (Config) jobDataMap.get(JOB_DATA_MAP_JOB_CONFIG);
    RuleSource ruleSource = (RuleSource) jobDataMap.get(JOB_DATA_MAP_RULE_SOURCE);
    KafkaAlertTaskProducer kafkaAlertTaskProducer =
        (KafkaAlertTaskProducer) jobDataMap.get(JOB_DATA_MAP_PRODUCER_QUEUE);

    int delayInMinutes =
        jobConfig.hasPath(DELAY_IN_MINUTES_CONFIG)
            ? jobConfig.getInt(DELAY_IN_MINUTES_CONFIG)
            : DELAYED_IN_MINUTES;

    try {
      List<Document> documents = ruleSource.getAllEventConditions(METRIC_ANOMALY_EVENT_CONDITION);
      documents.forEach(
          document -> {
            try {
              AlertTask task = AlertTaskConverter.toAlertTask(document, delayInMinutes);
              kafkaAlertTaskProducer.enqueue(task);
            } catch (IOException e) {
              LOGGER.debug(
                  "Failed to enqueue alert task for document:{} with exception:{}",
                  document.toJson(),
                  e);
            }
          });
      LOGGER.debug("job finished");
    } catch (IOException e) {
      LOGGER.error("Job failed with exception:{}", e);
    }
  }
}
