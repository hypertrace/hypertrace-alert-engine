package org.hypertrace.alert.engine.metric.anomaly.task.manager.job;

import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_PRODUCER_QUEUE;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_RULE_SOURCE;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.JOB_DATA_MAP_TASK_CONVERTER;
import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.METRIC_ANOMALY_EVENT_CONDITION;

import java.io.IOException;
import java.util.ArrayList;
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

  public void execute(JobExecutionContext jobExecutionContext) {
    JobDetail jobDetail = jobExecutionContext.getJobDetail();
    LOGGER.debug("Starting Metric Anomaly alert task Job:", jobDetail.getKey());

    JobDataMap jobDataMap = jobDetail.getJobDataMap();
    RuleSource ruleSource = (RuleSource) jobDataMap.get(JOB_DATA_MAP_RULE_SOURCE);
    KafkaAlertTaskProducer kafkaAlertTaskProducer =
        (KafkaAlertTaskProducer) jobDataMap.get(JOB_DATA_MAP_PRODUCER_QUEUE);
    AlertTaskConverter alertTaskConverter =
        (AlertTaskConverter) jobDataMap.get(JOB_DATA_MAP_TASK_CONVERTER);

    try {
      List<Document> documents = ruleSource.getAllEventConditions(METRIC_ANOMALY_EVENT_CONDITION);
      LOGGER.debug("Number of task to execute as part of this run: {}", documents.size());
      List<Document> queued = new ArrayList();
      documents.forEach(
          document -> {
            try {
              AlertTask task = alertTaskConverter.toAlertTask(document);
              kafkaAlertTaskProducer.enqueue(task);
              queued.add(document);
            } catch (Exception e) {
              LOGGER.debug(
                  "Failed execute alert task for document:{} with exception:{}",
                  document.toJson(),
                  e);
            }
          });
      LOGGER.debug("Total number of tasks queued as part of this run:{}", queued.size());
      LOGGER.debug("job finished");
    } catch (IOException e) {
      LOGGER.error("Job failed with exception:{}", e);
    }
  }
}
