package org.hypertrace.alert.engine.metric.anomaly.task.manager.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import java.util.List;
import java.util.Optional;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvdlAlertTaskJob implements Job {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvdlAlertTaskJob.class);
  private static final String DATA_SOURCE_CONFIG = "dataSource";

  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    LOGGER.info("job started with key:", jobExecutionContext.getJobDetail().getKey());
    JobDataMap jobDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
    Config appConfig = (Config) jobDataMap.get("config");

    // read rules
    Config dataSourceConfig = appConfig.getConfig(DATA_SOURCE_CONFIG);
    DataSource dataSource = DataSourceProvider.getDataSource(dataSourceConfig);
    List<JsonNode> objectList = dataSource.getAllNotificationRules();

    // prepare tasks
    List<Optional<AlertTask>> alertTasks = AvdlAlertTaskProvider.prepareTasks(objectList);

    // print as logs.
    AvdlAlertTaskProducer avdlAlertTaskProducer =
        new AvdlAlertTaskProducer(appConfig.getConfig("queue.config.kafka"));

    alertTasks.forEach(
        alertTask -> {
          if (alertTask.isPresent()) {
            avdlAlertTaskProducer.produceTask(alertTask.get());
          }
        });

    avdlAlertTaskProducer.close();
    LOGGER.info("job finished");
  }
}
