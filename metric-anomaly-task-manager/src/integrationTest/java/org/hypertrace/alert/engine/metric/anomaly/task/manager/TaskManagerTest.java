package org.hypertrace.alert.engine.metric.anomaly.task.manager;

import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.queue.KafkaAlertTaskConsumer;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobManager;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.JobManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class TaskManagerTest {
  private static final String EVENT_CONDITION_TYPE = "MetricAnomalyEventCondition";
  private static Network network;
  private static KafkaContainer kafkaZk;

  @BeforeAll
  public static void setup() throws Exception {
    network = Network.newNetwork();

    kafkaZk =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))
            .withNetwork(network)
            .withNetworkAliases("kafka", "zookeeper")
            .withExposedPorts(9093)
            .withStartupAttempts(2)
            .waitingFor(Wait.forListeningPort());
    kafkaZk.start();
  }

  @AfterAll
  public static void shutDown() {
    kafkaZk.stop();
    network.close();
  }

  @Test
  public void testAlertTask() throws InterruptedException, SchedulerException, URISyntaxException {
    Integer port = kafkaZk.getMappedPort(9093);
    URL resourceUrl =
        Thread.currentThread().getContextClassLoader().getResource("application.conf");
    URL pathUrl = Thread.currentThread().getContextClassLoader().getResource("rules.json");
    File file = Paths.get(pathUrl.toURI()).toFile();
    String absolutePath = file.getAbsolutePath();

    Map testConfigMap =
        Map.of(
            "queue.config.kafka.bootstrap.servers",
            "localhost:" + port,
            "ruleSource.fs.path",
            absolutePath);
    Config appConfig =
        ConfigFactory.parseMap(testConfigMap)
            .withFallback(ConfigFactory.parseURL(resourceUrl))
            .resolve();

    // start the job
    SchedulerFactory schedulerFactory = new StdSchedulerFactory();
    Scheduler scheduler = schedulerFactory.getScheduler();
    JobManager jobManager = new AlertTaskJobManager();
    jobManager.initJob(appConfig);

    jobManager.startJob(scheduler);
    scheduler.start();

    // start the consumer that will wait max 30 seconds
    Config kafkaQueueConfig = appConfig.getConfig("queue.config.kafka");
    KafkaAlertTaskConsumer kafkaAlertTaskConsumer = new KafkaAlertTaskConsumer(kafkaQueueConfig);
    boolean result = startConsumerLoop(kafkaAlertTaskConsumer);

    jobManager.stopJob(scheduler);
    scheduler.shutdown();

    // at least we found 1 message, and were able to parse it successfully
    Assertions.assertTrue(result);
  }

  private boolean startConsumerLoop(KafkaAlertTaskConsumer kafkaAlertTaskConsumer) {
    int sleepTimeMs = 1000; // 1 seconds
    int maxWaitTimeMs = 30 * sleepTimeMs; // wait for max 30 seconds
    int elapsedWaitTime = 0;
    while (true) {
      try {
        AlertTask alertTask = kafkaAlertTaskConsumer.dequeue();
        if (alertTask != null && alertTask.getEventConditionType().equals(EVENT_CONDITION_TYPE)) {
          MetricAnomalyEventCondition.parseFrom(alertTask.getEventConditionValue());
          return true;
        } else {
          Thread.sleep(sleepTimeMs);
          elapsedWaitTime += sleepTimeMs;
        }

        if (elapsedWaitTime > maxWaitTimeMs) {
          return false;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
