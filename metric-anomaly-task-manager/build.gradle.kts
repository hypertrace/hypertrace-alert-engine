plugins {
  java
  application
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
  id("org.hypertrace.integration-test-plugin")
  id("org.hypertrace.docker-java-application-plugin")
  id("org.hypertrace.docker-publish-plugin")
}

application {
  mainClass.set("org.hypertrace.core.serviceframework.PlatformServiceLauncher")
}

dependencies {
  // framework + libs (internal)
  implementation(project(":metric-anomaly-data-model"))
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.32")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.32")
  implementation("org.apache.kafka:kafka-clients:2.6.0")
  implementation("org.hypertrace.core.documentstore:document-store:0.5.7")
  implementation("org.hypertrace.config.service:config-service-api:0.1.12")
  implementation("org.hypertrace.config.service:alerting-config-service-api:0.1.12")
  implementation("org.hypertrace.config.service:notification-rule-config-service-api:0.1.12")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.6.2")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.6.2")
  implementation("org.apache.commons:commons-lang3:3.10")
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-serdes:0.1.23")
  implementation("io.confluent:kafka-streams-avro-serde:6.0.1")
  constraints {
    implementation("org.glassfish.jersey.core:jersey-common:2.34") {
      because("https://snyk.io/vuln/SNYK-JAVA-ORGGLASSFISHJERSEYCORE-1255637")
    }
  }

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.16.0")

  runtimeOnly("io.grpc:grpc-netty:1.42.0")
  constraints {
    runtimeOnly("io.netty:netty-codec-http2:4.1.71.Final")
    runtimeOnly("io.netty:netty-handler-proxy:4.1.71.Final")
  }

  // framework + libs
  implementation("com.google.protobuf:protobuf-java-util:3.17.3")
  implementation("org.quartz-scheduler:quartz:2.3.2")

  // testing
  testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")

  // integration test
  integrationTestImplementation("org.junit.jupiter:junit-jupiter-api:5.7.1")
  integrationTestImplementation("org.junit.jupiter:junit-jupiter-params:5.7.1")
  integrationTestImplementation("org.junit.jupiter:junit-jupiter-engine:5.7.1")
  integrationTestImplementation("org.testcontainers:testcontainers:1.15.2")
  integrationTestImplementation("org.testcontainers:junit-jupiter:1.15.2")
  integrationTestImplementation("org.testcontainers:kafka:1.15.2")
  integrationTestImplementation("org.hypertrace.config.service:alerting-config-service-api:0.1.12")
}

tasks.test {
  useJUnitPlatform()
}

tasks.integrationTest {
  useJUnitPlatform()
}

tasks.run<JavaExec> {
  jvmArgs = listOf("-Dservice.name=${project.name}")
}
