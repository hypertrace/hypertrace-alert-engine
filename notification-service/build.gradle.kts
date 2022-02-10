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

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.32")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.32")
  implementation("org.hypertrace.core.documentstore:document-store:0.6.7")
  implementation("org.hypertrace.config.service:notification-channel-config-service-api:0.1.12")
  implementation("org.hypertrace.config.service:config-proto-converter:0.1.12")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.6.2")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.6.2")

  implementation(project(":metric-anomaly-data-model"))
  implementation(project(":notification-transport"))

  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.6.2")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.32")
  implementation("com.typesafe:config:1.4.1")
  implementation("org.apache.kafka:kafka-clients:2.6.0")
  implementation("com.google.protobuf:protobuf-java-util:3.19.4")
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-serdes:0.1.23")
  implementation("io.confluent:kafka-streams-avro-serde:6.0.1")
  constraints {
    implementation("org.glassfish.jersey.core:jersey-common:2.34") {
      because("https://snyk.io/vuln/SNYK-JAVA-ORGGLASSFISHJERSEYCORE-1255637")
    }
  }

  annotationProcessor("org.projectlombok:lombok:1.18.18")
  compileOnly("org.projectlombok:lombok:1.18.18")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")

  runtimeOnly("io.grpc:grpc-netty:1.42.2")
  constraints {
    runtimeOnly("io.netty:netty-codec-http2:4.1.71.Final")
    runtimeOnly("io.netty:netty-handler-proxy:4.1.71.Final")
  }

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
  testImplementation("org.mockito:mockito-core:3.9.0")
  testImplementation("org.mockito:mockito-inline:3.9.0")
  testImplementation("org.junit-pioneer:junit-pioneer:1.3.8")
  testImplementation("com.squareup.okhttp3:mockwebserver:4.9.3")
}

tasks.test {
  useJUnitPlatform()
}

tasks.run<JavaExec> {
  jvmArgs = listOf("-Dservice.name=${project.name}")
}
