plugins {
    java
    application
    jacoco
    id("org.hypertrace.jacoco-report-plugin")
    id("org.hypertrace.docker-java-application-plugin")
    id("org.hypertrace.docker-publish-plugin")
    id("org.hypertrace.integration-test-plugin")
}

application {
    mainClass.set("org.hypertrace.core.serviceframework.PlatformServiceLauncher")
}

dependencies {
    constraints {
        implementation("com.fasterxml.jackson.core:jackson-databind:2.12.1")
        implementation("org.apache.httpcomponents:httpclient:4.5.13")
    }
    implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.32")
    implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.32")

    implementation(project(":metric-anomaly-data-model"))
    implementation(project(":metric-anomaly-task-manager"))
    implementation(project(":metric-anomaly-detector"))
    implementation(project(":notification-service"))

    implementation("org.hypertrace.core.documentstore:document-store:0.6.7")

    // Logging
    implementation("org.slf4j:slf4j-api:1.7.30")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")

    // framework + libs
    implementation("com.google.protobuf:protobuf-java-util:3.19.4")
    implementation("org.quartz-scheduler:quartz:2.3.2")

    // integration test
    integrationTestImplementation("org.junit.jupiter:junit-jupiter-api:5.7.1")
    integrationTestImplementation("org.junit.jupiter:junit-jupiter-params:5.7.1")
    integrationTestImplementation("org.junit.jupiter:junit-jupiter-engine:5.7.1")
    integrationTestImplementation("org.testcontainers:testcontainers:1.15.2")
    integrationTestImplementation("org.testcontainers:junit-jupiter:1.15.2")
    integrationTestImplementation("org.testcontainers:kafka:1.15.2")
    integrationTestImplementation("org.hypertrace.core.serviceframework:integrationtest-service-framework:0.1.32")
    integrationTestImplementation("com.github.stefanbirkner:system-lambda:1.2.0")

    integrationTestImplementation("org.apache.kafka:kafka-clients:5.5.1-ccs")
    integrationTestImplementation("org.apache.kafka:kafka-streams:5.5.1-ccs")
    integrationTestImplementation("org.apache.avro:avro:1.10.2")
    integrationTestImplementation("com.google.guava:guava:30.1.1-jre")
    integrationTestImplementation("org.hypertrace.core.datamodel:data-model:0.1.12")
    integrationTestImplementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-serdes:0.1.13")
    integrationTestImplementation("org.hypertrace.core.query.service:query-service-client:0.6.2")
    integrationTestImplementation("org.hypertrace.core.attribute.service:attribute-service-client:0.12.0")
    integrationTestImplementation("com.squareup.okhttp3:mockwebserver:4.9.3")
}

tasks.run<JavaExec> {
    jvmArgs = listOf("-Dservice.name=${project.name}")
}

tasks.test {
    useJUnitPlatform()
}

tasks.integrationTest {
    useJUnitPlatform()
}
