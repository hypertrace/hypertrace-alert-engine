plugins {
    java
    application
    jacoco
    id("org.hypertrace.jacoco-report-plugin")
    id("org.hypertrace.docker-java-application-plugin")
    id("org.hypertrace.integration-test-plugin")
}

application {
    mainClass.set("org.hypertrace.core.serviceframework.PlatformServiceLauncher")
}

dependencies {
    // framework + libs (internal)
    implementation(project(":metric-anomaly-data-model"))
    implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.23")
    implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.23")
    implementation("org.apache.kafka:kafka-clients:2.6.0")
    implementation("org.hypertrace.core.documentstore:document-store:0.5.7")

    // Logging
    implementation("org.slf4j:slf4j-api:1.7.30")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.14.1")

    // framework + libs
    implementation("com.google.protobuf:protobuf-java-util:4.0.0-rc-2")
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
