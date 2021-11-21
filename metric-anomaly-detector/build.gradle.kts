plugins {
    java
    application
    jacoco
    id("org.hypertrace.jacoco-report-plugin")
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
    implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.28")
    implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.28")
    implementation("org.apache.commons:commons-lang3:3.10")
    implementation("org.apache.commons:commons-math:2.2")

    implementation(project(":metric-anomaly-data-model"))

    implementation("org.hypertrace.core.query.service:query-service-client:0.5.2")
    implementation("org.hypertrace.core.attribute.service:attribute-service-client:0.9.3")
    implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.3.4")
    implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.28")
    implementation("com.typesafe:config:1.4.1")
    implementation("org.hypertrace.gateway.service:gateway-service-api:0.1.59")
    implementation("org.hypertrace.gateway.service:gateway-service-baseline-lib:0.1.167")
    implementation("org.apache.kafka:kafka-clients:2.8.1")
    implementation("com.google.protobuf:protobuf-java-util:4.0.0-rc-2")
    implementation("com.google.guava:guava:30.1.1-jre")
    implementation("org.hypertrace.config.service:alerting-config-service-api:0.1.12")
    implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-serdes:0.1.22")
    implementation("io.confluent:kafka-streams-avro-serde:6.0.1")

    annotationProcessor("org.projectlombok:lombok:1.18.18")
    compileOnly("org.projectlombok:lombok:1.18.18")

    // Logging
    implementation("org.slf4j:slf4j-api:1.7.30")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.14.1")

    implementation("io.grpc:grpc-netty:1.37.0")
    testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
    testImplementation("org.mockito:mockito-core:3.9.0")
    testImplementation("org.mockito:mockito-inline:3.9.0")
}

tasks.run<JavaExec> {
    jvmArgs = listOf("-Dservice.name=${project.name}")
}
