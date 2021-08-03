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
    implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.23")
    implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.23")
    implementation("org.hypertrace.core.documentstore:document-store:0.5.7")

    implementation(project(":metric-anomaly-task-manager"))
    implementation(project(":metric-anomaly-data-model"))
    implementation(project(":notification-transport"))

    implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.3.4")
    implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.23")
    implementation("com.typesafe:config:1.4.1")
    implementation("org.apache.kafka:kafka-clients:2.6.0")
    implementation("com.google.protobuf:protobuf-java-util:4.0.0-rc-2")

    annotationProcessor("org.projectlombok:lombok:1.18.18")
    compileOnly("org.projectlombok:lombok:1.18.18")

    // Logging
    implementation("org.slf4j:slf4j-api:1.7.30")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.14.1")
    implementation("io.grpc:grpc-netty:1.37.0")

    testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
    testImplementation("org.mockito:mockito-core:3.9.0")
    testImplementation("org.mockito:mockito-inline:3.9.0")
    testImplementation("org.junit-pioneer:junit-pioneer:1.3.8")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.9.1")
}

tasks.test {
    useJUnitPlatform()
}

tasks.run<JavaExec> {
    jvmArgs = listOf("-Dservice.name=${project.name}")
}
