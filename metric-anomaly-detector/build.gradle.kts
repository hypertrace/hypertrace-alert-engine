plugins {
    java
    application
    jacoco
    id("org.hypertrace.jacoco-report-plugin")
    id("org.hypertrace.docker-java-application-plugin")
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

    implementation(project(":metric-anomaly-data-model"))

    implementation("org.hypertrace.core.query.service:query-service-client:0.5.2")
    implementation("org.hypertrace.core.attribute.service:attribute-service-client:0.9.3")
    implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.3.4")
    implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.23")
    implementation("com.typesafe:config:1.4.1")
    implementation("org.hypertrace.gateway.service:gateway-service-api:0.1.59")
    implementation("org.apache.kafka:kafka-clients:2.6.0")
    implementation("com.google.protobuf:protobuf-java-util:4.0.0-rc-2")
    implementation("com.google.guava:guava:30.1.1-jre")

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
