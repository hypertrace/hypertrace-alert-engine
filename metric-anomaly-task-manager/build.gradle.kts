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

dependencies {
    // framework + libs (internal)
    implementation(project(":metric-anomaly-data-model"))
    implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.23")
    implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.23")
    implementation("org.hypertrace.config.service:config-proto-converter:0.1.3")
    implementation("org.apache.kafka:kafka-clients:2.6.0")

    // Logging
    implementation("org.slf4j:slf4j-api:1.7.30")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.14.1")

    // framework + libs
    implementation("com.google.protobuf:protobuf-java-util:4.0.0-rc-2")
    implementation("org.quartz-scheduler:quartz:2.3.2")

    // testing
    testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
}

tasks.test {
    useJUnitPlatform()
}

tasks.run<JavaExec> {
    jvmArgs = listOf("-Dservice.name=${project.name}")
}
