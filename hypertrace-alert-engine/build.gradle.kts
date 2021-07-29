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

tasks.test {
    useJUnitPlatform()
}

dependencies {
    constraints {
        implementation("com.fasterxml.jackson.core:jackson-databind:2.12.1")
        implementation("org.apache.httpcomponents:httpclient:4.5.13")
    }
    implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.23")
    implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.23")

    implementation(project(":metric-anomaly-data-model"))
    implementation(project(":metric-anomaly-task-manager"))
    implementation(project(":metric-anomaly-detector"))
    implementation(project(":notification-service"))

    implementation("org.hypertrace.core.documentstore:document-store:0.5.7")

    // Logging
    implementation("org.slf4j:slf4j-api:1.7.30")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.14.1")

    // framework + libs
    implementation("com.google.protobuf:protobuf-java-util:4.0.0-rc-2")
    implementation("org.quartz-scheduler:quartz:2.3.2")
}

tasks.run<JavaExec> {
    jvmArgs = listOf("-Dservice.name=${project.name}")
}
