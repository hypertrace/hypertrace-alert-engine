plugins {
    java
    application
    jacoco
    id("org.hypertrace.jacoco-report-plugin")
    id("org.hypertrace.docker-java-application-plugin")
}

application {
    mainClass.set("org.hypertrace.alerting.metric.anomaly.detector.MetricAnomalyDetector")
}

dependencies {
    implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.23")
    implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.23")

    // Logging
    implementation("org.slf4j:slf4j-api:1.7.30")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.14.1")
}
