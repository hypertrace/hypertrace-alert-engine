pluginManagement {
    repositories {
        mavenLocal()
        gradlePluginPortal()
        maven("https://hypertrace.jfrog.io/artifactory/maven")
    }
}

plugins {
    id("org.hypertrace.version-settings") version "0.2.0"
}

include(":metric-anomaly-detector")
include(":metric-anomaly-task-manager")
include(":metric-anomaly-detector-api")
include(":metric-anomaly-rule-api")
