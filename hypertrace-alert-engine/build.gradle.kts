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

}

tasks.run<JavaExec> {
    jvmArgs = listOf("-Dservice.name=${project.name}")
}