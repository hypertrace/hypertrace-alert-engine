import com.google.protobuf.gradle.generateProtoTasks
import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.ofSourceSet
import com.google.protobuf.gradle.plugins
import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
    `java-library`
    id("com.google.protobuf") version "0.8.12"
    id("io.freefair.lombok")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.12.3"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.30.2"
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach { task ->
            task.plugins {
                id("grpc")
            }
        }
    }
}

dependencies {
    constraints {
        implementation("com.fasterxml.jackson.core:jackson-databind:2.12.1")
        implementation("org.apache.httpcomponents:httpclient:4.5.13")
    }
    implementation("com.typesafe:config:1.4.1")
    implementation("com.sendgrid:sendgrid-java:4.7.1")
    implementation("com.squareup.okhttp3:okhttp:4.9.1")
    implementation("com.google.guava:guava:30.1-jre")
    // Logging
    implementation("org.slf4j:slf4j-api:1.7.30")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.14.0")

    testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
}

tasks.test {
    useJUnitPlatform()
}
