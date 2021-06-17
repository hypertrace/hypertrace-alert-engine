import com.google.protobuf.gradle.generateProtoTasks
import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.ofSourceSet
import com.google.protobuf.gradle.plugins
import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
    `java-library`
    id("com.google.protobuf") version "0.8.15"
    id("org.hypertrace.publish-plugin")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.15.7"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.37.0"
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
    api("org.apache.avro:avro:1.10.2")
    api("io.grpc:grpc-protobuf:1.37.0")
    api("io.grpc:grpc-stub:1.37.0")
    api("javax.annotation:javax.annotation-api:1.3.2")
}

sourceSets {
    main {
        java {
            srcDirs("build/generated/source/proto/main/java", "build/generated/source/proto/main/grpc")
        }
    }
}
