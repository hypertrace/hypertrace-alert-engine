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
    id("com.commercehub.gradle.plugin.avro")
    // id("org.hypertrace.avro-plugin")  version "0.3.1"
}

val generateLocalGoGrpcFiles = false

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.15.7"
    }
    plugins {
        id("grpc_java") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.36.1"
        }

        if (generateLocalGoGrpcFiles) {
            id("grpc_go") {
                path = "<go-path>/bin/protoc-gen-go"
            }
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                // Apply the "grpc" plugin whose spec is defined above, without options.
                id("grpc_java")

                if (generateLocalGoGrpcFiles) {
                    id("grpc_go")
                }
            }
            it.builtins {
                java
                if (generateLocalGoGrpcFiles) {
                    id("go")
                }
            }
        }
    }
}

dependencies {
    api("io.grpc:grpc-protobuf:1.37.0")
    api("io.grpc:grpc-stub:1.37.0")
    api("javax.annotation:javax.annotation-api:1.3.2")
    api("org.apache.avro:avro:1.10.2")
}

sourceSets {
    main {
        java {
            srcDirs("src/main/java", "build/generated/source/proto/main/java", "build/generated/source/proto/main/grpc_java", "build/generated-main-avro-java")
        }
    }
}
