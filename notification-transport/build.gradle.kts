import com.google.protobuf.gradle.generateProtoTasks
import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.ofSourceSet
import com.google.protobuf.gradle.plugins
import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
  `java-library`
  id("com.google.protobuf") version "0.8.18"
  id("io.freefair.lombok")
}

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:3.19.4"
  }
  plugins {
    id("grpc") {
      artifact = "io.grpc:protoc-gen-grpc-java:1.45.1"
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
  implementation("com.typesafe:config:1.4.1")
  implementation("com.sendgrid:sendgrid-java:4.8.2")
  constraints {
    implementation("commons-codec:commons-codec:1.13") {
      because("https://snyk.io/vuln/SNYK-JAVA-COMMONSCODEC-561518")
    }
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.3") {
      because("https://snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-2326698")
    }
    implementation("org.jetbrains.kotlin:kotlin-stdlib:1.6.0") {
      because("https://snyk.io/vuln/SNYK-JAVA-ORGJETBRAINSKOTLIN-2628385")
    }
  }
  implementation("com.squareup.okhttp3:okhttp:4.9.3")
  implementation("com.google.guava:guava:30.1-jre")
  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
}

tasks.test {
  useJUnitPlatform()
}
