plugins {
  `java-library`
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.avro-plugin")
}

dependencies {
  api("org.apache.avro:avro:1.10.2")
  api("io.grpc:grpc-protobuf:1.42.0")
  api("io.grpc:grpc-stub:1.42.0")
  api("javax.annotation:javax.annotation-api:1.3.2")
  api("org.apache.avro:avro:1.10.2")
  implementation("com.typesafe:config:1.4.1")
  implementation("org.apache.kafka:kafka-clients:2.6.0")
  implementation("org.hypertrace.core.documentstore:document-store:0.5.7")
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-serdes:0.1.23")
  implementation("io.confluent:kafka-streams-avro-serde:6.0.1")
  constraints {
    implementation("org.glassfish.jersey.core:jersey-common:2.34") {
      because("https://snyk.io/vuln/SNYK-JAVA-ORGGLASSFISHJERSEYCORE-1255637")
    }
  }

  annotationProcessor("org.projectlombok:lombok:1.18.18")
  compileOnly("org.projectlombok:lombok:1.18.18")
}
