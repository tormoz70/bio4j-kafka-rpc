# Kafka RPC

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9-black.svg)](https://kafka.apache.org/)
[![Release](https://img.shields.io/github/v/release/tormoz70/bio4j-kafka-rpc)](https://github.com/tormoz70/bio4j-kafka-rpc/releases)

**gRPC-style RPC over Apache Kafka for Java 21+.**

Define services in `.proto`, generate client stubs and server bases with a protoc plugin, and run unary, oneway, and server-streaming calls over Kafka request/reply topics. Optional Spring Boot starter adds auto-configuration and a channel pool.

[Example project](https://github.com/tormoz70/bio4j-kafka-rpc-example) · [Release notes](https://github.com/tormoz70/bio4j-kafka-rpc/releases) · [Русская версия](README.ru.md)

## Why Kafka RPC?

| Need | Kafka RPC |
|------|-----------|
| Familiar API | Proto services → stubs / `ServiceBase`, similar to gRPC |
| Transport | Apache Kafka (durable, scalable, already in many stacks) |
| Call types | Unary, oneway, server-streaming |
| Integration | Plain `kafka-clients` or Spring Boot starter |
| Codegen | Dedicated `protoc-gen-kafka-rpc` plugin |

## Features

- Java 21+, Gradle (Groovy DSL), Lombok
- Unary RPC, oneway, and server-streaming
- protoc plugin for client stubs and server base classes
- Runtime on Apache Kafka `kafka-clients`
- Spring Boot starter with channel pool and YAML config
- Apache License 2.0

## Modules

| Module | Description |
|--------|-------------|
| `kafka-rpc-runtime` | Core runtime: `KafkaRpcChannel`, `PooledKafkaRpcChannel`, `KafkaRpcServer` |
| `kafka-rpc-spring-boot-starter` | Auto-configuration, `KafkaRpcProperties`, `KafkaRpcChannelPool` |
| `kafka-rpc-protoc` | protoc plugin for code generation |

Coordinates: `ru.sbrf.uamc:<module>:1.7`

> Artifacts are published to your Maven repository (or built from source). Version `1.7` matches the [GitHub Release](https://github.com/tormoz70/bio4j-kafka-rpc/releases/tag/v1.7).

## Quick start

### 1. Dependencies

**Runtime only:**

```groovy
dependencies {
    implementation 'ru.sbrf.uamc:kafka-rpc-runtime:1.7'
    implementation 'org.apache.kafka:kafka-clients:3.9.2'
    implementation 'com.google.protobuf:protobuf-java:3.25.8'
}
```

**Spring Boot:**

```groovy
plugins {
    id 'org.springframework.boot' version '3.5.11'
    id 'io.spring.dependency-management' version '1.1.4'
}

dependencies {
    implementation 'ru.sbrf.uamc:kafka-rpc-spring-boot-starter:1.7'
    implementation 'org.apache.kafka:kafka-clients:3.9.2'
    implementation 'com.google.protobuf:protobuf-java:3.25.8'
}
```

### 2. Proto service

```protobuf
syntax = "proto3";

option java_package = "com.example";
option java_multiple_files = true;

service Greeter {
  rpc GetGreeting (GetGreetingRequest) returns (GetGreetingResponse);
}

message GetGreetingRequest { string name = 1; }
message GetGreetingResponse { string greeting = 1; }
```

### 3. Generate code

Enable the Protobuf Gradle plugin and wire `protoc-gen-kafka-rpc` (see [Gradle setup](#gradle-setup-client--server) below). Generation produces:

- Client: `GreeterKafkaRpc.Stub`
- Server: `GreeterKafkaRpc.ServiceBase`

### 4. Server

```java
var impl = new GreeterKafkaRpc.ServiceBase() {
  @Override protected GetGreetingResponse getGreeting(GetGreetingRequest req) {
    return GetGreetingResponse.newBuilder().setGreeting("Hello, " + req.getName()).build();
  }
};
String requestTopic = "greeter.request"; // Spring: kafka-rpc.service.<name>.request-topic
var server = new KafkaRpcServer(consumerConfig, producerConfig,
    requestTopic, impl.getHandlers());
server.start();
```

The reply topic always comes from the client request header — the server does not configure it. Service name defaults to the proto service name in lowercase (e.g. `greeter`).

The server handles only messages with a known `kafka-rpc-method`. Unknown or unregistered methods are ignored and logged at `warn`. There is no fallback to a “single handler”.

### 5. Client

Prefer a pooled channel from `KafkaRpcChannelPool` (Spring) or build `PooledKafkaRpcChannel` manually:

```java
try (var channel = PooledKafkaRpcChannel.builder()
    .producerConfig(properties.getProducerPropertiesForClient("greeter"))
    .consumerConfig(properties.getConsumerPropertiesForClientPooled("greeter"))
    .requestTopic(properties.getRequestTopicForClient("greeter"))
    .replyTopic(properties.getReplyTopicForClient("greeter"))
    .timeoutMs(properties.getTimeoutMsForClient("greeter"))
    .build()) {
  var stub = new GreeterKafkaRpc.Stub(channel);
  var resp = stub.getGreeting(GetGreetingRequest.newBuilder().setName("World").build());
  System.out.println(resp.getGreeting());
}
```

## Gradle setup (client & server)

Requirements:

- **Java 21+** (`java.toolchain.languageVersion = JavaLanguageVersion.of(21)`)
- **Protobuf Gradle plugin** for stubs and server base generation

```groovy
plugins {
    id 'java'
    id 'com.google.protobuf' version '0.9.4'
}
```

Client and server use the **same** artifacts — there is no separate “server-only” dependency. The same `.proto` yields both stubs and `ServiceBase`.

### Option A — multi-project (this repository)

```groovy
evaluationDependsOn ':kafka-rpc-protoc'

def pluginScriptDir = layout.buildDirectory.dir('scripts')
def pluginScriptName = System.getProperty('os.name').toLowerCase().contains('windows') ? 'protoc-gen-kafka-rpc.bat' : 'protoc-gen-kafka-rpc'
def pluginExe = new File(pluginScriptDir.get().asFile, pluginScriptName)

tasks.register('prepareKafkaRpcProtocPluginScript') {
    dependsOn ':kafka-rpc-protoc:protocPluginJar'
    outputs.dir(pluginScriptDir)
    doLast {
        def pluginJar = project(':kafka-rpc-protoc').tasks.named('protocPluginJar', Jar).get().archiveFile.get().asFile
        def scriptRoot = pluginScriptDir.get().asFile
        scriptRoot.mkdirs()

        def jarPath = pluginJar.absolutePath
        new File(scriptRoot, 'protoc-gen-kafka-rpc.bat').text = "@echo off\r\njava -jar \"$jarPath\" %*\r\n"
        def sh = new File(scriptRoot, 'protoc-gen-kafka-rpc')
        sh.text = "#!/bin/sh\nexec java -jar \"$jarPath\" \"\$@\"\n"
        sh.executable = true
    }
}

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:3.25.8" }
    plugins {
        kafkaRpc { path = pluginExe.absolutePath }
    }
    generateProtoTasks {
        ofSourceSet('main').each { task ->
            task.dependsOn tasks.named('prepareKafkaRpcProtocPluginScript')
            task.builtins { java {} }
            task.plugins { kafkaRpc {} }
        }
    }
}
```

### Option B — standalone project (plugin from a repository)

Build and publish `kafka-rpc-protoc` (or use a published artifact), then:

```groovy
dependencies {
    implementation 'ru.sbrf.uamc:kafka-rpc-runtime:1.7'
    compileOnly 'ru.sbrf.uamc:kafka-rpc-protoc:1.7'
}

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:3.25.8" }
    plugins {
        kafkaRpc { path = '<path-to-protoc-gen-kafka-rpc>' }
    }
    generateProtoTasks {
        ofSourceSet('main').each { task ->
            task.builtins { java {} }
            task.plugins { kafkaRpc {} }
        }
    }
}
```

Point `path` at the script or JAR wrapper produced when building `kafka-rpc-protoc`.

### Combined client + server app

See [bio4j-kafka-rpc-example](https://github.com/tormoz70/bio4j-kafka-rpc-example): one Spring Boot app with `kafka-rpc-spring-boot-starter`, a single protobuf setup, and both `kafka-rpc.clients.*` and `kafka-rpc.service.*` in `application.yml`.

## Configuration (`application.yml`)

Full reference (keys, priorities, `group.id`, do’s and don’ts):
[`docs/application-yml-configuration.md`](docs/application-yml-configuration.md)

Under `kafka-rpc`:

- **Shared:** `bootstrap-servers`, `producer`, `consumer` — defaults for all clients and servers. Default max message size is **10 MiB** (`producer.max.request.size`, `consumer.max.partition.fetch.bytes`). Override via `kafka-rpc.producer.*` / `kafka-rpc.consumer.*` or per client/service. For payloads above ~1 MiB, raise broker (`message.max.bytes`, `replica.fetch.max.bytes`) and topic (`max.message.bytes`) limits as well.
- **Streaming (global):** `stream-healthcheck-interval-ms` (default `5000`), `stream-healthcheck-timeout-ms` (default `15000`), `stream-server-idle-timeout-ms` (default `20000`; set on the client and sent to the server in a required header when a stream starts).
- **Clients:** `clients.<name>` (lowercase service name, e.g. `greeter`): `request-topic`, `reply-topic`, optional `timeout-ms`, stream healthcheck settings, `producer` / `consumer` overrides.
- **Servers:** `service.<name>`: `request-topic`, optional `producer` / `consumer` overrides.

Topic routing and `group.id` are the application’s responsibility. If several client/service pairs share the same topic pair, avoid overlapping consumer groups and cross-talk yourself.

```yaml
kafka-rpc:
  bootstrap-servers: localhost:9092
  producer: { acks: all }
  consumer: {}
  # stream-healthcheck-interval-ms: 5000
  # stream-healthcheck-timeout-ms: 15000
  # stream-server-idle-timeout-ms: 20000
  clients:
    greeter:
      request-topic: greeter.request
      reply-topic: greeter.reply
    inventory:
      request-topic: inventory.request
      reply-topic: inventory.reply
      timeout-ms: 10000
      producer: { linger.ms: "5" }
  service:
    greeter:
      request-topic: greeter.request
    inventory:
      request-topic: inventory.request
      consumer: { max.poll.records: "100" }
```

More docs:

- [Client channel topology & consumer groups](docs/client-channel-topology-and-consumer-groups.md)
- [Pooled request timeline](docs/pooled-kafka-rpc-request-timeline.md)

## Build

```bash
./gradlew clean build
# Windows:
gradlew.bat clean build
```

## Example

Start Kafka:

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka
```

Run the [example](https://github.com/tormoz70/bio4j-kafka-rpc-example):

```bash
# Terminal 1 — server/app
./gradlew bootRun

# Terminal 2 — call
curl "http://localhost:8080/greet?name=World"
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
