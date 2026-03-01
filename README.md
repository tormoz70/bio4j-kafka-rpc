# Kafka RPC

Библиотека для RPC поверх Apache Kafka, аналогичная gRPC по API. Использует Protocol Buffers для сериализации и генерирует Java-код из `.proto` файлов.

**Особенности:**
- Java 21+
- Gradle (Groovy DSL)
- Lombok
- Только unary RPC (без streaming)
- protoc-плагин для генерации клиентских stub'ов и серверной базы
- Runtime на `kafka-clients`

## Модули

| Модуль | Описание |
|--------|----------|
| `kafka-rpc-runtime` | Runtime: KafkaRpcChannel, KafkaRpcServer |
| `kafka-rpc-protoc` | protoc-плагин для генерации кода |
| `example` | Пример сервиса Greeter |

## Использование

### 1. Зависимости

```groovy
implementation 'io.bio4j:kafka-rpc-runtime:0.1.0-SNAPSHOT'
```

### 2. proto-файл

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

### 3. Генерация кода (Gradle)

```groovy
plugins {
    id 'com.google.protobuf' version '0.9.4'
}

dependencies {
    implementation project(':kafka-rpc-protoc')  // или io.bio4j:kafka-rpc-protoc:0.1.0-SNAPSHOT
}

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:3.25.5" }
    plugins {
        kafkaRpc { path = '<path-to-protoc-gen-kafka-rpc>' }
    }
    generateProtoTasks {
        ofSourceSet('main').each { task ->
            task.plugins { kafkaRpc {} }
        }
    }
}
```

### 4. Сервер

```java
var impl = new GreeterKafkaRpc.ServiceBase() {
  @Override public String getRequestTopic() { return "greeter.request"; }
  @Override public String getReplyTopic() { return "greeter.reply"; }
  @Override protected GetGreetingResponse getGreeting(GetGreetingRequest req) {
    return GetGreetingResponse.newBuilder().setGreeting("Hello, " + req.getName()).build();
  }
};
var server = new KafkaRpcServer(consumerConfig, producerConfig,
    impl.getRequestTopic(), impl.getReplyTopic(), impl.getHandlers());
server.start();
```

### 5. Клиент

```java
try (var channel = new KafkaRpcChannel(producerConfig, consumerConfig,
        "greeter.request", "greeter.reply")) {
  var stub = new GreeterKafkaRpc.Stub(channel, "greeter.request", "greeter.reply");
  var resp = stub.getGreeting(GetGreetingRequest.newBuilder().setName("World").build());
  System.out.println(resp.getGreeting());
}
```

## Сборка

```bash
./gradlew clean build
# или на Windows:
gradlew.bat clean build
```

## Пример

Запуск Kafka (Docker):

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka
```

Запуск сервера и клиента:

```bash
# Терминал 1 — сервер
./gradlew :example:runServer

# Терминал 2 — клиент
./gradlew :example:run
```
