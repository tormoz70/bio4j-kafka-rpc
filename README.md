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
  @Override protected GetGreetingResponse getGreeting(GetGreetingRequest req) {
    return GetGreetingResponse.newBuilder().setGreeting("Hello, " + req.getName()).build();
  }
};
var server = new KafkaRpcServer(consumerConfig, producerConfig,
    impl.getRequestTopic(), impl.getHandlers());
server.start();
```
Reply topic is always taken from the client request header; the server does not need it in config.

### 5. Клиент

Для каждого сервиса генерируется свой канал (например `GreeterRpcChannel`); конфигурация (producer/consumer, топики) берётся из `application.yml` через `KafkaRpcProperties`.

```java
try (var channel = new GreeterRpcChannel(properties)) {
  var stub = new GreeterKafkaRpc.Stub(channel);
  var resp = stub.getGreeting(GetGreetingRequest.newBuilder().setName("World").build());
  System.out.println(resp.getGreeting());
}
```

### 6. Конфигурация (много клиентов и серверов)

В `application.yml` под `kafka-rpc`:
- **Общие настройки:** `bootstrap-servers`, `producer`, `consumer` — база для всех клиентов и серверов.
- **Клиенты:** `clients.<имя>` (имя = сервис в нижнем регистре, например `greeter`). Для каждого: `request-topic`, `reply-topic`, опционально `timeout-ms`, `producer`, `consumer` (переопределяют общие).
- **Серверы (сервисы):** `service.<имя>`. Для каждого: `request-topic`, опционально `producer`, `consumer` (переопределяют общие).

Пример с двумя клиентами и переопределением только для одного:

```yaml
kafka-rpc:
  bootstrap-servers: localhost:9092
  producer: { acks: all }
  consumer: {}
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
