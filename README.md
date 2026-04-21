# Kafka RPC

Библиотека для RPC поверх Apache Kafka, аналогичная gRPC по API. Использует Protocol Buffers для сериализации и генерирует Java-код из `.proto` файлов.

**Особенности:**
- Java 21+
- Gradle (Groovy DSL)
- Lombok
- Unary RPC, oneway и server-streaming
- protoc-плагин для генерации клиентских stub'ов и серверной базы
- Runtime на `kafka-clients`

## Модули

| Модуль | Описание |
|--------|----------|
| `kafka-rpc-runtime` | Runtime: KafkaRpcChannel, KafkaRpcServer |
| `kafka-rpc-spring-boot-starter` | Spring Boot автоконфигурация, KafkaRpcProperties, KafkaRpcChannelPool |
| `kafka-rpc-protoc` | protoc-плагин для генерации кода |

## Настройка проекта в Gradle (клиент и сервер)

Ниже — как подключить библиотеку и настроить генерацию кода в зависимости от того, собираете ли вы только клиента, только сервер или оба в одном проекте.

### Общее

- **Java:** 21+ (в корневом `build.gradle` или в проекте задайте `java.toolchain.languageVersion = JavaLanguageVersion.of(21)`).
- **Плагин Protobuf:** нужен для генерации stub'ов и серверной базы из `.proto`.

```groovy
plugins {
    id 'java'
    id 'com.google.protobuf' version '0.9.4'
}
```

### Проект-клиент

Зависимости: runtime (или Spring Boot starter, если клиент в Spring-приложении). Для генерации кода — protoc-плагин и артефакт `kafka-rpc-protoc`.

**Только runtime (без Spring):**

```groovy
dependencies {
    implementation 'ru.sbrf.uamc:kafka-rpc-runtime:0.1.0-SNAPSHOT'
    implementation 'org.apache.kafka:kafka-clients:3.9.2'
    implementation 'com.google.protobuf:protobuf-java:3.25.8'
}
```

**С Spring Boot:**

```groovy
plugins {
    id 'org.springframework.boot' version '3.5.11'
    id 'io.spring.dependency-management' version '1.1.4'
}

dependencies {
    implementation 'ru.sbrf.uamc:kafka-rpc-spring-boot-starter:0.1.0-SNAPSHOT'
    implementation 'org.apache.kafka:kafka-clients:3.9.2'
    implementation 'com.google.protobuf:protobuf-java:3.25.8'
}
```

Генерация кода (см. раздел «Генерация кода» ниже) создаёт stub'ы для клиента (`GreeterKafkaRpc.Stub`, канал и т.д.). Конфигурация топиков и Kafka — через `application.yml` (при использовании starter) или вручную.

### Проект-сервер

Те же зависимости, что и для клиента: либо `kafka-rpc-runtime` (standalone), либо `kafka-rpc-spring-boot-starter` (Spring). Генерация кода из тех же `.proto` даёт серверную базу (`GreeterKafkaRpc.ServiceBase`), которую вы наследуете и реализуете методы RPC.

Отдельных «серверных» зависимостей нет — один и тот же артефакт используется и на клиенте, и на сервере.

### Генерация кода (protoc-плагин)

Плагин генерирует и клиентские stub'ы, и серверную базу. Подключение плагина и путь к `protoc-gen-kafka-rpc` зависят от того, как вы подключаете `kafka-rpc-protoc`:

**Вариант A — мультипроект Gradle (как в этом репозитории):**

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

**Вариант B — отдельный проект, плагин из репозитория:**

Сначала соберите и опубликуйте `kafka-rpc-protoc` (или используйте готовый артефакт). Затем в проекте клиента/сервера:

```groovy
dependencies {
    implementation 'ru.sbrf.uamc:kafka-rpc-runtime:0.1.0-SNAPSHOT'
    // для генерации (compileOnly или отдельная конфигурация):
    compileOnly 'ru.sbrf.uamc:kafka-rpc-protoc:0.1.0-SNAPSHOT'
}

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:3.25.8" }
    plugins {
        kafkaRpc { path = '<путь к исполняемому файлу protoc-gen-kafka-rpc>' }
    }
    generateProtoTasks {
        ofSourceSet('main').each { task ->
            task.builtins { java {} }
            task.plugins { kafkaRpc {} }
        }
    }
}
```

Путь к `protoc-gen-kafka-rpc` укажите после распаковки/сборки плагина (скрипт или jar, см. `kafka-rpc-protoc` в репозитории).

### Один проект (и клиент, и сервер)

Как в отдельном проекте `bio4j-kafka-rpc-example`: подключаете `kafka-rpc-spring-boot-starter` (или runtime + свои конфиги), настраиваете protobuf-плагин один раз. Из одних и тех же `.proto` получаете и stub'ы для клиента, и `ServiceBase` для сервера; в приложении используете и те, и другие (конфигурация через `kafka-rpc.clients.*` и `kafka-rpc.service.*` в `application.yml`).

## Использование

### 1. Зависимости

```groovy
implementation 'ru.sbrf.uamc:kafka-rpc-runtime:0.1.0-SNAPSHOT'
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

Полная настройка для клиента, сервера и мультипроекта — в разделе [Настройка проекта в Gradle (клиент и сервер)](#настройка-проекта-в-gradle-клиент-и-сервер). Минимальный пример:

```groovy
plugins {
    id 'com.google.protobuf' version '0.9.4'
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

### 4. Сервер

```java
var impl = new GreeterKafkaRpc.ServiceBase() {
  @Override protected GetGreetingResponse getGreeting(GetGreetingRequest req) {
    return GetGreetingResponse.newBuilder().setGreeting("Hello, " + req.getName()).build();
  }
};
String requestTopic = "greeter.request"; // in Spring Boot, resolved from kafka-rpc.service.<name>.request-topic
var server = new KafkaRpcServer(consumerConfig, producerConfig,
    requestTopic, impl.getHandlers());
server.start();
```
Reply topic is always taken from the client request header; the server does not need it in config.
Service name defaults to the proto service name (e.g. `greeter` for `service Greeter`); the request topic is resolved from `kafka-rpc.service.<name>.request-topic` by the auto-configuration.

Важно: сервер обрабатывает только сообщения с известным `kafka-rpc-method`. Если метод отсутствует или не зарегистрирован у данного сервиса, сообщение игнорируется и пишется `warn` в лог. Fallback на «единственный handler» не используется.

### 5. Клиент

Для каждого сервиса используйте pooled-канал из `KafkaRpcChannelPool` (Spring) или создавайте `PooledKafkaRpcChannel` вручную (без Spring).

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

### 6. Конфигурация (много клиентов и серверов)

Подробный справочник по `application.yml` (все ключи, приоритеты, `group.id`, кейсы и анти-кейсы):
[`docs/application-yml-configuration.md`](docs/application-yml-configuration.md)

В `application.yml` под `kafka-rpc`:
- **Общие настройки:** `bootstrap-servers`, `producer`, `consumer` — база для всех клиентов и серверов. По умолчанию максимальный размер сообщения — 10 MiB (`producer.max.request.size` и `consumer.max.partition.fetch.bytes`); переопределяется через `kafka-rpc.producer.*` / `kafka-rpc.consumer.*` или per-client/per-service. Для сообщений больше 1 MiB также потребуется поднять лимиты на брокере (`message.max.bytes`, `replica.fetch.max.bytes`) и на топике (`max.message.bytes`).
- **Стриминг (глобально):** `stream-healthcheck-interval-ms` (интервал хелсчека клиента, по умолчанию 5000), `stream-healthcheck-timeout-ms` (таймаут «стрим мёртв» на клиенте, 15000), `stream-server-idle-timeout-ms` (таймаут простоя стрима на сервере; задаётся только на клиенте, передаётся в обязательном заголовке при старте стрима; по умолчанию 20000).
- **Клиенты:** `clients.<имя>` (имя = сервис в нижнем регистре, например `greeter`). Для каждого: `request-topic`, `reply-topic`, опционально `timeout-ms`, `stream-healthcheck-enabled`, `stream-healthcheck-interval-ms`, `stream-healthcheck-timeout-ms`, `stream-server-idle-timeout-ms` (передаётся серверу в заголовке), `producer`, `consumer` (переопределяют общие).
- **Серверы (сервисы):** `service.<имя>`. Для каждого: `request-topic`, опционально `producer`, `consumer` (переопределяют общие).

Роутинг по топикам и `group.id` — ответственность прикладного разработчика. При общей паре топиков для нескольких связок client/service необходимо самостоятельно исключить пересечение консьюмер-групп и «чужие» сообщения.

Пример с двумя клиентами и переопределением только для одного:

```yaml
kafka-rpc:
  bootstrap-servers: localhost:9092
  producer: { acks: all }
  consumer: {}
  # опционально: стриминг (значения по умолчанию)
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
      # stream-healthcheck-interval-ms: 3000
      # stream-healthcheck-timeout-ms: 10000
      # stream-server-idle-timeout-ms: 25000  # передаётся серверу в заголовке при старте стрима
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
# Терминал 1 — сервер (в проекте bio4j-kafka-rpc-example)
./gradlew bootRun

# Терминал 2 — клиент
curl "http://localhost:8080/greet?name=World"
```
