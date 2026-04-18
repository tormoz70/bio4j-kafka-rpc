# Подробная конфигурация `application.yml` для Kafka RPC

Документ описывает конфигурацию Spring Boot starter (`kafka-rpc-spring-boot-starter`) для клиентской и серверной частей, а также типовые варианты настройки топиков, групп и Kafka-параметров.

## Содержание

- [1. Быстрый минимум](#1-быстрый-минимум)
- [2. Все ключи kafka-rpc](#2-все-ключи-kafka-rpc)
- [3. Как формируется group.id](#3-как-формируется-groupid)
- [4. Варианты описания топиков](#4-варианты-описания-топиков)
- [5. Варианты настройки клиента и сервера](#5-варианты-настройки-клиента-и-сервера)
- [6. Kafka producer/consumer overrides](#6-kafka-producerconsumer-overrides)
- [7. Стриминг: healthcheck, timeout, backpressure](#7-стриминг-healthcheck-timeout-backpressure)
- [8. Частые ошибки и диагностика](#8-частые-ошибки-и-диагностика)
- [9. Готовые шаблоны для dev/stage/prod](#9-готовые-шаблоны-для-devstageprod)
- [10. Ролевые пресеты](#10-ролевые-пресеты)

## 1. Быстрый минимум

### Минимум для клиента

```yaml
kafka-rpc:
  bootstrap-servers: localhost:9092
  clients:
    greeter:
      request-topic: greeter.request
      reply-topic: greeter.reply
```

### Минимум для сервера

```yaml
kafka-rpc:
  bootstrap-servers: localhost:9092
  service:
    greeter:
      request-topic: greeter.request
```

### Клиент и сервер в одном приложении

```yaml
kafka-rpc:
  bootstrap-servers: localhost:9092
  clients:
    greeter:
      request-topic: greeter.request
      reply-topic: greeter.reply
  service:
    greeter:
      request-topic: greeter.request
```

## 2. Все ключи kafka-rpc

Ниже перечислены актуальные ключи, которые реально используются в коде starter.

### 2.1 Глобальные ключи

| Ключ | Тип | По умолчанию | Назначение |
|---|---|---|---|
| `kafka-rpc.bootstrap-servers` | `string` | `localhost:9092` | Kafka bootstrap servers |
| `kafka-rpc.server-enabled` | `boolean` | `true` | Включить серверную автоконфигурацию |
| `kafka-rpc.server-consumer-count` | `int` | `1` | Число consumer threads сервера |
| `kafka-rpc.client-consumer-count` | `int` | `1` | Число consumer threads клиента |
| `kafka-rpc.timeout-ms` | `int` | `30000` | Таймаут unary RPC по умолчанию |
| `kafka-rpc.poll-interval-ms` | `int` | `100` | Интервал poll для consumer |
| `kafka-rpc.stream-buffer-size` | `int` | `1024` | Размер буфера стрима на клиенте |
| `kafka-rpc.stream-healthcheck-interval-ms` | `int` | `5000` | Интервал healthcheck для stream RPC |
| `kafka-rpc.stream-healthcheck-timeout-ms` | `int` | `15000` | Таймаут healthcheck для stream RPC |
| `kafka-rpc.stream-server-idle-timeout-ms` | `int` | `20000` | Таймаут idle на сервере (передается клиентом в заголовке) |
| `kafka-rpc.producer.*` | `map<string,string>` | `{}` | Глобальные overrides Kafka Producer |
| `kafka-rpc.consumer.*` | `map<string,string>` | `{}` | Глобальные overrides Kafka Consumer |

### 2.2 Клиенты (`kafka-rpc.clients.<clientName>`)

| Ключ | Тип | Обязателен | Назначение |
|---|---|---|---|
| `request-topic` | `string` | да | Топик запросов |
| `reply-topic` | `string` | да | Топик ответов |
| `group-id` | `string` | нет | Явный `group.id` для consumer group клиента. По умолчанию: `<reply-topic>-group` |
| `timeout-ms` | `int` | нет | Таймаут только для этого клиента |
| `consumer-count` | `int` | нет | Число consumer threads клиента |
| `poll-interval-ms` | `int` | нет | Poll interval только для клиента |
| `stream-buffer-size` | `int` | нет | Буфер стрима только для клиента |
| `stream-healthcheck-enabled` | `boolean` | нет | Вкл/выкл healthcheck стрима |
| `stream-healthcheck-interval-ms` | `int` | нет | Интервал healthcheck клиента |
| `stream-healthcheck-timeout-ms` | `int` | нет | Таймаут healthcheck клиента |
| `stream-server-idle-timeout-ms` | `int` | нет | idle timeout, который клиент отправляет серверу |
| `producer.*` | `map<string,string>` | нет | Kafka Producer overrides для клиента |
| `consumer.*` | `map<string,string>` | нет | Kafka Consumer overrides для клиента |

### 2.3 Сервисы (`kafka-rpc.service.<serviceName>`)

| Ключ | Тип | Обязателен | Назначение |
|---|---|---|---|
| `request-topic` | `string` | да | Топик запросов сервиса |
| `group-id` | `string` | нет | Явный `group.id` для consumer group сервиса. По умолчанию: `<request-topic>-group` |
| `consumer-count` | `int` | нет | Число consumer threads сервера для сервиса |
| `poll-interval-ms` | `int` | нет | Poll interval для сервиса |
| `producer.*` | `map<string,string>` | нет | Kafka Producer overrides для сервиса |
| `consumer.*` | `map<string,string>` | нет | Kafka Consumer overrides для сервиса |

## 3. Как формируется group.id

### Сервер

Для каждого сервиса `group.id` формируется так:

1. `kafka-rpc.service.<name>.group-id` — если задан явно
2. иначе `<request-topic>-group`

Пример:

```yaml
kafka-rpc:
  service:
    greeter:
      request-topic: greeter.request
      # group.id = greeter.request-group (по умолчанию)
    echo:
      request-topic: echo.request
      group-id: my-echo-group
      # group.id = my-echo-group (задан явно)
```

### Клиент

Для каждого клиента `group.id` формируется так:

1. `kafka-rpc.clients.<name>.group-id` — если задан явно
2. иначе `<reply-topic>-group`

Пример:

```yaml
kafka-rpc:
  clients:
    greeter:
      request-topic: greeter.request
      reply-topic: greeter.reply
      # group.id = greeter.reply-group (по умолчанию)
    pricing:
      request-topic: pricing.request
      reply-topic: pricing.reply
      group-id: my-pricing-group
      # group.id = my-pricing-group (задан явно)
```

## 4. Варианты описания топиков

### Вариант A: один клиент -> один сервис

```yaml
kafka-rpc:
  clients:
    greeter:
      request-topic: greeter.request
      reply-topic: greeter.reply
  service:
    greeter:
      request-topic: greeter.request
```

### Вариант B: несколько клиентов в одном приложении

```yaml
kafka-rpc:
  clients:
    greeter:
      request-topic: greeter.request
      reply-topic: greeter.reply
    echo:
      request-topic: echo.request
      reply-topic: echo.reply
```

### Вариант C: несколько серверных сервисов в одном приложении

```yaml
kafka-rpc:
  service:
    greeter:
      request-topic: greeter.request
    inventory:
      request-topic: inventory.request
```

### Вариант D: разные reply-topic на разных окружениях

```yaml
# application-dev.yml
kafka-rpc:
  clients:
    greeter:
      request-topic: greeter.request.dev
      reply-topic: greeter.reply.dev

# application-prod.yml
kafka-rpc:
  clients:
    greeter:
      request-topic: greeter.request
      reply-topic: greeter.reply
```

## 5. Варианты настройки клиента и сервера

### 5.1 Только клиент

Если приложение только вызывает RPC, можно отключить сервер:

```yaml
kafka-rpc:
  server-enabled: false
  clients:
    billing:
      request-topic: billing.request
      reply-topic: billing.reply
      timeout-ms: 10000
```

### 5.2 Только сервер

Если приложение только обрабатывает RPC:

```yaml
kafka-rpc:
  server-enabled: true
  service:
    billing:
      request-topic: billing.request
      consumer-count: 4
```

### 5.3 Клиент и сервер вместе

```yaml
kafka-rpc:
  clients:
    greeter:
      request-topic: greeter.request
      reply-topic: greeter.reply
  service:
    greeter:
      request-topic: greeter.request
      group-id: my-rpc-server-greeter
```

### 5.4 Масштабирование через partitioning

```yaml
kafka-rpc:
  server-consumer-count: 4
  client-consumer-count: 2
  service:
    greeter:
      request-topic: greeter.request
  clients:
    greeter:
      request-topic: greeter.request
      reply-topic: greeter.reply
```

Рекомендация: количество partitions request/reply topics должно быть не меньше соответствующего `consumer-count`.

## 6. Kafka producer/consumer overrides

### Значения по умолчанию starter'а

Starter проставляет следующие безопасные дефолты поверх `bootstrap.servers` и сериализаторов:

- Producer: `acks=all`, `enable.idempotence=true`, `retries=10`, `max.in.flight.requests.per.connection=5`, `request.timeout.ms=30000`, `delivery.timeout.ms=120000`, **`max.request.size=10485760` (10 MiB)**.
- Consumer: `auto.offset.reset` (`earliest` для сервера, `latest` для клиента), **`max.partition.fetch.bytes=10485760` (10 MiB)**.

Максимальный размер сообщения по умолчанию — 10 MiB. Чтобы реально пересылать сообщения больше 1 MiB, также увеличьте лимиты на брокере (`message.max.bytes`, `replica.fetch.max.bytes`) и на топике (`max.message.bytes`). Любой дефолт переопределяется через `kafka-rpc.producer.*` / `kafka-rpc.consumer.*` или per-client/per-service maps.

### 6.1 Глобальные настройки для всех

```yaml
kafka-rpc:
  producer:
    acks: all
    linger.ms: "5"
    compression.type: lz4
    # по умолчанию 10485760 (10 MiB); увеличьте/уменьшите при необходимости
    max.request.size: "10485760"
  consumer:
    max.poll.records: "500"
    fetch.min.bytes: "1"
    # по умолчанию 10485760 (10 MiB)
    max.partition.fetch.bytes: "10485760"
```

### 6.2 Переопределение только для одного клиента

```yaml
kafka-rpc:
  producer:
    acks: all
  clients:
    slow-service:
      request-topic: slow.request
      reply-topic: slow.reply
      producer:
        linger.ms: "20"
      consumer:
        max.poll.records: "50"
```

### 6.3 Переопределение только для одного сервиса

```yaml
kafka-rpc:
  service:
    heavy:
      request-topic: heavy.request
      consumer:
        max.poll.records: "1000"
        max.partition.fetch.bytes: "1048576"
```

Приоритет такой:

1. базовые значения starter
2. `kafka-rpc.producer` / `kafka-rpc.consumer`
3. `kafka-rpc.clients.<name>.*` или `kafka-rpc.service.<name>.*`

## 7. Стриминг: healthcheck, timeout, backpressure

### 7.1 Глобальная настройка stream RPC

```yaml
kafka-rpc:
  stream-healthcheck-interval-ms: 5000
  stream-healthcheck-timeout-ms: 15000
  stream-server-idle-timeout-ms: 20000
  stream-buffer-size: 1024
```

### 7.2 Тюнинг только для одного клиента

```yaml
kafka-rpc:
  clients:
    analytics:
      request-topic: analytics.request
      reply-topic: analytics.reply
      stream-healthcheck-interval-ms: 3000
      stream-healthcheck-timeout-ms: 10000
      stream-server-idle-timeout-ms: 25000
      stream-buffer-size: 4096
```

### 7.3 Негативный кейс: отключение healthcheck

```yaml
kafka-rpc:
  clients:
    greeter:
      request-topic: greeter.request
      reply-topic: greeter.reply
      stream-healthcheck-enabled: false
```

Так обычно используют только в тестовых сценариях. В production лучше оставлять `true`.

## 8. Частые ошибки и диагностика

### Ошибка: не заданы client topics

Сообщение вида:

`kafka-rpc.clients.<name>.request-topic and .reply-topic must be set`

Что проверить:

- есть ли `kafka-rpc.clients.<name>.request-topic`
- есть ли `kafka-rpc.clients.<name>.reply-topic`
- совпадает ли `<name>` с именем, которое использует generated stub provider

### Ошибка: не задан service request-topic

Сообщение вида:

`kafka-rpc.service.<service>.request-topic must be set`

Что проверить:

- есть ли блок `kafka-rpc.service.<service>`
- задан ли `request-topic`

### Таймауты/нестабильные ответы

Что обычно помогает:

- увеличить `kafka-rpc.timeout-ms` или `clients.<name>.timeout-ms`
- проверить `consumer-count` и число partitions
- для стриминга проверить `stream-healthcheck-*` и `stream-server-idle-timeout-ms`

### Конфликт групп и нежелательная конкуренция консюмеров

Если несколько приложений случайно используют одинаковую базу `group.id`, они могут делить партиции не так, как ожидалось. Обычно лучше явно задавать:

```yaml
kafka-rpc:
  consumer:
    group.id: my-app-rpc
```

а дальше starter добавит суффикс `-<clientName>` для каждого клиента.

## 9. Готовые шаблоны для dev/stage/prod

Ниже примеры, которые можно использовать как стартовые `application-*.yml`.

### 9.1 `application-dev.yml` (локальная разработка)

```yaml
spring:
  application:
    name: billing-app

kafka-rpc:
  bootstrap-servers: localhost:9092
  server-enabled: true

  # Небольшие значения для быстрой обратной связи
  timeout-ms: 10000
  poll-interval-ms: 100
  stream-buffer-size: 512
  stream-healthcheck-interval-ms: 3000
  stream-healthcheck-timeout-ms: 9000
  stream-server-idle-timeout-ms: 12000

  producer:
    acks: all
    linger.ms: "5"
  consumer:
    auto.offset.reset: earliest

  clients:
    pricing:
      request-topic: pricing.request.dev
      reply-topic: billing.reply.dev
      timeout-ms: 8000
      consumer-count: 1
    profile:
      request-topic: profile.request.dev
      reply-topic: billing.reply.dev

  service:
    billing:
      request-topic: billing.request.dev
      consumer-count: 1
```

### 9.2 `application-stage.yml` (интеграционный стенд)

```yaml
spring:
  application:
    name: billing-app

kafka-rpc:
  bootstrap-servers: kafka-stage-1:9092,kafka-stage-2:9092
  server-enabled: true

  timeout-ms: 20000
  poll-interval-ms: 100
  stream-buffer-size: 2048
  stream-healthcheck-interval-ms: 5000
  stream-healthcheck-timeout-ms: 15000
  stream-server-idle-timeout-ms: 20000

  # Небольшое масштабирование
  client-consumer-count: 2
  server-consumer-count: 2

  producer:
    acks: all
    compression.type: lz4
  consumer:
    max.poll.records: "500"

  clients:
    pricing:
      request-topic: pricing.request.stage
      reply-topic: billing.reply.stage
      consumer-count: 2
    profile:
      request-topic: profile.request.stage
      reply-topic: billing.reply.stage
      consumer:
        max.poll.records: "200"

  service:
    billing:
      request-topic: billing.request.stage
      consumer-count: 2
      consumer:
        max.poll.records: "1000"
```

### 9.3 `application-prod.yml` (production)

```yaml
spring:
  application:
    name: billing-app

kafka-rpc:
  bootstrap-servers: kafka-prod-1:9092,kafka-prod-2:9092,kafka-prod-3:9092
  server-enabled: true

  timeout-ms: 30000
  poll-interval-ms: 100
  stream-buffer-size: 4096
  stream-healthcheck-interval-ms: 5000
  stream-healthcheck-timeout-ms: 15000
  stream-server-idle-timeout-ms: 20000

  # Масштабирование под нагрузку (подбирайте по числу partitions)
  client-consumer-count: 4
  server-consumer-count: 4

  producer:
    acks: all
    enable.idempotence: "true"
    compression.type: lz4
    linger.ms: "5"
  consumer:
    max.poll.records: "1000"
    fetch.min.bytes: "1"

  clients:
    pricing:
      request-topic: pricing.request
      reply-topic: billing.reply
      timeout-ms: 15000
      consumer-count: 4
    profile:
      request-topic: profile.request
      reply-topic: billing.reply
      timeout-ms: 15000
      consumer-count: 4

  service:
    billing:
      request-topic: billing.request
      consumer-count: 4
      consumer:
        max.poll.records: "2000"
```

### 9.4 Рекомендации по переходу между окружениями

- Используйте одинаковые ключи `clients.<name>` и `service.<name>` во всех окружениях, меняйте только значения.
- Разделяйте topic names по окружениям (`*.dev`, `*.stage`) или отдельными кластерами Kafka.
- Используйте `service.<name>.group-id` для явного контроля `group.id` серверных сервисов.
- Для production проверяйте соответствие `consumer-count` и количества partitions до релиза.

## 10. Ролевые пресеты

### 10.1 `application-client-only.yml`

Используйте для сервисов, которые только вызывают RPC и не должны поднимать серверную часть.

```yaml
spring:
  application:
    name: billing-client

kafka-rpc:
  bootstrap-servers: localhost:9092
  server-enabled: false

  timeout-ms: 15000
  client-consumer-count: 2
  poll-interval-ms: 100
  stream-buffer-size: 2048
  stream-healthcheck-interval-ms: 5000
  stream-healthcheck-timeout-ms: 15000
  stream-server-idle-timeout-ms: 20000

  producer:
    acks: all
    compression.type: lz4
  consumer:
    group.id: billing-client-rpc
    max.poll.records: "500"

  clients:
    pricing:
      request-topic: pricing.request
      reply-topic: billing.reply
      timeout-ms: 10000
    profile:
      request-topic: profile.request
      reply-topic: billing.reply
```

### 10.2 `application-server-only.yml`

Используйте для сервисов, которые только обрабатывают входящие RPC и не выступают клиентом.

```yaml
spring:
  application:
    name: billing-server

kafka-rpc:
  bootstrap-servers: localhost:9092
  server-enabled: true

  server-consumer-count: 4
  poll-interval-ms: 100

  producer:
    acks: all
    compression.type: lz4
  consumer:
    auto.offset.reset: earliest
    max.poll.records: "1000"

  service:
    billing:
      request-topic: billing.request
      group-id: billing-server
      consumer-count: 4
      consumer:
        max.poll.records: "2000"
```

### 10.3 Когда какой профиль выбирать

- `client-only` — API gateway, orchestrator, BFF, integration adapter.
- `server-only` — domain worker, processor, internal service endpoint.
- mixed (`clients` + `service`) — когда один сервис и вызывает другие RPC, и сам обслуживает входящие RPC.

