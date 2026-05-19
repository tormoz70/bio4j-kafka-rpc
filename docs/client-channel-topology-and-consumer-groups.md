# Топология клиентских каналов и consumer group

Документ дополняет [application-yml-configuration.md](application-yml-configuration.md) и [pooled-kafka-rpc-request-timeline.md](pooled-kafka-rpc-request-timeline.md).

## Содержание

- [1. Партиции, каналы и consumer-count](#1-партиции-каналы-и-consumer-count)
- [2. Новая consumer group при каждом старте канала](#2-новая-consumer-group-при-каждом-старте-канала)

---

## 1. Партиции, каналы и consumer-count

### Как устроено в библиотеке

**Один `PooledKafkaRpcChannel`** = один `KafkaProducer` + `consumer-count` потоков consumer в **одной** consumer group (на инстанс канала).

Партиции reply-топика **делятся между потоками этого канала**, а не между разными каналами в пуле.

Сообщения идут с ключом `correlationId` → партиция = `hash(correlationId) % numPartitions`.

### Несколько каналов на один reply-топик

Если в одном JVM (или в кластере) несколько записей в `kafka-rpc.clients` указывают на **один** `reply-topic`:

- у каждого канала **своя** consumer group (`group.id` из конфига + суффикс `-inst-{uuid}`);
- **каждая** группа читает **все** партиции топика;
- ответ на запрос обрабатывает только канал, который держит `pending` для этого `correlationId`;
- остальные каналы получат то же сообщение как **orphan** (`ORPHAN_REPLY` в логах) — лишняя нагрузка на Kafka и CPU.

```
reply-topic (10 partitions)
    ├─ group channel-1-inst-uuid1  → consumer-count потоков
    ├─ group channel-2-inst-uuid2  → ...
    └─ ...
```

**Функционально корректно, но неэффективно.** Лучше: один pooled-канал на reply-топик или отдельный reply-топик на каждого клиента.

### 10 партиций и `consumer-count` на канал

Правило из конфигурации: **число партиций request/reply topics ≥ `consumer-count`**.

| `consumer-count` на канал | Когда имеет смысл |
|---------------------------|-------------------|
| **1** | Умеренный трафик, мало одновременных unary RPC, проще rebalance при старте |
| **2–4** | Много параллельных запросов на **этом** канале |
| **до 10** | Очень высокий QPS; **не больше числа партиций** |
| **> 10** | Лишние потоки без назначенных партиций |

Для **одного** канала с reply-топиком на 10 партиций:

- старт: `kafka-rpc.client-consumer-count: 1` (или per-client `consumer-count: 1`);
- при нехватке пропускной способности poll — 2, 4, максимум 10;
- **5 каналов × N consumer** = **5 независимых групп**, каждая читает весь топик → до `5 × N` consumer-потоков на один топик, обычно избыточно.

### Практические сценарии

| Сценарий | Партиции | Каналов | `consumer-count` на канал |
|----------|----------|---------|---------------------------|
| 5 RPC-клиентов, один reply-топик, обычный трафик | 10 | лучше свести к 1 каналу | **1** |
| 5 клиентов, общий топик, объединить нельзя | 10 | 5 | **1** у каждого (не поднимать без нужды) |
| 1 клиент, много параллельных RPC | 10 | 1 | **2–4**, при необходимости до **10** |
| 5 pods, один клиент на pod | 10 | 1 на pod | **1** на pod |

10 партиций для RPC обычно с запасом; для многих нагрузок достаточно 2–4 партиций.

### На что смотреть в проде

- Логи `ORPHAN_REPLY` — часто при нескольких каналах на один reply-топик.
- `REBALANCE` при старте — чаще при `consumer-count > 1`.
- Lag по consumer group (у клиента имя группы меняется при каждом новом инстансе канала, см. раздел 2).
- Пик одновременных in-flight unary на канал — если он невелик, `consumer-count: 10` почти не даст выигрыша.

---

## 2. Consumer group при старте канала

### Настройка (Spring)

| `channel-pool-idle-timeout-ms` | `channel-pool-mutate-consumer-group-on-channel-start` | Поведение |
|-------------------------------|--------------------------------------------------------|-----------|
| `0` (eviction выключен, по умолчанию) | `true` (по умолчанию) | `{group.id}-inst-{uuid}` на каждый новый канал — как раньше |
| `0` | `false` | Стабильный `group.id` из конфига клиента (без uuid) |
| `> 0` (eviction включён) | любое | **Всегда** мутировать (`-inst-{uuid}`), флаг игнорируется |

```yaml
kafka-rpc:
  channel-pool-idle-timeout-ms: 0
  # Стабильная группа для долгоживущего канала (метрики, меньше мусорных group в Kafka):
  channel-pool-mutate-consumer-group-on-channel-start: false
  clients:
    greeter:
      reply-topic: greeter.reply
      group-id: greeter-reply-group   # опционально; иначе {reply-topic}-group
```

Без Spring (builder): `PooledKafkaRpcChannel.builder().mutateConsumerGroupPerChannelStart(false)`.

### Режим с мутацией (`-inst-{uuid}`)

Фактический `group.id` consumer:

```
{group.id из конфига}-inst-{UUID}
```

Уникальная группа на инстанс канала, чтобы **замена канала не ждала**, пока брокер исключит предыдущего члена группы (`session.timeout.ms`, по умолчанию до 60 с). При `channel-pool-idle-timeout-ms > 0` этот режим **обязателен**.

Причины:

1. **Быстрая замена** при eviction, рестарте JVM, пересоздании канала в пуле.
2. **Reply consumer не читает историю** — после assign вызывается `seekToEnd`, нужны только ответы **после** готовности канала; старые offset не критичны.
3. **Изоляция** — новый процесс не делит rebalance с «призраком» старого consumer в той же группе.

Базовый `group.id` задаётся в YAML (`clients.<name>.group-id` или `{reply-topic}-group`), см. [application-yml-configuration.md §3](application-yml-configuration.md#3-как-формируется-groupid).

### Плюсы и минусы

| Плюс | Минус |
|------|--------|
| Быстрый старт канала после рестарта / eviction | В Kafka накапливаются **неактивные** группы `…-inst-*` |
| Нет гонки двух consumer в одной группе | Записи в `__consumer_offsets` до истечения retention |
| Согласовано с `seekToEnd` | При частых рестартах — лишние join/rebalance на брокере |
| | Метрики lag по **одному** стабильному имени группы неудобны |

Для RPC-клиента на reply-топике это **ожидаемое поведение**: ответы не теряются из‑за новой группы, если consumer успел получить непустой assign и выполнить `seekToEnd` (см. исправление пустого `onPartitionsAssigned` в runtime).

### Когда становится проблемой

- **Частое** пересоздание канала (агрессивный `channel-pool-idle-timeout-ms` + eviction).
- Лимиты на число consumer groups в managed Kafka.
- Нужен один стабильный `group.id` в дашбордах.

### Сравнение со стабильной группой (без `-inst-uuid`)

| Стабильный `group.id` | `-inst-{uuid}` (текущая реализация) |
|-----------------------|-------------------------------------|
| Меньше мусорных групп | Быстрый старт без ожидания session timeout |
| После краша новый consumer может ждать исключения старого | Каждый инстанс — «чистая» группа |
| Важнее для сервера и долгоживущих offset | Подходит для reply-only, `seekToEnd` |

У **сервера** (`KafkaRpcServer`) группа **без** uuid — там важны offset и горизонтальное масштабирование по партициям request-топика.

### Рекомендации

1. **Долгоживущий сервис без eviction** (`idle-timeout-ms: 0`): для стабильных метрик и меньшего числа групп в Kafka — `mutate-consumer-group-on-channel-start: false`.
2. **С eviction** (`idle-timeout-ms > 0`): мутация включена автоматически; не отключать.
3. **Один канал на reply-топик** в JVM, где возможно.
4. На брокере при тысячах `*-inst-*` — мониторинг и политика очистки неактивных consumer groups.
5. Не путать с таймаутом RPC (`timeout-ms`) и с eviction пула (`channel-pool-idle-timeout-ms`).

### Связанные файлы

- [`PooledKafkaRpcChannel.java`](../kafka-rpc-runtime/src/main/java/ru/sbrf/uamc/kafkarpc/PooledKafkaRpcChannel.java) — `channelConsumerGroupId`, `seekToEnd`, `runConsumerLoop`
- [`KafkaRpcChannelPool.java`](../kafka-rpc-spring-boot-starter/src/main/java/ru/sbrf/uamc/kafkarpc/spring/KafkaRpcChannelPool.java) — lazy-создание и idle eviction каналов
