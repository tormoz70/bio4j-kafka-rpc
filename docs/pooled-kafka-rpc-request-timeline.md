# Ось времени: `PooledKafkaRpcChannel` — `build()` → пауза → `request()` → ответ

Consumer на **reply**-топик **не** стартует вместе с `request()`. Он поднимается при создании канала и между вызовами `request()` продолжает `poll()` в фоне.

## Диаграмма последовательности по фазам

```mermaid
sequenceDiagram
    autonumber

    participant App
    participant Channel as PooledKafkaRpcChannel
    participant PoolThread as kafka-rpc-pool thread
    participant Producer
    participant ReqTopic as requestTopic
    participant Server as KafkaRpcServer
    participant ReplyTopic as replyTopic

    rect rgb(240, 248, 255)
        Note over App,ReplyTopic: Фаза A — создание канала (один раз)
        App->>Channel: build() / getOrCreate()
        Channel->>Producer: new KafkaProducer
        Channel->>PoolThread: start runConsumerLoop()
        PoolThread->>ReplyTopic: subscribe + seekToEnd
        PoolThread->>PoolThread: poll() цикл (пусто / ждёт)
        Channel->>Channel: await consumerReadyLatch
        Channel-->>App: канал готов
    end

    rect rgb(255, 250, 240)
        Note over App,ReplyTopic: Фаза B — приложение живёт (минуты, часы…)
        Note over PoolThread: poll() продолжается в фоне
        Note over App: request() ещё не вызывали
    end

    rect rgb(240, 255, 240)
        Note over App,ReplyTopic: Фаза C — один RPC-вызов
        App->>Channel: request(correlationId, body)
        Channel->>Channel: pending.put(correlationId, future)
        Channel->>Producer: send → requestTopic
        Producer->>ReqTopic: сообщение
        Note over App: future.get() — поток App заблокирован

        ReqTopic->>Server: poll request
        Server->>Server: handler.handle()
        Server->>ReplyTopic: reply + correlation-id

        PoolThread->>ReplyTopic: poll (уже был подписан)
        ReplyTopic-->>PoolThread: ответ
        PoolThread->>Channel: pending.remove + future.complete()
        Channel-->>App: byte[] response
        Channel->>Channel: finally: pending.remove
    end

    rect rgb(255, 250, 240)
        Note over App,ReplyTopic: Фаза D — снова пауза
        Note over PoolThread: poll() снова крутится, ждёт следующий request
    end
```

## Линейная шкала времени

```
время ──────────────────────────────────────────────────────────────►

│◄── build() ──►│◄────────── пауза (нет request) ──────────►│◄─ request() ─►│◄─ пауза ─►
                │                                            │               │
  PoolThread:   [subscribe replyTopic][poll][poll][poll]...[poll][poll][poll][poll]...
  App:         [ждёт latch]          [работает]              [get блок]     [работает]
                                            ↑                  ↑       ↑
                                            │                  │       └─ future.complete → return
                                            │                  └─ producer.send → server → reply
                                            └─ consumer УЖЕ слушает replyTopic
```

## Два потока в фазе C (не два «запуска» consumer)

```
Поток App (Caller)              Поток kafka-rpc-pool-0
────────────────────            ─────────────────────────
request()
  pending.put
  producer.send ──────────────► Kafka requestTopic
  future.get()  ████████████   poll() … poll() …
  (блокировка)  ████████████       │
                ████████████       ▼
                ████████████   reply в replyTopic
                ████████████   future.complete()
  return bytes  ◄──────────────
  finally remove
```

## Итог

| Вопрос | Ответ |
|--------|--------|
| Consumer стартует при `request()`? | **Нет** |
| Когда стартует? | При `new` / `build()` канала (фаза A) |
| Что делает `request()` с consumer? | **Ничего** — только `pending` + `send` + `get` |
| Параллельно с send на сервер? | Consumer **уже** poll'ит reply-топик; send и poll — разные топики |

## Ссылки на код

- Запуск consumer-потоков: `PooledKafkaRpcChannel` конструктор, цикл `runConsumerLoop` — [`PooledKafkaRpcChannel.java`](../kafka-rpc-runtime/src/main/java/ru/sbrf/uamc/kafkarpc/PooledKafkaRpcChannel.java)
- Синхронный вызов: метод `request(String, byte[], Map)`
