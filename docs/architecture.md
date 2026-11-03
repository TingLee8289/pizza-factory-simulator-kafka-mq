# 架構與流程圖

> 規則:RabbitMQ 傳指令(command),Kafka 傳事實(fact)。詳見 [messaging-roles.md](messaging-roles.md)。
> 標示 P2~P7 的元件為後續 Phase 的目標,Phase 1 已實作的是 Compose 堆疊與 topology。

## 1a. 系統架構圖(總覽)

```mermaid
flowchart TB
    DASH[Browser Dashboard]
    API[FastAPI backend<br/>REST + SSE]
    GEN[Order Generator]
    SCH[Scheduler]
    LINE[7 Stations]
    KPI[KPI service]
    RMQ[(RabbitMQ<br/>COMMANDS)]
    KFK[(Kafka<br/>FACTS)]

    DASH -- REST --> API
    API -- SSE --> DASH
    API -- order / control --> RMQ
    GEN -- workorders --> RMQ
    RMQ -- workorders --> SCH
    SCH -- dispatch cmd --> RMQ
    RMQ -- station jobs --> LINE
    LINE -- op events --> KFK
    SCH -- order lifecycle --> KFK
    KFK -- station.events --> SCH
    KFK -- events --> KPI
    KPI -- kpi.snapshots --> KFK
    KFK -- events + kpi --> API
```

## 1b. 產線細節(7 站與 broker 資源)

```mermaid
flowchart TB
    SCH[Scheduler]

    subgraph RMQ[RabbitMQ - COMMANDS]
        JOBS[station.id.jobs x7]
        RETRY[station.id.retry x7<br/>TTL 5s]
        DLQ[dispatch.dlq]
    end

    subgraph Line[7 station containers - one image, STATION_NAME]
        direction TB
        S1[1 dough] --> S2[2 sauce] --> S3[3 cheese] --> S4[4 toppings]
        S4 --> S5[5 bake] --> S6[6 slice] --> S7[7 package]
    end

    subgraph KFK[Kafka - FACTS]
        SE[station.events x3]
        OL[order.lifecycle x3]
    end

    SCH -- dispatch cmd --> JOBS
    JOBS --> Line
    JOBS -. reject .-> RETRY
    RETRY -. TTL expire .-> JOBS
    JOBS -. over limit .-> DLQ
    Line -- op_started / op_completed / status --> SE
    SE --> SCH
    SCH -- created / released / completed --> OL
```

## 2. Docker Compose 部署圖(Phase 1 已實作)

```mermaid
flowchart TB
    subgraph Host[Windows host - 127.0.0.1 only]
        P1[:8080]
        P2[:15672]
        P3[:5672]
        P4[:9094]
    end

    subgraph Net[Docker compose network]
        UI[kafka-ui]
        K[kafka<br/>KRaft 4.2<br/>kafka:9092]
        R[rabbitmq<br/>4.2 management]
        INIT[init one-shot<br/>pizza.common.bootstrap<br/>exit 0]
        VK[(kafka-data)]
        VR[(rabbitmq-data)]
    end

    P1 --> UI
    P2 --> R
    P3 --> R
    P4 --> K
    UI --> K
    K --- VK
    R --- VR
    K -. service_healthy .-> INIT
    R -. service_healthy .-> INIT
    INIT -- create topics --> K
    INIT -- declare exchanges/queues --> R
```

## 3. RabbitMQ 重試 / DLQ 流程

```mermaid
flowchart TB
    SCH[Scheduler] -- routing key station.bake --> DP[dispatch exchange]
    DP --> JOBS[station.bake.jobs]
    JOBS --> ST[bake station]
    ST --> OK{success?}
    OK -- yes --> FACT[emit Kafka fact] --> ACK[ACK] --> DONE([done])
    OK -- no: reject --> DR[dispatch.retry exchange]
    DR --> RQ[station.bake.retry<br/>TTL 5s]
    RQ -- expire, dead-letter --> DP
    JOBS -. delivery count over limit .-> DLX[dlx exchange]
    ST -. invalid envelope: reject, no retry .-> DLX
    DLX --> DLQ[dispatch.dlq]
```

## 4. 單張訂單流程

```mermaid
flowchart TB
    A[Generator / Dashboard<br/>product, qty, due_ts] --> B[workorders.intake]
    B --> C[Scheduler]
    C -- lifecycle: created --> K[(Kafka order.lifecycle)]
    C --> D{credit available?}
    D -- no --> C
    D -- yes: released --> E

    subgraph LOOP[for each step S1 to S7]
        E[Scheduler sends cmd to station.step.jobs]
        E --> F[Station consumes<br/>prefetch 1, dedupe]
        F --> G[op_started to Kafka]
        G --> H[sleep cycle_time<br/>real time]
        H --> I[op_completed to Kafka + flush]
        I --> J[ACK]
        J --> L[Scheduler reads fact<br/>from station.events]
        L --> M{more steps?}
        M -- yes: scheduler releases next --> E
    end

    M -- no --> N[lifecycle: completed]
    N --> K
    K --> O[KPI / Dashboard]
```

## 5. Station「先 Kafka 後 ack」時序(Phase 2)

```mermaid
sequenceDiagram
    participant R as RabbitMQ
    participant S as Station
    participant K as Kafka

    R->>S: deliver job (prefetch 1)
    alt event_id already seen
        S->>R: ACK (skip duplicate)
    else new job
        S->>K: op_started
        Note over S: busy, sleep(cycle_time)
        S->>K: op_completed
        S->>K: flush()
        K-->>S: delivered
        S->>R: ACK
    end
    Note over R,S: crash before ACK -> redelivery -> dedupe records work once
```

## 6. KPI 資料流(Phase 4)

```mermaid
flowchart TB
    SE[(station.events)] --> KPI
    OL[(order.lifecycle)] --> KPI
    KPI[KPI service<br/>own consumer group<br/>fold state, event -> state]
    KPI --> T[throughput]
    KPI --> W[WIP]
    KPI --> C[cycle time<br/>mean / p50 / p95]
    KPI --> U[utilization]
    KPI --> B[bottleneck<br/>hysteresis]
    T & W & C --> LL[Little's Law<br/>L vs lambda*W<br/>warm-up excluded]
    LL --> KS[(kpi.snapshots)]
    U --> KS
    B --> KS
    KS --> SSE[SSE] --> DASH[Dashboard]
```

## 7. Phase 依賴

```mermaid
flowchart LR
    P1[P1 Infra and Contracts] --> P2[P2 One-station skeleton]
    P2 --> P3[P3 Full line + Scheduler + Orders]
    P3 --> P4[P4 KPI + Theory validation]
    P4 --> P5[P5 Dashboard]
    P5 --> P6[P6 Failures, Retry/DLQ, Chaos]
    P6 --> P7[P7 Dispatch rules + Comparison]
```
