# Pizza Factory Simulator

以童謠 "Pizza Party" 為靈感的披薩工廠即時模擬器。用真實的 **Kafka 事件流** 與 **RabbitMQ 指令佇列** 驅動一條七站產線,並透過 Web 儀表板觀察 throughput、WIP、cycle time、bottleneck 等 KPI。

產線順序:

```
spin → sauce → cheese → toppings → bake → slice → package
```

- **RabbitMQ**:傳遞 command(派工、重試、DLQ、控制訊息)
- **Kafka**:傳遞 fact(站點事件、訂單生命週期、KPI 快照)
- **Simulation**:wall-clock 即時模擬,不是模擬時鐘

## 需求

| 項目 | 說明 |
|---|---|
| Docker Desktop | 需啟用 WSL2 backend |
| Docker Compose v2 | `docker compose` 指令 |
| uv | 只有在主機端跑 CLI / 測試 / demo 腳本時需要 |
| Bash | 執行 `scripts/*.sh` 時需要(Git Bash 或 WSL) |

> Windows 主機請確認 Docker Desktop 已啟動,且連接埠 `8000`、`8080`、`9094`、`5672`、`15672` 沒有被占用。

## 快速開始

### 1. 啟動完整產線

```powershell
cd C:\project\pizza-factory-simulator-kafka-mq

# 第一次啟動,或修改過 src/ 之後要重建 image
docker compose build

# 啟動 Kafka、RabbitMQ、Kafka UI、init,以及整條產線
docker compose --profile line up -d --wait
```

`init` 是一次性服務,負責建立 Kafka topics 與 RabbitMQ exchanges/queues;跑完後顯示 `Exited (0)` 是正常的。

若 `--wait` 因為 `init` 結束而回傳非零,可用:

```powershell
docker compose --profile line up -d
docker compose ps
```

確認 `kafka`、`rabbitmq`、`dashboard` 都是 `healthy` 即可。

### 2. 開啟服務

| 服務 | 網址 | 帳密 |
|---|---|---|
| 產線儀表板 | http://127.0.0.1:8000 | - |
| Kafka UI | http://127.0.0.1:8080 | - |
| RabbitMQ Management | http://127.0.0.1:15672 | `pizza` / `pizza` |

所有連接埠都只綁定 `127.0.0.1`,不會對外開放。

### 3. 開始一次模擬

**方式 A:儀表板**

開啟 [http://127.0.0.1:8000](http://127.0.0.1:8000)，點 **Start run**，設定 seed、arrival rate 與產品組合後開始。

儀表板提供:

- 七站狀態、佇列長度、WIP、即時 KPI 圖表
- **+ Order** 手動下單、**Compare** 比較 FIFO / EDD / SPT 派工規則
- 站點詳細資訊、recipe 秒數即時修改
- 手動 breakdown / repair、order bank / release

**方式 B:CLI**

```powershell
uv run python -m pizza.tools.send_job start-run `
  --run-id demo-1 --seed 42 --arrival-rate 0.05 --max-orders 12
```

- `--arrival-rate` 單位是「張/秒」,`0.05` = 每 20 秒一張訂單
- 想看到瓶頸(bake)塞車,可調高到 `0.1`
- 同一個 `--seed` 會產生相同的訂單內容,方便重現

`uv run` 會自動讀取根目錄的 `.env`,主機端預設連線為:

```dotenv
KAFKA_BOOTSTRAP=127.0.0.1:9094
RABBITMQ_URL=amqp://pizza:pizza@127.0.0.1:5672/
```

若尚未建立 `.env`:

```powershell
Copy-Item .env.example .env
```

## 常用指令

| 目的 | 指令 |
|---|---|
| 只啟動基礎設施(Kafka / RabbitMQ / Kafka UI) | `docker compose up -d --wait` |
| 啟動完整產線 | `docker compose --profile line up -d --wait` |
| 查看服務狀態 | `docker compose ps` |
| 查看日誌 | `docker compose logs -f dashboard` |
| 查看產線日誌 | `docker compose --profile line logs -f station-bake scheduler` |
| 停止產線與容器(保留資料) | `docker compose --profile line down` |
| 停止並清空 Kafka / RabbitMQ 資料 | `docker compose --profile line down -v` |
| 重建 image | `docker compose build` |
| 只重啟 dashboard | `docker compose --profile line up -d dashboard` |

> `down -v` 是明確的重置手段。只有在 broker 資料損壞、佇列參數變更或想從乾淨狀態開始時才需要。

## 測試

整合測試需要 Kafka 與 RabbitMQ,但 **不能** 啟動 `line` profile,否則站點會搶走測試用的 job:

```powershell
docker compose up -d --wait
uv run pytest -q
```

只跑單元測試:

```powershell
uv run pytest tests/unit -q
```

只跑整合測試(需要 broker):

```powershell
uv run pytest -m integration -q
```

常用開發指令:

```powershell
uv sync --dev          # 安裝/更新主機端開發依賴
uv run ruff check .    # lint
uv run ruff format .   # format
```

## Demo 腳本

`scripts/` 下提供各階段的示範腳本,請用 Git Bash 或 WSL 執行。執行前先啟動完整產線。

| 腳本 | 內容 |
|---|---|
| `scripts/demo_phase2.sh` | 單一站點、單一 job、重複訊息去重、recorded-once |
| `scripts/demo_phase3.sh` | 七站全線、generator + scheduler、seed 重現 |
| `scripts/demo_phase4.sh` | 即時 KPI 對照 IE 理論值、bottleneck 與 Little's Law |
| `scripts/demo_phase6.sh` | breakdown、rework + DLQ、stocker / BANK、chaos kill |
| `scripts/demo_phase7.sh` | FIFO / EDD / SPT 派工規則切換、比較與 Kafka replay |

範例:

```bash
bash scripts/demo_phase7.sh
```

## 專案結構

```
src/pizza/
  common/      # 設定、envelope、Kafka/RabbitMQ I/O、topology、bootstrap
  station/     # 單一站點服務(STATION_NAME 決定站別)
  generator/   # 訂單產生器
  scheduler/   # 派工、重工、stocker、健康檢查、replay
  kpi/         # KPI 聚合、理論值、瓶頸、比較、recompute
  dashboard/   # FastAPI + SSE + 靜態前端
  tools/       # send_job 等 CLI
tests/
  unit/        # 純函式與模型測試
  integration/ # 需要 Docker broker 的端到端測試
deploy/        # RabbitMQ 等部署設定
scripts/       # demo 與維運腳本
docs/          # 架構、規格、訊息角色文件
```

## 疑難排解

**`init` 失敗,提示 topology 不一致**

通常是 broker volume 裡有舊的佇列參數。重置後重跑:

```powershell
docker compose --profile line down -v
docker compose --profile line up -d --wait
```

**修改程式碼後行為沒變**

所有服務共用 `pizza-app:dev` image,原始碼沒有 bind mount。請重建:

```powershell
docker compose build
docker compose --profile line up -d
```

**Dashboard 一直不 healthy**

```powershell
docker compose logs dashboard
docker compose logs init
```

確認 `init` 已成功結束、Kafka 與 RabbitMQ 都是 healthy。

**連接埠被占用**

調整 `docker-compose.yml` 的 `ports` 對應,或先停止占用該埠的程式。

**Windows 上無法執行 `.sh`**

請改用 Git Bash 或 WSL,不要直接用 PowerShell 執行。

## 相關文件

- [docs/architecture.md](docs/architecture.md) — 系統與 Compose 架構圖
- [docs/messaging-roles.md](docs/messaging-roles.md) — Kafka(fact)與 RabbitMQ(command)的分工
- [docs/spec/](docs/spec/) — PRD / FRD / SRD / 驗收條件
- [CLAUDE.md](CLAUDE.md) — 專案背景、技術棧與開發慣例
