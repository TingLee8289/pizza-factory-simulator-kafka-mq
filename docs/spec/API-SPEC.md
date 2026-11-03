# API-SPEC_Pizza Factory Simulator

> 狀態:v1.0(已審核)。
> 來源:`src/pizza/dashboard/routes.py`、`control.py`、`orders.py`、`comparison.py`、`common/constants.py`。
> 本文件描述儀表板對外的 REST 與 SSE 契約。若與程式碼衝突,以程式碼為準。
> 需求對應見 FRD.md(FR-DASH-*、FR-ORDER-*、FR-FAIL-*、FR-STN-*、FR-SCHED-*、FR-COMP-*)與 SRD.md。

## 0. 修訂歷史

| 版本 | 日期 | 修訂者 | 說明 |
| --- | -------- | ---------- | ---- |
| 1.0 | 2026-10-29 | Ting Lee | Init:由程式碼整理,經審核 |

## 1. 總則

| 項目 | 規則 | 來源 |
| --- | --- | --- |
| Base URL | `http://localhost:8000`(`DASHBOARD_PORT` 預設 8000) | `dashboard/settings.py` |
| 內容格式 | 所有 POST 必須 `Content-Type: application/json`,否則 415 | `SecurityMiddleware` |
| 同源 | POST 若帶 `Origin`,其 host 必須與 `Host` 相同,否則 403。不提供 CORS | `SecurityMiddleware` |
| 安全標頭 | 回應皆帶 CSP、`X-Content-Type-Options: nosniff`、`Referrer-Policy: no-referrer` | `SecurityMiddleware` |
| 錯誤格式 | `{"detail": <string or object>}` | `routes.py` 模組說明 |
| 內部錯誤 | 500 回 `{"detail": "internal error"}`,不回傳 broker 例外文字 | T-05-33 |
| 未就緒 | 503 `not ready`(runtime 尚未初始化) | `get_runtime` |

### 1.1 錯誤碼

| HTTP | 意義 | 常見來源 |
| --- | --- | --- |
| 403 | 跨來源 POST | `SecurityMiddleware` |
| 409 | 狀態機不允許的轉移 | `ConflictError`(如 pause 於 none 狀態) |
| 415 | POST 的 Content-Type 不是 JSON | `SecurityMiddleware` |
| 422 | 驗證失敗、`NotAvailableError`、不合法的 `due_ts`、不合法的 `run_id`/`comparison_id` | Pydantic / 自訂 |
| 404 | 未知工單(lot)或未知站名 | `UnknownLotError`、`UnknownStationError`、lot 查詢 |
| 502 | KPI 重算的 replay 失敗 | `recompute_run` |
| 503 | SSE 連線過多、runtime 未就緒、time scale 發佈失敗 | `routes.py` |
| 500 | 未預期錯誤 | `SecurityMiddleware` |

## 2. 端點總覽

| 分組 | 方法 | 路徑 | 用途 | 需求 ID |
| --- | --- | --- | --- | --- |
| 健康 | GET | `/healthz` | 服務存活 | INFRA-01 |
| 狀態 | GET | `/api/state` | 完整快照 | DASH-01、DASH-02 |
| 串流 | GET | `/api/stream/state` | SSE 狀態快照與 1 Hz tick | DASH-02、NFR-02 |
| 串流 | GET | `/api/stream/events` | SSE 逐筆事實 | DASH-02 |
| Run | POST | `/api/run/start` | 開始一次 run | DASH-03 |
| Run | POST | `/api/run/pause` | 暫停 run | DASH-03 |
| Run | POST | `/api/run/resume` | 續跑 run | DASH-03 |
| Run | POST | `/api/run/reset` | 重置 run(換新 `run_id`) | DASH-03 |
| 參數 | POST | `/api/rate` | 設定到達率 | DASH-03 |
| 參數 | POST | `/api/rule` | 切換派工規則 | DASH-03、FR-DASH-05 |
| 參數 | POST | `/api/time_scale` | 設定模擬倍速 | 待 FRD 補充 |
| 配方 | GET | `/api/recipes` | 七站生效配方表 | STATION-06 |
| 配方 | POST | `/api/station/{name}/recipe` | 即時修改配方秒數 | STATION-06 |
| 故障 | POST | `/api/station/{name}/break` | 注入故障 | FAIL-04 |
| 故障 | POST | `/api/station/{name}/repair` | 手動修復 | FAIL-04 |
| 故障 | GET | `/api/failure-config` | 故障與 rework 參數(唯讀) | FAIL-01、FAIL-03 |
| 工單 | POST | `/api/order` | 手動下單 | ORDER-03 |
| 工單 | GET | `/api/order/estimate` | 試算 ETA(不建單) | ORDER-06 |
| 工單 | GET | `/api/lot/{order_id}` | lot 詳細資料 | DASH-05 |
| 工單 | GET | `/api/lot/{order_id}/eta` | lot 的 ETA 與 current estimate | DASH-05 |
| Lot | POST | `/api/lot/{order_id}/bank` | 手動入庫(BANK) | SYS-18 |
| Lot | POST | `/api/lot/{order_id}/scrap` | 報廢 | FAIL-03 |
| Lot | POST | `/api/lot/{order_id}/release` | 從 stocker 放行 | 待 FRD 補充 |
| Lot | POST | `/api/lot/{order_id}/hold/cancel` | 取消待執行的 bank/scrap 請求 | 待 FRD 補充 |
| Lot | POST | `/api/lot/{order_id}/priority` | 調整優先權 | SCHED-06 |
| 比較 | POST | `/api/comparison/start` | 開始三規則比較 | COMP-01 |
| 比較 | POST | `/api/comparison/next` | 進入下一規則 | COMP-01 |
| 比較 | POST | `/api/comparison/end` | 結束目前回合 | 待 FRD 補充 |
| 比較 | POST | `/api/comparison/cancel` | 取消比較 | COMP-01 |
| 比較 | GET | `/api/comparison` | 比較狀態與結果 | COMP-02 |
| 比較 | GET | `/api/comparison.csv` | 匯出 CSV | COMP-04 |
| 重算 | GET | `/api/runs` | 列出可重算的歷史 run | COMP-03 |
| 重算 | POST | `/api/runs/{run_id}/recompute` | 以 Kafka replay 重算 KPI | COMP-03 |
| 靜態 | GET | `/` | 儀表板頁面(不列入本契約) | — |

## 3. 詳細定義

### 3.1 `POST /api/run/start`

- Request body(可省略,省略時全部取預設)

| 欄位 | 型別 | 必填 | 預設 | 限制 |
| --- | --- | --- | --- | --- |
| `seed` | int | 否 | null | 0 ≤ seed ≤ 2^31−1 |
| `arrival_rate_per_min` | float | 否 | 60/35 ≈ 1.714 | 0 < 值 ≤ 12.0 |
| `weights` | object<string, float> | 否 | null | 品項權重 |
| `random_breakdowns` | bool | 否 | false | 是否啟用隨機停機 |
| `k` | float | 否 | 2.0 | 1.0 ≤ k ≤ 10.0,due 的 buffer |
| `max_orders` | int | 否 | null | 1 ≤ 值 ≤ 10000 |
| `rule` | string | 否 | null | `fifo` / `edd` / `spt` |

- 額外欄位一律拒絕(`extra="forbid"`)。
- 回應:啟動後的狀態快照(含新 `run_id`)。
- 狀態機:`none → running`;在 running 或 paused 時呼叫回 409。

### 3.2 `POST /api/run/pause` / `resume` / `reset`

- 無 body。
- 狀態機:`running → paused`(pause)、`paused → running`(resume)、`running|paused → none`(reset,會清空佇列並換新 `run_id`)。
- 不合法轉移回 409。

### 3.3 `POST /api/rate`

- Body:`{"arrival_rate_per_min": float}`,0 < 值 ≤ 12.0。
- 回應:更新後的到達率。

### 3.4 `POST /api/rule`

- Body:`{"rule": "fifo" | "edd" | "spt"}`。
- 回應:`{"rule": <值>}`。

### 3.5 `POST /api/time_scale`

- Body:`{"scale": 1 | 2 | 4 | 8}`(僅允許這四個整數)。
- 任何 run 狀態皆可呼叫。
- 成功:回傳目前倍速;廣播 `time_scale.changed` 到 `sim.timescale`。
- 發佈失敗:503 `time scale publish failed`。
- 值域維持 1/2/4/8;若要開放其他值,需另行變更本節。

### 3.6 `POST /api/station/{name}/recipe`

- Path:`name` 為七站之一(`spin`、`sauce`、`cheese`、`toppings`、`bake`、`slice`、`package`)。未知站名回 404。
- Body:`{"product": "margherita" | "pepperoni" | "veggie", "unit_time_s": float}`,0 < 值 ≤ 3600。
- 未知品項回 422。
- 行為:站點發佈 `recipe.changed`(值未變則不發)。

### 3.7 `POST /api/station/{name}/break`

- Body:`{"duration_s": float | null}`,null 或 0 < 值 ≤ 3600。
- null 時使用抽樣 MTTR。
- 行為:站點立即進入 down,加工中的 lot 依 SYS-16 判定為 `op_failed(cause=breakdown)`。

### 3.8 `POST /api/station/{name}/repair`

- 無 body。
- 行為:手動修復 down 站點。

### 3.9 `POST /api/order`(手動下單)

- Body:

| 欄位 | 型別 | 必填 | 預設 | 限制 |
| --- | --- | --- | --- | --- |
| `product` | string | 是 | — | `margherita` / `pepperoni` / `veggie` |
| `qty` | int | 是 | — | 1 ≤ 值 ≤ 20 |
| `priority` | int(strict) | 否 | 50 | 1 ≤ 值 ≤ 99,數字越小越緊急 |
| `due_ts` | AwareDatetime | 否 | null | 必須含時區;null 時由 ETA × buffer 帶入 |
| `buffer_k` | float | 否 | 1.5 | 0.5 ≤ 值 ≤ 3,不接受 inf/nan |

- 回應:generator 回覆的最終工單(含 `order_id`,格式 `L-NNNN`)。
- 逾時:422(`NotAvailableError`)。
- 優先權與 `due_ts` 彼此獨立(見 FR-ORDER-05)。

### 3.10 `GET /api/order/estimate`(試算)

- Query 參數同 3.9 的 `product`、`qty`、`priority`、`buffer_k`。
- 不建立工單、不消耗序號(D-07)。
- 未知的 query 參數回 422。

### 3.11 `POST /api/lot/{order_id}/priority`

- Path:`order_id` 只允許 `[A-Za-z0-9_.-]`,長度 1–64。
- Body:`{"priority": int}`,1 ≤ 值 ≤ 99,必須為整數型別。
- 錯誤:未知工單 404;已終結或尚未釋放的工單 409;超出範圍 422。
- 值未變不發事實(SCHED-06)。

### 3.12 `POST /api/lot/{order_id}/bank`

- Body:`{"reason": "capacity" | "customer" | "schedule"}`。
- 行為:發佈 `lot.bank` 指令,由排程器處理並產生 `lot.banked`。

### 3.13 `POST /api/lot/{order_id}/scrap`

- Body:`{"reason": "operator" | "quality" | "material" | "other"}`。
- 行為:發佈 `lot.scrap`;經 `lot.hold` 狀態追蹤(requested → applied 或 cancelled)。

### 3.14 `POST /api/lot/{order_id}/release` 與 `/hold/cancel`

- 無 body。
- `release`:將 stocker 中的 lot 放行回待辦(`lot.release`,`by` 為 manual)。
- `hold/cancel`:取消尚未套用的 bank/scrap 請求(`lot.hold_cancel`)。

### 3.15 `GET /api/lot/{order_id}` 與 `/eta`

- `GET /api/lot/{order_id}`:未知工單回 404;回傳 lot 詳細(狀態、各站紀錄等)。
- `GET /api/lot/{order_id}/eta`:回傳 Initial ETA、Current estimate(儀表板輪詢排程器,不是 Kafka 事實)。
- **待確認**:兩支回應的完整 schema 尚未逐欄核對,本版不列欄位。

### 3.16 `POST /api/comparison/start`

- Body(可省略):

| 欄位 | 型別 | 預設 | 限制 |
| --- | --- | --- | --- |
| `n_orders` | int | 30 | 5 ≤ 值 ≤ 99 |
| `arrival_rate_per_min` | float | 2.0 | 0 < 值 ≤ 12.0 |
| `k` | float | 1.5 | ≥ 1.0 |
| `seed` | int | null | 0 ≤ 值 ≤ 2^31−1 |

- 回應:比較狀態(`/api/comparison` 同格式)。

### 3.17 `GET /api/comparison.csv`

- Query:`comparison_id`(可省略;格式 `^c-[0-9a-z-]{1,36}$`,不符回 422)。
- 回應:`text/csv`,每規則一列,含四個指標欄位(FIFO/EDD/SPT)。

### 3.18 `POST /api/runs/{run_id}/recompute`

- Path:`run_id` 必須符合 `RUN_ID_RE`,否則 422。
- 無已存快照時回 422(`NoStoredSnapshot`)。
- Replay 失敗回 502 `Replay failed`。

## 4. SSE 串流

| 端點 | 事件名 | 內容 | 頻率 |
| --- | --- | --- | --- |
| `/api/stream/state` | `snapshot`(連線時一次) | 完整快照,帶 `seq` | — |
| `/api/stream/state` | 狀態 tick | 增量狀態 | 1 Hz(NFR-02:1–2 Hz) |
| `/api/stream/events` | 由 Kafka 事實轉出,事件名即 `type` | 逐筆事實 | 依事實 |

- 重連提示 `retry: 2000` ms。
- 同時連線上限 20(超過回 503)。
- 每個訂閱者的佇列上限 64(超過即丟棄)。
- 前端圖表每條序列最多 600 點(`kpi_ring_size`)。

## 5. 決議與待確認事項

### 5.1 已決議

1. 倍速值域維持 1/2/4/8(§3.5)。
2. 422 語意維持共用,不拆分(§1.1)。
3. 本契約為獨立檔案 `API-SPEC.md`,不併入 SRD。
4. 文件中「待 FRD 補充」的端點尚無 FR 編號,編號待 FRD 補充時決定,本文件不自行編造。

### 5.2 待確認

1. `GET /api/lot/{order_id}` 與 `/eta` 的回應 schema(§3.15)。
