# PRD_Pizza Factory Simulator

## 0. 修訂歷史


| 版本  |  日期     |  修訂者        | 說明   |
| --- | -------- | ---------- | ---- |
| 1.0 | 2026-10-15 | Ting Lee | Init |
| 1.1 | 2026-10-20 | Ting Lee | DASH-04 改為 broker 指標由外部管理介面提供(RabbitMQ 管理介面、Kafka UI),儀表板不內嵌面板(見 quick 261020-u55) |
| 1.2 | 2026-10-20 | Ting Lee | Core Value 不再要求瓶頸為儀表板獨立 KPI 卡片;瓶頸辨識維持由 KPI 快照提供(見 quick 261020-qmk) |
| 1.3 | 2026-10-20 | Ting Lee | ORDER-05 生命週期補 failed;補齊 STATION-06 與 Phase 8;§9、§10 狀態欄依 ROADMAP 對齊 |
| 1.4 | 2026-10-21 | Ting Lee | Phase 07.1:新增 ORDER-06、ORDER-07、KPI-08、DASH-05(下貨 ETA) |
| 1.5 | 2026-10-21 | Ting Lee | Phase 07.2:rush 改為數字優先權 1~99,新增 SCHED-05、SCHED-06 |
| 1.6 | 2026-10-29 | Ting Lee | 補充規格缺漏(quick 261029-keo):時間縮放(DASH-07、KPI-09)、暫停/續跑(DASH-08;DASH-03 的「停止」改為暫停/續跑)、主機睡眠自動暫停(DASH-09)、3D 產線與相機(DASH-10)、搜尋與事件 feed(DASH-11)、REST/SSE 契約(DASH-12)、自動單優先權(ORDER-08)、到達率上限與單位(ORDER-09)、配方 TOML(STATION-07)、lot 手動操作(SCHED-07)、比較回合與流程(COMP-05、COMP-06);§9 補 07.1、07.2、09;Q1 已決定 |


## 1. 背景與簡介

### 1.1 背景

開發者想同時練習四塊容易各學各的概念:Industrial Engineering (IE) 的產線指標、Production Control (PC) 的排程派工、  
Kafka 事件流、RabbitMQ 指令佇列。只讀文件或寫零散範例，很難看出它們在同一條產線上如何互相約束。  
因此需要一個能實際跑起來、可觀察、結果可對照理論值的環境。

### 1.2 產品簡介

一個以童謠 [[Pizza Party](https://www.youtube.com/watch?v=KP69YPqHquY)] 為靈感的披薩工廠即時模擬器,是個人學習專案。製程依序七站:  
spin the dough、spread the sauce、shred the cheese、spread the toppings、bake the pizza、slice the pizza、package。  
各站為獨立服務,以真實時間加工;RabbitMQ 傳遞「指令」,Kafka 記錄「事實」;使用者以 Web 儀表板觀察產線。

**Core Value:** 用真實的 Kafka + RabbitMQ 訊息流驅動一條可觀察的披薩產線，並且儀表板上的 KPI(throughput、WIP、cycle time)能和 IE 理論值對得上;瓶頸站由 KPI 快照辨識(KPI-02),不要求在儀表板上以獨立卡片呈現。

### 1.3 本文件的讀者

開發者本人(需求提出者、實作者與驗收者為同一人)。文件目的是讓日後的規劃、實作與驗收都有單一依據。

## 2. 目標與成功指標


| 編號  | 目標                  | 成功指標(可量測)                                                      | 驗證方式          |
| --- | ------------------- | -------------------------------------------------------------- | ------------- |
| G1  | 一條產線可用一個指令啟動並運作     | `docker compose up` 後 120 秒內所有 healthcheck 為 healthy(見 NFR-07) | 自動測試          |
| G2  | KPI 與 IE 理論值對得上     | Little's Law 的 L 與 λW 相對誤差在容許範圍內(預設 10%,見 NFR-04);瓶頸站與理論瓶頸一致   | KPI 對照 IE 理論值 |
| G3  | 訊息分工可被觀察與驗證         | RabbitMQ 重送不造成重複計數(見 NFR-05);失敗指令(品質失敗與加工中機台故障共用同一張表)rework 達每站上限必進 DLQ(見 NFR-08)         | 自動測試          |
| G4  | 派工規則可被比較            | 同一組固定 seed 的工單與停機排程,FIFO/EDD/SPT 的 KPI 可並排比較且可重現(見 NFR-06)     | 自動測試、手動       |
| G5  | KPI 邏輯可脫離 broker 驗證 | KPI fold 以錄製事件在無 broker 下重現結果(見 NFR-03)                        | 自動測試          |


非目標:效能壓測、高可用、多使用者、生產環境部署(詳見第 7 節範圍外)。

## 3. 使用者與使用情境

### 3.1 使用者角色


| 角色        | 說明                    | 主要需求                                 |
| --------- | --------------------- | ------------------------------------ |
| 操作技術員     | 由開發者本人扮演,透過 Web 儀表板操作 | 觀察產線、下工單、切換規則、注入故障                   |
| 開發者 / 驗證者 | 同一人,以學習與驗證為目的         | 檢查 KPI 是否對得上理論值、檢視 Kafka/RabbitMQ 行為 |


### 3.2 使用情境

1. **啟動與觀察產線**:執行 `docker compose up`,開啟儀表板,看七站狀態與佇列長度即時變化(INFRA-01、DASH-01)。
2. **下高優先權 lot**:技術員手動下單並選擇優先權(1~99,數字越小越緊急),觀察它如何在待辦中先於其他工單並影響其他工單,也可於下單後調整優先權(ORDER-03、SCHED-03、SCHED-05、SCHED-06)。
3. **驗證 KPI**:比對實測 throughput、cycle time 與瓶頸產能、Little's Law 檢查面板(KPI-03、KPI-04)。
4. **注入故障**:按下「注入故障」或強制終止容器,觀察停機、重送、重試與 DLQ(FAIL-03、FAIL-04)。
5. **比較派工規則**:以相同 seed 依序跑 FIFO/EDD/SPT,並排看 KPI 並匯出 CSV(COMP-01、COMP-02、COMP-04)。

## 4. 名詞定義


| 名詞                   | 定義                                                                            |
| -------------------- | ----------------------------------------------------------------------------- |
| WIP                  | Work In Process,在製品數量,已進入產線但尚未完成的工單或件數                                        |
| Throughput           | 單位時間完成的產出量                                                                    |
| Cycle time           | 一張工單從進入產線到完成所經過的時間(本專案顯示 mean/p50/p95)                                        |
| 瓶頸 (Bottleneck)      | 產能最低、限制整體產出的站點;本專案刻意把烘烤站設為最慢站                                                 |
| 臨界 WIP               | 達到瓶頸產能所需的最小 WIP,為理論參考值之一                                                      |
| Little's Law         | 平均 WIP = 到達率 × 平均 cycle time(L = λW),用來交叉檢查 KPI                               |
| 暖機 (Warm-up)         | 產線剛啟動、尚未達穩態的期間;以排除前 N 張訂單(`WARMUP_ORDERS`)處理,KPI 統計時需排除                                                   |
| 工單 (Order)           | 含品項、批量 `qty`、交期 `due_ts` 的生產需求                                                |
| Priority(優先權)    | 工單的整數優先權 1~99,數字越小越緊急,預設 50;與 due_ts 無關,僅決定排程器待辦的先後(不搶占加工中的 lot) |
| FIFO / EDD / SPT     | 派工規則:先進先出 / 最早交期優先 / 最短加工時間優先                                                 |
| Tardiness            | 完成時間晚於交期的時間量                                                                  |
| Makespan             | 一組工單從第一張開始到最後一張完成的總時間                                                         |
| Credit               | 排程器控制每站同時最多一張指令的許可機制                                                          |
| MTBF / MTTR          | 平均故障間隔時間 / 平均修復時間,用來模擬站點停機                                                    |
| DLQ                  | Dead Letter Queue,超過重試次數的指令所進入的佇列                                             |
| TTL 重試佇列             | 失敗指令先在此等待一段時間,到期後重新投遞                                                         |
| Envelope             | 所有訊息共用的外層格式(`event_id`、`run_id`、`line_id`、`machine_id`、`ts`、`schema_version`) |
| At-least-once / 冪等消費 | 訊息至少投遞一次(可能重複),消費端以 `event_id` 等鍵去重使結果相同                                      |
| Run                  | 一次模擬執行,以 `run_id` 識別,KPI 可依此重置或重算                                             |
| Seed                 | 隨機種子,固定後工單序列與停機時程可重現                                                          |


## 5. 功能清單與功能結構

### 5.1 功能結構

```
Pizza Factory Simulator
├─ 基礎設施 (INFRA)      一鍵啟動、拓撲建立、統一 envelope、分工規則
├─ 工單與品項 (ORDER)    工單模型、多品項、手動下單、自動產生、生命週期事件
├─ 站點 (STATION)        七個獨立站點、即時加工、狀態與事件、先 flush 再 ack、無限緩衝
├─ 排程派工 (SCHED)      credit 控制、排程器釋放、FIFO/EDD/SPT 切換、去重
├─ 故障與重試 (FAIL)     MTBF/MTTR、停機/恢復事件、TTL 重試與 DLQ、故障注入與 chaos
├─ KPI                   throughput/WIP/cycle time/使用率、瓶頸、理論值、Little's Law、暖機、交期指標
├─ 儀表板 (DASH)         產線圖、KPI 圖表、控制面板、broker 指標外部入口(RabbitMQ 管理介面、Kafka UI)
├─ 規則比較 (COMP)       同 seed 多規則並排、replay 重算、CSV 匯出
└─ v2 延後 (ADV / TOOL)  CONWIP、平行機台、換線、隨機分佈、OEE、Avro、Gantt
```

### 5.2 功能清單

優先級以學習價值與依賴順序判定:P0 為核心價值必要,P1 為完整體驗,P2 為延後(v2)。


| 功能模組  | 功能                      | 對應 REQ                          | Phase    | 優先級 |
| ----- | ----------------------- | ------------------------------- | -------- | --- |
| 基礎設施  | 一鍵啟動與冪等拓撲               | INFRA-01、INFRA-02               | 1        | P0  |
| 基礎設施  | 統一 envelope 與分工規則文件     | INFRA-03、INFRA-04               | 1        | P0  |
| 工單與品項 | 工單模型與多品項加工時間            | ORDER-01、ORDER-02               | 3        | P0  |
| 工單與品項 | 自動產生器(可固定 seed)         | ORDER-04                        | 3        | P0  |
| 工單與品項 | 工單生命週期事件                | ORDER-05                        | 3        | P1  |
| 工單與品項 | 儀表板手動下單                 | ORDER-03                        | 5        | P1  |
| 工單與品項 | 下貨 ETA 估算與 due_ts 預設帶入 | ORDER-06、ORDER-07               | 07.1     | P1  |
| 站點    | 七站獨立服務與事件發佈             | STATION-01、STATION-03           | 2        | P0  |
| 站點    | 先 flush Kafka 再 ack     | STATION-04                      | 2        | P0  |
| 站點    | 即時加工、烘烤為最慢站、無限緩衝        | STATION-02、STATION-05           | 3        | P0  |
| 站點    | recipe 變更事實與 run 開始快照          | STATION-06                       | 8        | P1  |
| 排程派工  | credit 排程器與逐站釋放         | SCHED-01、SCHED-02               | 3        | P0  |
| 排程派工  | 重送去重                    | SCHED-04                        | 2        | P0  |
| 排程派工  | 執行中切換 FIFO/EDD/SPT      | SCHED-03                        | 7        | P1  |
| 排程派工  | 優先權優先的待辦排序與下單後調整優先權 | SCHED-05、SCHED-06              | 07.2     | P1  |
| 故障與重試 | 隨機停機與恢復事件               | FAIL-01、FAIL-02                 | 6        | P1  |
| 故障與重試 | TTL 重試與 DLQ             | FAIL-03                         | 6        | P1  |
| 故障與重試 | 故障注入與 chaos 腳本          | FAIL-04                         | 6        | P1  |
| KPI   | 核心 KPI 與瓶頸辨識            | KPI-01、KPI-02                   | 4        | P0  |
| KPI   | 理論參考值與 Little's Law     | KPI-03、KPI-04                   | 4        | P0  |
| KPI   | 暖機排除、run 重置、純函式 fold    | KPI-05、KPI-06                   | 4        | P0  |
| KPI   | 交期指標                    | KPI-07                          | 7        | P1  |
| KPI   | ETA 誤差                   | KPI-08                          | 07.1     | P1  |
| 儀表板   | 產線圖與 KPI 圖表             | DASH-01、DASH-02                 | 5        | P0  |
| 儀表板   | 控制面板與 broker 指標外部入口  | DASH-03、DASH-04                 | 5        | P1  |
| 儀表板   | lot 視窗 ETA 顯示            | DASH-05                         | 07.1     | P1  |
| 規則比較  | 同 seed 多規則比較、replay、CSV | COMP-01、COMP-02、COMP-03、COMP-04 | 7        | P1  |
| v2 延後 | 進階 IE 概念與工具             | ADV-01 至 ADV-05、TOOL-01、TOOL-02 | deferred | P2  |


## 6. 限制條件

- 使用者:操作技術員(由開發者本人扮演),透過 Web 儀表板觀察產線並驗證 KPI,可下單時指定優先權(1~99,數字越小越緊急)並於下單後調整。
- 技術棧:Python + Apache Kafka + RabbitMQ(使用者指定,亦為學習目標)。
- 執行環境:本機 Docker Compose,無雲端成本。
- 模擬方式:即時(wall-clock)模擬,站點為獨立服務,以真實時間加工。
- 範圍:單一線性產線、固定加工時間(各品項各站不同,但無隨機分佈)。
- UI:Web 儀表板。
- 非功能需求:啟動冪等、SSE 推送頻率、重送容忍、可重現性、乾淨結束等門檻定義於 SRD.md 第 2 節(NFR-01 至 NFR-09)。

## 7. 範圍外 (Out of Scope)


| 項目                      | 原因                    |
| ----------------------- | --------------------- |
| 離散事件模擬時鐘                | 已選擇即時模擬;時間縮放因子可視需要加入  |
| 多條產線                    | 先單一產線,擴充列為後續里程碑       |
| ERP/MRP 整合              | 與學習目標無關               |
| 授權與多使用者                 | 純本機學習專案               |
| 預測性維護 / ML              | 超出學習範圍                |
| 最佳化求解器 (GA/MIP)         | 派工只做 FIFO/EDD/SPT 規則  |
| exactly-once / Kafka 交易 | 採 at-least-once 加冪等消費 |
| 雲端部署 / Kubernetes       | 本機 Docker Compose 即可  |


## 8. 假設、依賴與風險

### 8.1 假設

- 開發機為 Windows,透過 Docker Desktop(WSL2)執行 Docker Compose。
- 單機資源足以同時跑兩個 broker、七個站點、排程器、KPI 服務與儀表板。
- 單一使用者、單一產線、單一程序內的 KPI 狀態即可,不需水平擴充。
- 加工時間固定,即時模擬的時間誤差可忽略。

### 8.2 依賴


| 依賴                                           | 說明                    |
| -------------------------------------------- | --------------------- |
| Apache Kafka(KRaft、單節點)                      | 事實事件流;見 CLAUDE.md 技術棧 |
| RabbitMQ(quorum queue)                       | 指令佇列、重試與 DLQ          |
| Python 3.13、confluent-kafka、aio-pika、FastAPI | 全部服務與儀表板後端            |
| Docker Desktop / Docker Compose              | 本機執行環境                |


### 8.3 風險與緩解


| 風險                             | 影響               | 緩解                                                    |
| ------------------------------ | ---------------- | ----------------------------------------------------- |
| Kafka advertised listener 設定錯誤 | 容器或主機端連不上 broker | 內外兩組 listener、healthcheck、啟動測試(NFR-07)                |
| RabbitMQ 重送造成重複計數              | KPI 失真           | 以 `event_id` 與 `(order_id, step, visit)` 去重(SCHED-04、NFR-05) |
| 即時模擬測試耗時長                      | 驗證緩慢             | 加工時間與故障參數可由設定縮短;KPI fold 為純函式可離線測試(NFR-03)            |
| KPI 瓶頸辨識來回跳動                   | 儀表板不穩定           | 加入遲滯(KPI-02)                                          |
| quorum queue 參數變更需刪除重建         | 重新宣告失敗           | 單一 `topology.py` 定義;重置用 `docker compose down -v`      |
| 範圍膨脹到 v2 項目                    | 里程碑延宕            | 以第 7 節與 v2 延後清單守住範圍                                   |


## 9. Phase 對照


| Phase | 名稱                                        | 目標                                           | 狀態                 |
| ----- | ----------------------------------------- | -------------------------------------------- | ------------------ |
| 1     | Infra and Contracts                       | 一個指令起兩個 broker 與全部 topic/queue,並有統一訊息契約與角色規則 | 已完成(2026-10-08) |
| 2     | One-Station Walking Skeleton              | 單站證明即時加工、先 Kafka 後 ack、重複容忍                  | 已完成(2026-10-16) |
| 3     | Full Line, Scheduler and Order Generation | 七站、credit 排程器、seed 化多品項工單                    | 已完成(2026-10-16) |
| 4     | KPI Aggregator and Theory Validation      | 純函式 KPI、瓶頸、理論參考值、Little's Law                | 進行中 |
| 5     | Dashboard                                 | 產線圖、KPI 圖表、手動下單與控制、broker 指標外部入口          | 已完成(2026-10-19) |
| 6     | Failures, Retry/DLQ and Chaos             | MTBF/MTTR、TTL 重試、DLQ、故障注入與 chaos             | 進行中 |
| 7     | Dispatch Rules and Comparison             | EDD/SPT、交期指標、規則並排比較、replay、CSV               | 進行中 |
| 8     | Recipe Change Events on Kafka             | recipe 變更事實與 run 開始快照,供 replay 重建生效表                  | 已完成(2026-10-20)  |
| 07.1  | 下貨 ETA 與 due_ts 預設 | 下貨時估算 ETA 並帶入 due_ts,lot 視窗顯示 ETA 與完成後誤差 | 進行中 |
| 07.2  | 數字優先權 | 優先權 1~99、待辦依優先權排序、下單後可調整 | 進行中 |
| 09    | Portfolio Video Showcase | 四支示範影片與 Playwright 錄製工具 | 待開始(0/6) |


最新狀態以 `.planning/ROADMAP.md` 為準。

## 10. 需求目錄 (REQ)

每條需求格式:`- **ID** — 描述`,其後 `Phase:` 與 `狀態:`。v2 需求標為 `Phase: deferred`,不拆 FR。
REQ 對應 FR 與驗收案例,請見 FRD.md 與 AT.md 的追溯矩陣。

### 10.1 基礎設施 (INFRA)

- **INFRA-01** — 一個指令(`docker compose up`)啟動 Kafka、RabbitMQ 與所有服務
  Phase: 1
  狀態: 已實作
- **INFRA-02** — 啟動時冪等地建立所有 Kafka topic 與 RabbitMQ exchange/queue(單一 `topology.py` 定義)
  Phase: 1
  狀態: 已實作
- **INFRA-03** — 所有訊息使用統一 envelope(`event_id`、`run_id`、`line_id`、`machine_id`、`ts`、`schema_version`)
  Phase: 1
  狀態: 已實作
- **INFRA-04** — 有一份文件明確寫出「RabbitMQ 傳指令、Kafka 傳事實」的分工規則
  Phase: 1
  狀態: 已實作

### 10.2 工單與品項 (ORDER)

- **ORDER-01** — 工單含品項、批量 `qty`、交期 `due_ts`
  Phase: 3
  狀態: 已實作
- **ORDER-02** — 支援多種披薩品項,各品項在各站有不同單位加工時間(無換線時間)
  Phase: 3
  狀態: 已實作
- **ORDER-03** — 可從儀表板手動下單
  Phase: 5
  狀態: 已實作
- **ORDER-04** — 自動產生器依可設定、可固定 seed 的到達率產生工單
  Phase: 3
  狀態: 已實作
- **ORDER-05** — 工單生命週期(已建立、已釋放、加工中、完成、失敗)發佈到 Kafka `order.lifecycle`
  Phase: 3
  狀態: 已實作
- **ORDER-06** — 下貨當下以瓶頸+WIP 排隊逐站估算 ETA 並凍結為 Kafka fact
  Phase: 07.1
  狀態: 進行中
- **ORDER-07** — due_ts 預設由 ETA x buffer_k 帶入,手動單可預填與覆寫,generator 自動單同
  Phase: 07.1
  狀態: 進行中

- **ORDER-08** — 自動產生器依 `priority_high_ratio`(預設 0.10)以獨立 seed 串流分配高緊急優先權 `priority_high_value`(預設 10),其餘為預設 50
  Phase: 07.2
  狀態: 已實作
- **ORDER-09** — 後端到達率以 lots/min 送入,上限 12.0;儀表板以 pizzas/min(整數 1–12)呈現,送出前除以平均每張 lot 的數量 2 換算為 lots/min
  Phase: 5
  狀態: 已實作

### 10.3 站點 (STATION)

- **STATION-01** — 七站各為獨立服務,同一份程式映像,以環境變數指定站名
  Phase: 2
  狀態: 已實作
- **STATION-02** — 每站以真實時間加工,烘烤站刻意設為最慢站
  Phase: 3
  狀態: 已實作
- **STATION-03** — 站點狀態(idle/busy/down)與開始/完成事件發佈到 Kafka `station.events`
  Phase: 2
  狀態: 已實作
- **STATION-04** — 站點先 flush Kafka 事件再 ack RabbitMQ 訊息,使用 prefetch 1 與手動 ack
  Phase: 2
  狀態: 已實作
- **STATION-05** — 站間為無限緩衝
  Phase: 3
  狀態: 已實作
- **STATION-06** — 修改 recipe 秒數時發佈 Kafka fact(站點、品項、舊值、新值、時間),並在每次 run 開始時發佈生效 recipe 表快照,使 replay 能重建當時的設定
  Phase: 8
  狀態: 已實作

- **STATION-07** — 七站配方以 TOML 檔(`src/pizza/recipes/*.toml`)定義各品項的單位加工秒數
  Phase: 3
  狀態: 已實作

### 10.4 排程派工 (SCHED)

- **SCHED-01** — 排程器持有各站待辦,以 credit 控制每站同時最多一張指令
  Phase: 3
  狀態: 已實作
- **SCHED-02** — 站點之間不直接傳遞,完成後由排程器依規則釋放下一站指令
  Phase: 3
  狀態: 已實作
- **SCHED-03** — 派工規則可在執行中切換 FIFO / EDD / SPT
  Phase: 7
  狀態: 進行中
- **SCHED-04** — 消費者以 `event_id` 與 `(order_id, step, visit)` 去重,容忍 RabbitMQ 重送
  Phase: 2
  狀態: 已實作
- **SCHED-05** — 排程器待辦以優先權(1~99,數字越小越優先)為第一排序鍵,三種派工規則皆適用,不搶占加工中的 lot
  Phase: 07.2
  狀態: 進行中
- **SCHED-06** — 工單優先權可於下單後調整,以可重播的 `priority.changed` 事實記錄;舊事件的 rush 重播為 10、normal 重播為 50
  Phase: 07.2
  狀態: 進行中

- **SCHED-07** — 手動 lot 操作:報廢 WAIT/BANK lot、放行 BANK lot 回到待辦、對加工中或已派工 lot 下延後的 bank/scrap 請求(該站完工後生效)並可取消
  Phase: 6
  狀態: 已實作

### 10.5 故障與重試 (FAIL)

- **FAIL-01** — 站點依可設定、可固定 seed 的 MTBF/MTTR 隨機停機並修復(進行中的工單暫停,修復後判定失敗,依 rework 上限 rework 或 scrap)
  Phase: 6
  狀態: 進行中
- **FAIL-02** — 站點停機/恢復事件發佈到 Kafka,排程器據此暫停對該站派工
  Phase: 6
  狀態: 進行中
- **FAIL-03** — 失敗的指令經 TTL 重試佇列,超過次數進入 DLQ
  Phase: 6
  狀態: 進行中
- **FAIL-04** — 提供儀表板「注入故障」按鈕與強制終止容器(chaos)的腳本,並驗證 RabbitMQ 重送
  Phase: 6
  狀態: 進行中

### 10.6 KPI

- **KPI-01** — 即時計算 throughput、WIP、cycle time(mean/p50/p95)、各站使用率
  Phase: 4
  狀態: 進行中
- **KPI-02** — 辨識瓶頸站(含遲滯,避免來回跳動)
  Phase: 4
  狀態: 進行中
- **KPI-03** — 顯示理論參考值(瓶頸產能、原始加工時間、臨界 WIP)與實測值並排
  Phase: 4
  狀態: 進行中
- **KPI-04** — Little's Law 檢查面板(L 對照 λW,以容許誤差帶判定)
  Phase: 4
  狀態: 進行中
- **KPI-05** — 支援暖機排除與以 `run_id` 重置
  Phase: 4
  狀態: 進行中
- **KPI-06** — KPI 計算為純函式 fold,可用錄製事件在無 broker 情況下單元測試
  Phase: 4
  狀態: 進行中
- **KPI-07** — 交期指標(延遲張數、延遲比例、tardiness)
  Phase: 7
  狀態: 進行中
- **KPI-08** — ETA 誤差為純函式(完成時間減初始 ETA)
  Phase: 07.1
  狀態: 進行中

- **KPI-09** — 以倍速歷史將牆鐘區間換算為模擬秒,使 cycle time 等時間型指標不受倍速切換影響
  Phase: 09
  狀態: 已實作

### 10.7 儀表板 (DASH)

- **DASH-01** — 即時顯示七站產線圖、各站狀態與佇列長度
  Phase: 5
  狀態: 已實作
- **DASH-02** — 顯示 KPI 圖表與數值(SSE 推送,1-2 Hz)
  Phase: 5
  狀態: 已實作
- **DASH-03** — 儀表板可手動下單、設定到達率、切換派工規則、開始/暫停/續跑/重置
  Phase: 5
  狀態: 已實作
- **DASH-04** — broker 指標由外部管理介面提供:RabbitMQ 管理介面(:15672)檢視佇列深度與 DLQ 數量,Kafka UI 檢視 consumer lag;儀表板不內嵌此面板
  Phase: 5
  狀態: 已實作
- **DASH-05** — lot 視窗顯示 Initial ETA / Current estimate / Remaining / ETA error 與 stepper 各站預估
  Phase: 07.1
  狀態: 進行中

- **DASH-07** — 儀表板可切換模擬倍速 1x/2x/4x/8x,任何 run 狀態皆可
  Phase: 09
  狀態: 已實作
- **DASH-08** — run 執行中可暫停與續跑;暫停取代原 DASH-03 的「停止」
  Phase: 5
  狀態: 已實作
- **DASH-09** — 主機睡眠後(牆鐘跳動超過門檻)自動暫停產線並顯示橫幅
  Phase: 5
  狀態: 已實作
- **DASH-10** — 以 3D 數位雙生呈現產線,支援平移、縮放、聚焦與跟隨
  Phase: 5
  狀態: 已實作
- **DASH-11** — 搜尋站名或工單號定位,並以事件 feed 顯示最近站點事件
  Phase: 5
  狀態: 已實作
- **DASH-12** — 儀表板 REST 與 SSE 端點契約由 `docs/spec/API-SPEC.md` 定義
  Phase: 5
  狀態: 已實作

### 10.8 規則比較 (COMP)

- **COMP-01** — 以同一組固定 seed 的工單與停機排程,依序跑 FIFO/EDD/SPT
  Phase: 7
  狀態: 進行中
- **COMP-02** — 並排比較各規則的 KPI(平均 cycle time、tardiness、延遲比例、makespan)
  Phase: 7
  狀態: 進行中
- **COMP-03** — 可用 Kafka replay(新 consumer group、earliest)重算歷史 run 的 KPI
  Phase: 7
  狀態: 進行中
- **COMP-04** — 匯出比較結果為 CSV
  Phase: 7
  狀態: 進行中

- **COMP-05** — 比較每回合結束時發佈回合結果事實 `comparison.result`,由 `kpi.close_round` 命令觸發
  Phase: 7
  狀態: 已實作
- **COMP-06** — 比較流程以 start、next、end、cancel 控制三回合
  Phase: 7
  狀態: 已實作

### 10.9 v2 延後需求 (ADV / TOOL)

- **ADV-01** — CONWIP / kanban 與有限緩衝,TH-vs-WIP 曲線
  Phase: deferred
  狀態: 延後
- **ADV-02** — 平行機台(例如多台烤爐)
  Phase: deferred
  狀態: 延後
- **ADV-03** — 換線時間(setup/changeover)
  Phase: deferred
  狀態: 延後
- **ADV-04** — 加工時間隨機分佈
  Phase: deferred
  狀態: 延後
- **ADV-05** — 良率損失與完整 OEE
  Phase: deferred
  狀態: 延後
- **TOOL-01** — Avro / Schema Registry
  Phase: deferred
  狀態: 延後
- **TOOL-02** — Gantt 時間軸
  Phase: deferred
  狀態: 延後

## 11. 開放問題


| 編號  | 問題                          | 影響範圍             | 狀態              |
| --- | --------------------------- | ---------------- | --------------- |
| Q1  | 時間縮放因子於 v1 加入,1x/2x/4x/8x(見 DASH-07、SYS-20) | 測試、儀表板           | 已決定             |
| Q2  | 各品項在各站的單位加工時間具體數值           | ORDER-02、KPI 理論值 | 待 Phase 3 規劃時決定 |
| Q3  | 各 KPI 的容許誤差帶與暖機訂單數 `WARMUP_ORDERS` 的預設值是否足夠   | KPI-04、KPI-05    | 沿用 SRD 預設,實測後調整 |


## 12. 參考文件

- `docs/spec/FRD.md`:功能需求(FR)
- `docs/spec/SRD.md`:系統設計(SYS)與非功能需求(NFR)
- `docs/spec/AT.md`:驗收測試與追溯矩陣
- `.planning/PROJECT.md`、`.planning/REQUIREMENTS.md`:歷史規劃輸入
- `.planning/ROADMAP.md`:Phase 規劃與最新狀態
- `docs/architecture.html`、`docs/kafka-architecture.html`、`docs/rabbitmq-architecture.html`:架構圖
- `CLAUDE.md`:技術棧與限制
