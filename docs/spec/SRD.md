# SRD_Pizza Factory Simulator

## 0. 修訂歷史

| 版本 | 日期 | 修訂者 | 說明|
| --- | -------- | ---------- | ---- |
| 1.0 | 2026-10-15 | Ting Lee | Init |
| 1.1 | 2026-10-16 | Ting Lee | 新增 lot 單站狀態機與 op_dispatched (SYS-16, SYS-17) |
| 1.2 | 2026-10-19 | Ting Lee | 新增 stocker 與 BANK 狀態、WIP 定義 (SYS-18) |
| 1.3 | 2026-10-19 | Ting Lee | Phase 6 對齊:站點不 reject、改回報 op_failed,由排程器擁有 rework;stocker 觸發改為排程器 backlog 超過 STATION_QUEUE_LIMIT;更正 delivery limit 語意 (SYS-04, SYS-16, SYS-18, NFR-08) |
| 1.4 | 2026-10-20 | Ting Lee | 新增 recipe 變更事實與 run 開始快照,供 replay 重建生效表 (SYS-19) |
| 1.5 | 2026-10-20 | Ting Lee | SYS-01 補註 recorder 為重播工具;SYS-03 註明 wip.snapshots 尚無 producer |
| 1.6 | 2026-10-20 | Ting Lee | Phase 06.1:加工中 Down 於修復後判定失敗(不再續跑),`op_failed` 增選用欄位 `cause`,rework 表共用於品質與機台故障失敗 (SYS-04, SYS-16, NFR-08) |
| 1.7 | 2026-10-20 | Ting Lee | 06.2 rework credit release:op_failed 即歸還 credit,rework 改走 `scheduler.rework.<station>.hold`,rework 冷卻為 WAIT,`op_dispatched` 每 visit 於真實派工發出一次 (SYS-04, SYS-06, SYS-16, SYS-17) |
| 1.8 | 2026-10-29 | Ting Lee | 補充規格缺漏(quick 261029-keo):SYS-03 新增 `sim.timescale`;新增 SYS-20(時間縮放)、SYS-21(暫停機制)、SYS-22(主機睡眠偵測)、SYS-23(延後手動 hold) |

> 系統設計(SYS)與非功能門檻(NFR)。訊息結構的唯一權威是 `src/pizza/common/envelope.py` 的 Pydantic 模型:
> 本文件不複製欄位表,若與程式碼衝突,以程式碼為準。拓撲的唯一權威是 `src/pizza/common/topology.py`。
> 狀態標示「已實作」者為 Phase 1 現況;其餘為設計目標。每條 SYS/NFR 以 Refines 欄連回 FR。
> 儀表板的 REST 與 SSE 端點契約見 `docs/spec/API-SPEC.md`(本文件不重述端點細節)。

## 1. 系統設計 (SYS)

| ID | 描述 | Refines |
| --- | --- | --- |
| **SYS-01** | 元件布局:七個站點服務共用同一映像,以 `STATION_NAME` 選擇行為(spin、sauce、cheese、toppings、bake、slice、package,站名清單在 `constants.py`);另有 scheduler、order generator、kpi-service、dashboard(FastAPI)服務。recorder(`src/pizza/recorder/`)是重播工具,以命令列執行,非常駐服務,不在 Compose 中。各服務皆為單一 asyncio event loop。 | FR-STN-01 |
| **SYS-02** | 訊息角色規則:RabbitMQ 傳指令、Kafka 傳事實。規則本文與範例在 `docs/messaging-roles.md`(不在此複製)。已實作。 | FR-INFRA-07 |
| **SYS-03** | Kafka topics(replication factor 1):`station.events`(key=`machine_id`,3 partitions)、`order.lifecycle`(key=`order_id`,3 partitions)、`wip.snapshots`(compact;目前僅宣告 topic,尚無 producer)、`kpi.snapshots`(compact)、`sim.timescale`(delete,1 partition,保留 30 天;倍速歷史,見 SYS-20)。delete 型 topic 保留 30 天。權威為 `topology.py`。已實作。`station.events` 另含排程器發佈的 `op_dispatched`(見 SYS-17),key 仍為目標站 `machine_id`。 | FR-INFRA-03 |
| **SYS-04** | RabbitMQ 拓撲:exchanges `workorders`、`dispatch`、`dispatch.retry`、`scheduler.rework`、`dlx`、`control`;全部 queue 為 durable quorum queue。每站有 `station.<id>.jobs`、`station.<id>.retry`(TTL 5000 ms,到期 dead-letter 回 `dispatch`)、`station.<id>.control`;另有 `workorders.intake`、`dispatch.dlq`、`scheduler.control`、`generator.control`、`kpi.control`(僅承載 run.started,由 kpi-service 消費)。失敗與重試流程:站點從不 reject;處理失敗時先發 Kafka `op_failed` 再 ack(失敗來源有二:fail flag/poison 的品質失敗,以及站點在加工中 Down、修復後判定失敗的機台故障;payload 帶選用欄位 `cause` = `breakdown` 或 `quality`,缺省視為 `quality`,兩者走同一條 retry/DLQ 路徑),排程器在收到 `op_failed` 時即歸還該站 credit(06.2 D-01),並把 rework lot 放進排程器自有的 hold queue `scheduler.rework.<station>.hold`(quorum,TTL = `RETRY_TTL_MS` = 5000 ms,到期 dead-letter 到 exchange `scheduler.rework` 以 key `return` 進入 `scheduler.rework.return`,由排程器消費);排程器取回後讓 lot 以 `visit+1` 回到待辦清單(保留原進入時間排序),之後在真實派工時才發佈指令到站點 jobs queue。`dispatch.retry` 與 `station.<id>.retry` 維持不變,僅作為 jobs queue crash / delivery-limit 的路徑(Phase 6 D-15),rework 不再使用;rework 次數用盡則進 `dlx` 與 `dispatch.dlq`。(06.2 D-02/D-03 覆寫 Phase 6 原本由站點在 TTL 期間保有 credit 的設計。)jobs queue 未明確宣告 `x-delivery-limit` 參數,實際上限為 RabbitMQ 4.x 預設值 20(同一訊息投遞失敗 20 次後 dead-letter 到 `dispatch.retry`,僅可能由反覆 crash 或連線中斷觸發)。已實作(拓撲部分)。 | FR-FAIL-03 |
| **SYS-05** | `topology.py` 是 topic/exchange/queue/binding 的唯一宣告處,只含資料不做 I/O;`bootstrap.py` 是唯一套用拓撲的模組,從不刪除或清除資源,偏離時失敗並提示 `docker compose down -v`。已實作。 | FR-INFRA-04 |
| **SYS-06** | Credit 派工協定:排程器每站持有 1 個 credit;派出指令時消耗,收到該站 `op_completed` 或 `op_failed` 事實時歸還(失敗後無論 rework 或 scrap 皆歸還;覆寫 Phase 6 D-13 / 06.1 D-09,見 06.2 D-01)。排程器是唯一決定「下一個工作」的元件,站點之間不直接傳遞。 | FR-SCHED-01 |
| **SYS-07** | Ack 邊界:站點先以 Kafka producer flush 確認事件已送達,再對 RabbitMQ 訊息手動 ack;prefetch_count=1。重送會導致重複事件,因此消費端必須冪等(SYS-08)。 | FR-STN-04 |
| **SYS-08** | 冪等鍵:消費者以指令 `event_id` 識別重送,並以 `(order_id, step, visit)` 識別邏輯工作,防止同一工作被重複計入;合法再次加工以新 `event_id` 與遞增 `visit` 表示。`step` 為站名(station slug)。站點本身不去重,去重由下游消費者負責。 | FR-SCHED-03 |
| **SYS-09** | 時間規則:持續時間以 `time.monotonic()` 計算;事件時間戳為 UTC、毫秒精度(見 `envelope.py`);cycle time 由事件時間戳計算,不以消費者接收時間計算。 |  |
| **SYS-10** | 技術棧:Python 3.13、uv、asyncio、confluent-kafka 2.16(經薄 async 包裝)、aio-pika 10.1、Pydantic v2、orjson/JSON 線格式、FastAPI + SSE + 靜態 HTML/Chart.js(不用 Node build)、structlog、pydantic-settings、pytest。Kafka 為 `apache/kafka:4.2.2` KRaft 單節點;RabbitMQ 為 `rabbitmq:4.2-management-alpine`。已實作(基礎設施部分)。 |  |
| **SYS-11** | Compose 布局:`kafka`、`rabbitmq`、`kafka-ui`、`init`(一次性,執行 `python -m pizza.common.bootstrap`);Kafka 與 RabbitMQ 具 healthcheck,`init` 以 `depends_on: service_healthy` 等待;broker 資料使用具名 volume,`docker compose down -v` 為明確重置。已實作。 | FR-INFRA-02 |
| **SYS-12** | 訊息契約:所有訊息繼承 `Envelope`(六個必要欄位加判別欄位 `type`),以 `type` 為判別的 union 解析;`Command` 與 `Fact` 為角色標記;未知 `schema_version` 被拒絕。欄位與型別的權威為 `src/pizza/common/envelope.py`,本文件不重述。已實作。 | FR-INFRA-05 |
| **SYS-13** | 控制通道:儀表板對 scheduler、generator、各站的控制指令(開始/暫停/續跑/重置、到達率、派工規則、注入故障)經 RabbitMQ `control` topic exchange 的 `*.control` 佇列傳送。 | FR-DASH-05 |
| **SYS-14** | Run 生命週期:`run_id` 由「開始」動作產生(非服務啟動),同一 run 的所有訊息共用;重置或新 run 即換新 `run_id`。 | FR-KPI-05 |
| **SYS-15** | KPI 架構:kpi-service 為獨立 consumer group,內含純模組 `fold(state, event) -> state`;狀態只在記憶體,定期發佈快照到 `kpi.snapshots`;dashboard 消費快照並以 SSE 推給瀏覽器。 | FR-KPI-01 |
| **SYS-16** | Lot 單站狀態機:lot 指某訂單批量在某站的一次加工,以 `(order_id, step, visit)` 識別。狀態:WAIT(已可加工但在排程器待辦清單,尚未派工)、Track-In(派工指令已發佈並停在 `station.<id>.jobs`;lot 已進站,設備端等待加工;rework 冷卻中的 lot 不算 Track-In,而是 WAIT)、RUN(站點發出 `op_started`,開始加工)、Track-Out(站點發出 `op_completed`)。HOLD(MES)有兩種:站點 Down 時 RUN 中的 lot 暫停(狀態維持 RUN、指令維持 unacked),修復後(MTTR 自動修復或手動 Repair)該 job 不再繼續,而是判定失敗並發出 `op_failed(cause=breakdown)`,由排程器依 NFR-08 rework(`visit+1`)或 scrap;Down 時站點閒置(無 job)與站點 offline/crash 皆不判定(offline 由 RabbitMQ 以相同 `visit` 重送);rework 用盡(NFR-08)進 `dispatch.dlq` 並發出 `order.lifecycle failed` 的終止例外。轉移:WAIT 到 Track-In 於排程器發佈指令(發出 `op_dispatched`);Track-In 到 RUN 於 `op_started`;RUN 到 Track-Out 於 `op_completed`;站點處理失敗時發 `op_failed` 並 ack,站點 credit 隨即歸還,排程器將 lot 放入 `scheduler.rework.<station>.hold` 冷卻 TTL,期間狀態為 WAIT(非 Track-In),TTL 後以 `visit+1` 回到待辦清單並於真實派工時發出 `op_dispatched`(帶 `visit+1`);rework 用盡轉 HOLD(`dispatch.dlq`,`order.lifecycle failed`);站點在 ack 前被終止則 RUN 回到 Track-In(重送,第二次 `op_started` 帶 `attempt=2`,lot 不回 WAIT,不重發 `op_dispatched`)。 | FR-STN-07 |
| **SYS-17** | `op_dispatched` 契約:Kafka `station.events` 上的事實,由排程器發佈(排程器擁有待辦清單與派工決定,是唯一知道派工時刻與可加工時刻的元件;站點尚未看到指令,無法發出)。key 為目標站 `machine_id`(同 SYS-03),使同站的 `op_dispatched`、`op_started`、`op_completed` 落在同一 partition 並保持順序。於 RabbitMQ 派工指令的 publisher confirm 成功後才發佈,事實不描述未成功發佈的指令;消費者以 `(order_id, step, visit)` 去重(同 SYS-08)。語意上攜帶 `order_id`、`step`(站名)、`visit`、`machine_id` 與 `ready_ts`(lot 對該站變為可加工的時刻:第一站為工單釋放時刻,第 k 站(k>1)為前一站 `op_completed` 的 ts);欄位名稱與型別以 `envelope.py` 為準,本文件不複製欄位表。排隊時間公式:WAIT = `op_dispatched.ts` - `ready_ts`;Track-In = 該 `(order_id, step, visit)` 第一個 `op_started.ts` - `op_dispatched.ts`;站內佇列時間 = WAIT + Track-In;時間戳為事件時間戳(SYS-09),不是接收時間。不另設 `op_enqueued` 事件:WAIT 起點可由 `ready_ts` 推得,一個事件即足夠,且聚合器僅讀 `station.events` 即可計算,不必讀 `order.lifecycle`。`op_dispatched` 於每個 `(order_id, step, visit)` 在真實派工時恰好發出一次:crash 重送與 jobs queue retry 不重發;rework visit 於冷卻結束後真實派工時發出一次,其 `ready_ts` = 該失敗 `op_failed` 的 ts(06.2 D-10)。 | FR-SCHED-05 |
| **SYS-18** | Stocker 與 BANK 狀態:stocker 是排程器內的模組,專職儲存「當下不處理」的 lot,分類為 (a) 超過每站 `STATION_QUEUE_LIMIT` 的溢出 WAIT lot、(b) 被 down/offline 站點阻擋的 WAIT lot、(c) BANK lot,原因 `capacity|customer|schedule`(手動經 lot inspector 或 CLI;站點 down/offline 超過 `BANK_AFTER_S` 後自動;WAIT 超過 `BANK_WAIT_THRESHOLD` 時可選擇自動)。觸發條件為排程器 backlog 超過 `STATION_QUEUE_LIMIT`,而非 jobs queue 的 delivery limit:站點從不 reject,CR-03 的無止境迴圈由 rework 次數上限(NFR-08)與 stocker 共同處理。事實 `lot.stocked|lot.unstocked|lot.banked|lot.released` 發佈於 `order.lifecycle`。WIP = WAIT(含 stocker 內的 WAIT)+ RUN,不含 BANK 與 failed;cycle time 計入 `capacity` BANK 的停留時間,不計 `customer`/`schedule` 的停留時間;Little's Law 的 L = WIP + `capacity` BANK。BANK 屬 SYS-16 狀態機的擴充,不改既有轉移。此規則僅對 stocker 路徑覆寫 Phase 3 決定 D-11(buffer 無上限、無 WIP cap),其餘路徑 D-11 不變。物理屬性:stocker 充填 N2 保鮮,僅為文件說明,不影響任何 KPI。 | FR-FAIL-03 |
| **SYS-19** | 設定變更事實:recipe 秒數的即時修改由站點發佈 `recipe.changed` 到 `station.events`,key 為站名(與該站 `op_*` 同 partition);`run_id` 取站點當下的執行中 run,套用時間為事實自身的 `ts`,先發事實再改記憶體表,值未變則不發。每次 run 開始時排程器發佈 `recipe.snapshot`(一次含七站生效表,key = `scheduler`,與 `rule.changed` 同 stream)於 `core.start_run` 之前。兩者皆為既有 union 的附加成員,不新增 topic、不改 `schema_version`。重播時 KPI 重算以「該 run 最新的 snapshot + `ts <= 快照 ts` 的 `recipe.changed`」重建生效表,無 snapshot 的歷史 run 回退到磁碟值並在報告標示來源。訊息欄位以 `envelope.py` 為準,本文件不複製欄位表。 | FR-STN-09 |
| **SYS-20** | 時間縮放:所有持續時間以模擬秒定義,只有真實等待時間除以倍速。倍速變更以 `time_scale.changed` 發佈到 `sim.timescale`(key `line`),服務啟動時重播最後一筆並持續追蹤;切換時喚醒所有等待者,依切換前的牆鐘時間重算剩餘模擬時間。值域為 1、2、4、8。 | FR-DASH-07, FR-KPI-10 |
| **SYS-21** | 暫停機制:`run.pause`、`run.resume`、`run.reset` 為經 `control` exchange 送達的控制命令。生成器與站點以 `PauseGate` 凍結計時器,暫停期間保留剩餘時間;reset 時 `abort` 釋放所有等待者,避免工作卡住。排程器發佈 `run.paused`、`run.resumed` 事實。 | FR-DASH-08 |
| **SYS-22** | 主機睡眠偵測:生成器以 `HostSleepDetector` 比較相鄰樣本的牆鐘差與 monotonic 差,使用真實 monotonic 時鐘而非模擬時鐘,負向跳動忽略。偵測結果以 `host.sleep_detected`(欄位 slept_s)發佈。 | FR-DASH-09 |
| **SYS-23** | 延後手動 hold:排程器以 HoldRecord 保存請求。加工中或已派工的 lot 於請求時記錄 `lot.hold`(requested),於該站 op 結束(`op_completed` 或 `op_failed`)時套用為 `lot.banked` 或 failed 生命週期;冷卻中的 lot 立即套用;取消時發 `lot.hold`(cancelled)。 | FR-SCHED-10 |

## 2. 非功能需求 (NFR)

| ID | 描述 | Refines |
| --- | --- | --- |
| **NFR-01** | 啟動冪等:對已初始化的 broker 重跑 `init`,拓撲變更數為 0 且錯誤數為 0。 | FR-INFRA-03 |
| **NFR-02** | SSE 推送頻率為 1 至 2 Hz(每秒 1 到 2 個訊息);前端圖表每條序列最多保留 600 個資料點。 | FR-DASH-02 |
| **NFR-03** | KPI fold 單元測試不需任何 broker 即可執行,並能對固定事件固件重現 Little's Law 結果。 | FR-KPI-06 |
| **NFR-04** | Little's Law 容許誤差帶以設定參數 `LITTLE_TOLERANCE` 表示,預設 0.10(L 與 λW 相對誤差 10% 內為 pass);暖機以排除前 N 張訂單表示,參數為 `WARMUP_ORDERS`,預設 10。 | FR-KPI-04 |
| **NFR-05** | 重送容忍:同一 `event_id` 被投遞 2 次以上,加工結果在 KPI 與事件記錄中恰好出現 1 次。 | FR-SCHED-03 |
| **NFR-06** | 可重現性:相同 seed、參數下,產生器的工單序列與停機時程逐筆相同(誤差 0)。 | FR-ORDER-03 |
| **NFR-07** | 單一指令啟動:`docker compose up` 後,在無手動步驟下,所有 healthcheck 於 120 秒內 healthy。 | FR-INFRA-01 |
| **NFR-08** | 重試上限:失敗指令的 rework 次數為每站可設定表(預設 `spin` 2、`package` 2,其餘 0,以 `visit` 計數);品質失敗(fail flag/poison)與機台故障失敗共用同一張表,上限計的是 rework 次數(`visit <= 上限` 才 rework,否則 scrap;例如 `spin`=2:v1、v2 失敗各 rework 一次,v3 失敗即 scrap,上限 0 的站首次失敗即 scrap),用盡後必進 `dispatch.dlq`,不得無限重試。broker 層另有 RabbitMQ 4.x 預設 delivery limit 20 作為最後防線。 | FR-FAIL-03 |
| **NFR-09** | 乾淨結束:站點收到 SIGTERM 後於 10 秒內結束,且不留下未 ack 但已完成的工作。 | FR-STN-05 |
