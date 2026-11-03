# AT_Pizza Factory Simulator

## 0. 修訂歷史


| 版本  | 日期         | 修訂者      | 說明   |
| --- | ---------- | -------- | ---- |
| 1.0 | 2026-10-15 | Ting Lee | Init |
| 1.1 | 2026-10-16 | Ting Lee | 新增 AT-44 至 AT-46 |
| 1.2 | 2026-10-20 | Ting Lee | 新增 AT-47(recipe 變更事實與 replay 重建) |
| 1.3 | 2026-10-20 | Ting Lee | AT-32 對齊 DASH-04 改版:改為核對 RabbitMQ 管理介面與 Kafka UI(見 quick 261020-u55) |
| 1.4 | 2026-10-20 | Ting Lee | AT-44 筆誤修正(`_dispatched.ts` → `op_dispatched.ts`) |
| 1.5 | 2026-10-20 | Ting Lee | Phase 06.1:AT-34/AT-36/AT-45 改為修復後判定失敗,新增 AT-48 |
| 1.6 | 2026-10-20 | Ting Lee | 06.2 rework credit release:AT-36、AT-45(b) 改為 hold queue 與 credit 歸還 |
| 1.7 | 2026-10-21 | Ting Lee | Phase 07.1:新增 AT-49 至 AT-52 |
| 1.8 | 2026-10-21 | Ting Lee | Phase 07.2:AT-28 改為優先權;新增 AT-53、AT-54 |
| 1.9 | 2026-10-29 | Ting Lee | 補充規格缺漏(quick 261029-keo):新增 AT-55 至 AT-72;AT-31 的「停止」改為暫停/續跑 |


> 每條 AT 驗證一個或多個 FR / NFR(`Verifies:`)。Method 只能是:自動測試、手動、KPI 對照 IE 理論值。
> Phase 1 的 AT 引用既有測試檔;其餘 Phase 為待實作的驗收條件。
> 檔尾的追溯矩陣只是索引表(僅含 ID),不參與定義與覆蓋檢查。

## 1. Phase 1(已實作)

- **AT-01** — 一鍵啟動三服務並 healthy


Verifies: FR-INFRA-01, NFR-07  
Phase: 1  
Method: 手動  
Given: 乾淨環境(`docker compose down -v` 之後)  
When: 執行 `docker compose up`  
Then: kafka、rabbitmq、kafka-ui 啟動且 healthy,`init` 正常結束,全程無手動步驟


- **AT-02** — init 等待 broker healthy 才執行


Verifies: FR-INFRA-02  
Phase: 1  
Method: 手動  
Given: `docker compose up` 進行中  
When: 觀察 `docker compose ps` 與 `init` 日誌  
Then: `init` 在 kafka 與 rabbitmq 皆 healthy 之後才執行


- **AT-03** — topic 與 queue 存在且 bootstrap 冪等

Verifies: FR-INFRA-03, NFR-01  
Phase: 1  
Method: 自動測試  
Given: 已啟動且已初始化的 broker  
When: 執行 `tests/integration/test_topology.py`(`test_kafka_topics_exist`、`test_rabbit_entities_exist`、`test_bootstrap_idempotent`)  
Then: 所有 topic 與 exchange/queue 存在,第二次 bootstrap 無變更且無錯誤


- **AT-04** — topology 宣告含 3-partition 與 quorum queue

Verifies: FR-INFRA-04  
Phase: 1  
Method: 自動測試  
Given: `topology.py` 的資料宣告  
When: 執行 `tests/unit/test_topology.py`(`test_kafka_topics`、`test_exchanges`、`test_queue_count_and_quorum`、`test_station_queues`、`test_singleton_queues`、`test_references_exist`、`test_frozen`)  
Then: 全部通過,3-partition `station.events` 與所有 quorum queue 皆在宣告中


- **AT-05** — 非法 envelope 解析失敗並拋 InvalidEnvelope

Verifies: FR-INFRA-05  
Phase: 1  
Method: 自動測試  
Given: 一則缺少必要欄位、無時區 `ts`、`schema_version` 為 2 或未知 `type` 的訊息  
When: 執行 `tests/unit/test_envelope.py`  
Then: 解析失敗並拋出 `InvalidEnvelope`;合法訊息 round trip 成功,`ts` 以 `Z` 結尾


- **AT-06** — 無效訊息被擋下或進 DLQ 而非丟失

Verifies: FR-INFRA-06  
Phase: 1  
Method: 自動測試  
Given: 無效指令、無效事實與垃圾位元組  
When: 執行 `tests/unit/test_io_guards.py` 與 `tests/integration/test_smoke.py`  
Then: Producer 拒絕發送且不送出任何東西;無效 RabbitMQ 訊息進入 `dispatch.dlq`;Kafka 無效訊息被略過並記錄


- **AT-07** — messaging-roles 文件要素齊全

Verifies: FR-INFRA-07  
Phase: 1  
Method: 自動測試  
Given: `docs/messaging-roles.md`  
When: 執行 `tests/unit/test_docs.py`  
Then: 規則句、mermaid 流程圖、指令與事實範例、名詞與清單皆存在


## 2. Phase 2

- **AT-08** — 站名環境變數選擇行為，無效站名啟動失敗

Verifies: FR-STN-01  
Phase: 2  
Method: 自動測試  
Given: 環境變數 `STATION_NAME=bake` 與 `STATION_NAME=nonsense`  
When: 啟動站點程序  
Then: 前者以 bake 站運作;後者啟動失敗並回報無效站名


- **AT-09** — 站點以真實時間加工並切換 busy/idle

Verifies: FR-STN-02  
Phase: 2  
Method: 自動測試  
Given: 一個站點與一個加工時間為 2 秒的工作  
When: 以腳本發佈該工作  
Then: 站點狀態先為 busy,約 2 秒(容許 ±0.3 秒)後回到 idle


- **AT-10** — 加工事件發佈到 station.events 且 key 為站名

Verifies: FR-STN-03  
Phase: 2  
Method: 自動測試  
Given: 站點處理完一個工作  
When: 以新 consumer group 讀取 `station.events`  
Then: 看得到該工作的 `op_started`、`op_completed` 與狀態事件,key 為站名


- **AT-11** — flush 前被殺訊息重送不遺失且只記一次

Verifies: FR-STN-04, NFR-05  
Phase: 2  
Method: 自動測試  
Given: 站點加工中  
When: 在 Kafka flush 前強制終止站點  
Then: RabbitMQ 訊息被重送而非遺失;重啟後工作完成,事件記錄中只有一次完成


- **AT-12** — SIGTERM 乾淨結束且未 ack 不超過 1 則

Verifies: FR-STN-05, NFR-09  
Phase: 2  
Method: 自動測試  
Given: 站點加工中,prefetch 為 1  
When: 送出 SIGTERM  
Then: 站點在 10 秒內乾淨結束,同一時間未 ack 的訊息不超過 1 則


- **AT-13** — 重複投遞去重只記一次

Verifies: FR-SCHED-03, NFR-05  
Phase: 2  
Method: 自動測試  
Given: 同一個工作指令(相同 `event_id`,`(order_id, step, visit)` 相同)  
When: 發佈兩次(或強制重送)  
Then: 加工結果只被記錄一次;另以新 `event_id` 與 `visit=2` 發佈同站同單時,記錄為另一筆工作


## 3. Phase 3

- **AT-14** — 工單缺欄位拒絕，完整者欄位齊全

Verifies: FR-ORDER-01  
Phase: 3  
Method: 自動測試  
Given: 一張缺少 `due_ts` 的工單與一張完整工單  
When: 送入工單驗證  
Then: 缺欄位者被拒絕;完整者含品項、`qty`、`due_ts` 與唯一 `order_id`


- **AT-15** — 多品項各站時間不同且批量時間為單位乘 qty

Verifies: FR-ORDER-02  
Phase: 3  
Method: 自動測試  
Given: 兩種不同品項的工單  
When: 取得各站加工時間  
Then: 各品項在各站的單位時間不同,批量加工時間等於單位時間乘以 `qty`,且無換線時間


- **AT-16** — 固定 seed 產生器輸出完全可重現

Verifies: FR-ORDER-03, NFR-06  
Phase: 3  
Method: 自動測試  
Given: 固定 seed 與到達率  
When: 產生器連續跑兩次各 N 張工單  
Then: 兩次的工單序列(品項、`qty`、`due_ts`)逐筆相同


- **AT-17** — 工單生命週期依序發佈到 order.lifecycle

Verifies: FR-ORDER-04  
Phase: 3  
Method: 自動測試  
Given: 一張工單完整走完產線  
When: 讀取 `order.lifecycle`  
Then: 依序看到 created、released、in process、completed,key 為 `order_id`


- **AT-18** — 高負載下 bake 為瓶頸且前隊累積

Verifies: FR-STN-06  
Phase: 3  
Method: KPI 對照 IE 理論值  
Given: 負載高於 bake 產能的多品項工單  
When: 跑穩態一段時間  
Then: bake 前佇列持續累積,其他站未被阻塞,實測 bake 使用率接近 100%,且 bake 的單位時間最長


- **AT-19** — 每站同時在途指令不超過 1 張

Verifies: FR-SCHED-01  
Phase: 3  
Method: 自動測試  
Given: 每站有大量待辦  
When: 觀察 RabbitMQ 各站 jobs 佇列  
Then: 任一時刻每站在途指令不超過 1 張


- **AT-20** — 下一站指令由排程器觸發而非站間直傳

Verifies: FR-SCHED-02  
Phase: 3  
Method: 自動測試  
Given: 一張工單在 spin 站完成  
When: 觀察 `dispatch` exchange 與事件順序  
Then: 下一站(sauce)指令由排程器在 spin 完成事實之後發出,站點之間沒有直接傳遞


- **AT-44** — lot 依 op_dispatched、op_started、op_completed 順序轉移

Verifies: FR-STN-07, FR-SCHED-05  
Phase: 3  
Method: 自動測試  
Given: 一個有 credit 的站點,以及一個在待辦清單中排在另一個 lot 之後的 lot  
When: 排程器派工且站點加工,以新 consumer group 讀取 `station.events`  
Then: 每個 lot 的事件順序為 `op_dispatched`、`op_started`、`op_completed`;`op_dispatched` 的 key 為目標站名並攜帶 `ready_ts`,且 `op_dispatched.ts` 不早於 `ready_ts`;lot 在該站 credit 可用之前沒有 `op_dispatched`(同站沒有第二張在途派工)


## 4. Phase 4

- **AT-21** — kpi.snapshots 含 throughput/WIP/cycle time/使用率

Verifies: FR-KPI-01  
Phase: 4  
Method: KPI 對照 IE 理論值  
Given: 基準產線(無故障)在固定負載下運行  
When: 讀取 `kpi.snapshots`  
Then: 含 throughput、WIP、cycle time(mean/p50/p95)與各站使用率,且屬於目前 `run_id`


- **AT-22** — 高負載下瓶頸穩定為 bake 不跳動

Verifies: FR-KPI-02  
Phase: 4  
Method: KPI 對照 IE 理論值  
Given: 負載高於 bake 產能  
When: 連續讀取 10 個以上快照  
Then: 瓶頸一律為 bake,不來回跳動


- **AT-23** — 理論參考值與實測值並列且吞吐接近瓶頸產能

Verifies: FR-KPI-03  
Phase: 4  
Method: KPI 對照 IE 理論值  
Given: 基準產線穩態  
When: 比較快照中的理論與實測值  
Then: 理論瓶頸產能、原始加工時間、臨界 WIP 與實測並列,實測 throughput 接近瓶頸產能


- **AT-24** — Little's Law 誤差帶內 pass 超出 fail

Verifies: FR-KPI-04, NFR-04  
Phase: 4  
Method: KPI 對照 IE 理論值  
Given: 基準產線已排除前 `WARMUP_ORDERS` 張暖機訂單  
When: 計算 L 與 λW  
Then: 相對誤差落在 `LITTLE_TOLERANCE` 內時回報 pass,超出則回報 fail


- **AT-25** — 新 run 重置 KPI 且舊資料不污染

Verifies: FR-KPI-05  
Phase: 4  
Method: 自動測試  
Given: 一個已有資料的 run  
When: 開始新 `run_id`  
Then: KPI 全部重置,舊 run 的事件不影響新 run,前 `WARMUP_ORDERS` 張訂單的資料被排除


- **AT-26** — 無 broker 下 fold 單元測試可重現

Verifies: FR-KPI-06, NFR-03  
Phase: 4  
Method: 自動測試  
Given: 錄製的事件固件且沒有任何 broker  
When: 執行 KPI fold 單元測試  
Then: 通過,結果可重現


- **AT-46** — 站內佇列時間與手算值一致

Verifies: FR-KPI-08  
Phase: 4  
Method: 自動測試  
Given: 錄製的事件固件,含已知的 `ready_ts`、`op_dispatched`、`op_started` 時間戳,並含一則重複的 `op_dispatched` 與一則 attempt=2 的重送 `op_started`  
When: 執行純函式 `fold`  
Then: 各站 WAIT、Track-In 與合計佇列時間的 mean/p95 與手算值一致,重複事件只計一次,前 `WARMUP_ORDERS` 張訂單被排除,不需任何 broker


- **AT-27** — 交期指標與手算值一致

Verifies: FR-KPI-07  
Phase: 7  
Method: 自動測試  
Given: 含已延遲與準時完成工單的事件固件  
When: 執行 KPI fold  
Then: 延遲張數、延遲比例與 tardiness 與手算值一致


## 5. Phase 5

- **AT-28** — 手動下單進產線且帶入所選優先權

Verifies: FR-ORDER-05  
Phase: 5  
Method: 手動  
Given: 儀表板已開啟  
When: 填表單手動下單一次(含以滑桿選定一個優先權,例如 10)  
Then: 工單出現在 `order.lifecycle` 並進入產線,事實攜帶所選優先權;`due_ts` 由 ETA 推得(或使用者覆寫),與優先權無關


- **AT-29** — 七站狀態與佇列長度即時顯示

Verifies: FR-DASH-01  
Phase: 5  
Method: 手動  
Given: 產線運行中  
When: 觀察瀏覽器  
Then: 七站皆顯示狀態與佇列長度並即時更新


- **AT-30** — SSE 推送頻率正常且長時開啟不卡

Verifies: FR-DASH-02, NFR-02  
Phase: 5  
Method: 手動  
Given: 儀表板連續開啟 30 分鐘  
When: 檢查 SSE 訊息頻率與記憶體  
Then: 頻率為 1 至 2 Hz,圖表資料點不超過上限,頁面保持流暢


- **AT-31** — 開始/暫停/續跑/重置與到達率設定即時生效

Verifies: FR-DASH-03  
Phase: 5  
Method: 手動  
Given: 儀表板已開啟  
When: 設定到達率並按開始、暫停、續跑、重置  
Then: 產線依操作改變,重置後產生新 `run_id`


- **AT-32** — broker 指標與外部管理介面一致

Verifies: FR-DASH-04  
Phase: 5  
Method: 手動  
Given: 產線運行中且有訊息在佇列  
When: 分別查看 RabbitMQ 管理介面與 Kafka UI  
Then: RabbitMQ 管理介面顯示各佇列深度與 `dispatch.dlq` 數量,Kafka UI 顯示 consumer group lag,兩者與產線實際狀況一致


- **AT-33** — 規則切換指令經 control 通道送達排程器

Verifies: FR-DASH-05  
Phase: 5  
Method: 自動測試  
Given: 儀表板送出規則切換指令  
When: 觀察 `scheduler.control` 佇列  
Then: 排程器收到控制訊息並回應目前規則


## 6. Phase 6

- **AT-34** — 固定 seed 停機時程可重現且工作中斷後於修復後判定失敗

Verifies: FR-FAIL-01, NFR-06  
Phase: 6  
Method: 自動測試  
Given: 固定 seed 與 MTBF/MTTR  
When: 跑兩次  
Then: 兩次停機與恢復時間點相同(以忙碌時間為基準);進行中的工作暫停,修復後不續跑而是判定失敗(`op_failed`),不產生 `op_completed`


- **AT-35** — 站點 down 時停派恢復後續派

Verifies: FR-FAIL-02  
Phase: 6  
Method: 自動測試  
Given: 某站發佈 down 事實  
When: 觀察排程器派工  
Then: 排程器停止對該站派工,恢復事實之後繼續;儀表板顯示 down


- **AT-36** — poison 指令重試達上限後進 DLQ

Verifies: FR-FAIL-03, NFR-08  
Phase: 6  
Method: 自動測試  
Given: 一個必定失敗的 poison 指令  
When: 站點每次處理都回報 `op_failed`  
Then: 排程器經 hold queue `scheduler.rework.<station>.hold`(可在 RabbitMQ 管理介面觀察)剛好 rework 兩次(每次 `op_failed` 即歸還站點 credit,該站在失敗 lot 冷卻期間持續被派下一張 lot;`visit` 遞增;品質失敗與機台故障失敗共用同一張 rework 表,上限計 rework 次數),最後進入 `dispatch.dlq` 並發出 `order.lifecycle failed`,站點 credit 已歸還,沒有 `op_completed`,並可在管理介面看到


- **AT-37** — 儀表板注入故障按鈕即時讓站點 down

Verifies: FR-FAIL-04  
Phase: 6  
Method: 手動  
Given: 儀表板已開啟,某站運行中  
When: 按下該站「注入故障」按鈕  
Then: 該站立即進入 down 狀態


- **AT-38** — chaos 殺容器後工作重送且恰好完成一次

Verifies: FR-FAIL-05, NFR-05  
Phase: 6  
Method: 手動  
Given: 某站容器加工中  
When: 執行 chaos 腳本強制終止該容器  
Then: 工作被重送並完成,記錄中恰好一次


- **AT-45** — lot 例外轉移符合狀態機

Verifies: FR-STN-08  
Phase: 6  
Method: 自動測試  
Given: 分別觸發各例外的 lot  
When: (a) 站點在 RUN 中 Down 後恢復,(b) 處理失敗一次後 rework 成功,(c) poison 指令 rework 用盡,(d) 站點在 ack 前被終止  
Then: (a) lot 維持 RUN、指令 unacked,修復後不續跑,站點發 `op_failed(cause=breakdown)` 後 ack,依 rework 上限 rework(`visit+1`)或進 `dispatch.dlq`,不是「完成」;(b) 站點發 `op_failed` 後 ack,lot 在 hold queue TTL 期間為 WAIT(非 Track-In),第二個 `op_dispatched`(`visit` 2)於 TTL 後真實派工時發出,`ready_ts` = 失敗 `op_failed` 的 ts,Track-In 不含 TTL,rework 完成;(c) 指令進入 `dispatch.dlq`(HOLD)且不產生 `op_completed`;(d) 指令重送,第二次 `op_started` 帶 `attempt=2`,lot 不回 WAIT,實際只有一次 `op_completed`

- **AT-48** — 加工中 Down 修復後判定失敗,依共用 rework 表 rework 或 scrap

Verifies: FR-STN-08, FR-FAIL-01, FR-FAIL-03, NFR-08  
Phase: 6  
Method: 自動測試  
Given: 加工中的 job,站點隨機停機或手動 Break;各站 rework 上限為預設(`spin` 2、其餘 0)  
When: (a) `spin` 站加工中連續 Down 並修復,(b) 上限為 0 的站加工中 Down 並修復,(c) 站點閒置時 Down,(d) 站點加工中被終止(offline),(e) 重播不含 `cause` 的舊 `op_failed`  
Then: (a) 每次修復後發一個 `op_failed(cause=breakdown)` 且不產生 `op_completed`,`visit` 1、2 各 rework 一次(經 `dispatch.retry` 約 5 秒 TTL,`visit+1`),`visit` 3 失敗即進 `dispatch.dlq` 並發出 `order.lifecycle failed`、credit 歸還;(b) 首次失敗即進 `dispatch.dlq`;(c) 不產生 `op_failed`,恢復後照常派工;(d) 不產生 `op_failed`,指令以相同 `visit` 重送;(e) 缺少 `cause` 的事件重播時視為 `quality`


## 7. Phase 7

- **AT-39** — 切換規則後釋放順序依新規則

Verifies: FR-SCHED-04  
Phase: 7  
Method: 自動測試  
Given: 待辦中有不同交期與加工時間的工單  
When: 於執行中切換 FIFO、EDD、SPT  
Then: 釋放順序依對應規則改變


- **AT-40** — 一鍵依序跑三規則且輸入相同狀態全新

Verifies: FR-COMP-01, NFR-06  
Phase: 7  
Method: 自動測試  
Given: 固定 seed 工單與停機時程  
When: 觸發一次比較動作  
Then: FIFO、EDD、SPT 依序執行,每次以全新狀態與新 `run_id` 開始,輸入工單相同


- **AT-41** — 並排表顯示四項指標

Verifies: FR-COMP-02  
Phase: 7  
Method: 手動  
Given: 比較已完成  
When: 查看並排表格  
Then: 每個規則顯示平均 cycle time、tardiness、延遲比例、makespan


- **AT-42** — Kafka replay 重算 KPI 與即時一致

Verifies: FR-COMP-03  
Phase: 7  
Method: 自動測試  
Given: 一個歷史 run 的事件仍在 Kafka  
When: 以新 consumer group 與 earliest 重播並重算  
Then: 重算的 KPI 與當時即時結果一致


- **AT-43** — 比較結果匯出 CSV 格式正確

Verifies: FR-COMP-04  
Phase: 7  
Method: 自動測試  
Given: 比較結果  
When: 匯出 CSV  
Then: 檔案含每個規則一列與四個指標欄位,可被標準 CSV 解析


## 8. Phase 8

- **AT-47** — recipe 變更事實與 replay 重建

Verifies: FR-STN-09  
Phase: 8  
Method: 自動測試  
Given: 一個執行中的 run,以及一個已記錄的歷史 run  
When: (a) 於執行中修改某站某品項的 recipe 秒數,(b) 於 run 開始前修改(尚未按開始),(c) 以新的 consumer group 重播該 run 並重算 KPI  
Then: (a) `station.events` 上出現恰好一筆 `recipe.changed`,key 為站名,帶站點、品項、舊值、新值與套用時間;同值重送不產生事實;(b) 不產生任何 `recipe.changed`,其效果出現在該 run 開始時的 `recipe.snapshot`(含七站生效表,key = `scheduler`);(c) 重算以 snapshot 加上 `ts` 不晚於既有 KPI 快照的 `recipe.changed` 重建生效表,報告標示來源為事件;無 snapshot 的舊 run 回退到磁碟值且報告標示來源為磁碟


## 9. Phase 07.1

- **AT-49** — 下貨 ETA 凍結為事實,空線時等於理論總加工時間

Verifies: FR-ORDER-06, FR-SCHED-06  
Phase: 07.1  
Method: 自動測試  
Given: 排程器持有數張在製 lot 與真實 recipe;另一情境為空線  
When: 對新工單估算 ETA,組成 `order.submitted` 與 `order.lifecycle` 事實,再交給排程器 `rebuild` 與儀表板 fold(含亂序與重複)  
Then: 兩者還原出相同的 `eta_ts` 與 `station_eta`(七站嚴格遞增);空線時 ETA 領先時間等於理論總加工時間;FIFO 與 SPT 在加工時間不同時給出不同估算;估算 RPC 不改動排程器狀態

- **AT-50** — due_ts 由 ETA x buffer_k 帶入,可覆寫並可回退

Verifies: FR-ORDER-07  
Phase: 07.1  
Method: 自動測試  
Given: 自動與手動下單路徑,`buffer_k` 為 1.0 或 1.5  
When: (a) 手動單不給 `due_ts`,(b) 手動單明確給 `due_ts`,(c) 自動單在 `due_mode=eta`,(d) 排程器不可用或 `due_mode=theory`  
Then: (a) `due_ts = created_ts + lead x buffer_k`;(b) 使用明確值;(c) 同 (a) 且帶 `eta_ts`;(d) 使用理論 due 且不帶 ETA 欄位,下單仍成功;同 seed 的 `plan_orders` 輸出不變

- **AT-51** — ETA 誤差為純函式,且可由重播的事實重算

Verifies: FR-KPI-09  
Phase: 07.1  
Method: 自動測試  
Given: 一張帶 `eta_ts` 的工單完成,事實存在 Kafka `order.lifecycle`  
When: 以新的 consumer group 從最早位移重播並以 `pizza.kpi.eta` 計算  
Then: `eta_error_s` 等於 `completed_ts - eta_ts` 且為有限值;缺時間戳時為 None;重複送達以 `order_id` 收斂

- **AT-52** — lot 視窗與下單表單的 ETA 顯示

Verifies: FR-DASH-06  
Phase: 07.1  
Method: 手動  
Given: 瀏覽器開啟 http://localhost:8000 且有執行中的 run  
When: 開啟下單視窗、手動輸入 Due、調整 Buffer、下單後點開 lot、等完成、切換速度到 4x、讓 bake 站故障、停止 scheduler 容器  
Then: Due 預填並標 Auto,輸入後標 Manual 且可重設;Buffer 1.5 使 Auto due 較晚;lot 視窗顯示 Initial ETA、Current estimate、Remaining 倒數與 stepper 各站預估;完成後出現 ETA error 與 On time/Late;4x 時新預估約縮為 1/4;故障估算含修復時間;排程器停止時顯示 Auto 不可用且仍可下單


- **AT-53** — 待辦以優先權為第一排序鍵

Verifies: FR-SCHED-07  
Phase: 07.2  
Method: 自動測試  
Given: 同一站待辦含不同優先權、不同 due_ts 與加工時間的 lot,以及一個加工中的 lot  
When: 分別在 FIFO、EDD、SPT 下釋放下一張指令  
Then: 三種規則皆先釋放優先權數字最小者;同優先權才依規則鍵,再依 `entered_ts`;加工中的 lot 不被搶占,RabbitMQ 站佇列不變

- **AT-54** — 下單後調整優先權發出 priority.changed 並可重播

Verifies: FR-SCHED-08, FR-ORDER-05  
Phase: 07.2  
Method: 自動測試  
Given: 一個待辦中的工單與一份含舊 rush 欄位的歷史事件  
When: 送出 `lot.set_priority`(新值、同值、超出 1~99、未知工單),再以 `rebuild` 重播事實  
Then: 新值產生恰好一筆 `priority.changed`(key = `order_id`)且待辦立即重排;同值不產生事實;超出範圍回 422、未知工單回 404、已終結工單回 409;重播還原相同順序,舊 rush 重播為 10、normal 為 50


## 9A. 補充驗收(quick 261029-keo)

- **AT-55** — 倍速切換發佈 time_scale.changed

Verifies: FR-DASH-07  
Phase: 09  
Method: 自動測試  
Given: 儀表板運作中,倍速為 1x  
When: 依序送出 `POST /api/time_scale` 的 scale 2 與 8,並以新 consumer group 讀取 `sim.timescale`  
Then: 每次成功切換各發佈恰好一筆 `time_scale.changed`(scale 分別為 2 與 8);scale 不在 {1, 2, 4, 8} 時回 422 且不發佈事實


- **AT-56** — KPI 將倍速歷史換算為模擬秒

Verifies: FR-KPI-10  
Phase: 09  
Method: 自動測試  
Given: 一個 run 中途由 1x 切到 4x 的錄製事件與 `sim.timescale` 歷史  
When: 以純函式 KPI fold 重播並計算 cycle time  
Then: 切換前的牆鐘區間以 1 換算、切換後以 4 換算;結果與手算模擬秒一致,不需 broker


- **AT-57** — 倍速按鈕高亮與動畫步進上限

Verifies: FR-DASH-07  
Phase: 09  
Method: 手動  
Given: 儀表板開啟且產線運行中  
When: 依序點選 1x、2x、4x、8x  
Then: 對應按鈕即時高亮;8x 時 lot 動畫不跳格(每幀推進不超過 0.4 模擬秒)


- **AT-58** — 暫停與續跑狀態機

Verifies: FR-DASH-08  
Phase: 5  
Method: 自動測試  
Given: 系統無 run(none)  
When: (a) 送出 pause,(b) start 後 pause,(c) resume,(d) 於 paused 狀態 reset  
Then: (a) 回 409;(b) 狀態為 paused 並發佈 `run.paused`;(c) 回到 running 並發佈 `run.resumed`;(d) 狀態回到 none,並換新 `run_id`


- **AT-59** — 暫停時凍結、續跑後接續

Verifies: FR-DASH-08  
Phase: 5  
Method: 手動  
Given: 產線運行中且有 lot 加工中  
When: 按下暫停,等待 10 秒後續跑  
Then: 3D 動畫與站點剩餘加工時間凍結;續跑後從剩餘時間繼續,不重新開始


- **AT-60** — 主機睡眠偵測門檻

Verifies: FR-DASH-09  
Phase: 5  
Method: 自動測試  
Given: 注入可控的牆鐘與 monotonic 時間源,`sleep_detect_s` 為 5 秒  
When: (a) 牆鐘跳 30 秒而 monotonic 不動,(b) 牆鐘倒退 30 秒  
Then: (a) 偵測到約 30 秒的睡眠並發佈 `host.sleep_detected`;(b) 不發佈任何事實


- **AT-61** — 睡眠後自動暫停與橫幅

Verifies: FR-DASH-09  
Phase: 5  
Method: 手動  
Given: 產線運行中  
When: 讓主機睡眠超過門檻後喚醒  
Then: 線路自動進入 paused,儀表板顯示睡眠橫幅,並可手動續跑


- **AT-62** — 自動單優先權分配

Verifies: FR-ORDER-08  
Phase: 07.2  
Method: 自動測試  
Given: 固定 seed,`priority_high_ratio` 為 0.10、`priority_high_value` 為 10  
When: (a) 以同一 seed 產生 1000 張工單兩次;(b) `priority_high_ratio` 分別設為 0 與 1 各產生一次  
Then: (a) 兩次的優先權序列逐筆相同,高緊急比例落在 0.10 ± 0.03;(b) ratio 0 時全部為 50,ratio 1 時全部為 10


- **AT-63** — 到達率上限

Verifies: FR-ORDER-09  
Phase: 5  
Method: 自動測試  
Given: 儀表板與 run 參數  
When: (a) `POST /api/rate` 送 12.5,(b) 送 12.0,(c) start 帶 `arrival_rate_per_min` 12.5  
Then: (a) 與 (c) 回 422;(b) 回成功並更新到達率


- **AT-64** — 儀表板 pizzas/min 換算

Verifies: FR-ORDER-09  
Phase: 5  
Method: 手動  
Given: 儀表板開啟  
When: 將到達率滑桿設為 6 pizzas/min 後開始 run  
Then: 送出的到達率為 3.0 lots/min(6 ÷ 2);滑桿範圍為整數 1 至 12


- **AT-65** — 手動報廢與放行

Verifies: FR-SCHED-09  
Phase: 6  
Method: 自動測試  
Given: 一張在 WAIT 的 lot,以及一張已 BANK 的 lot  
When: (a) 對 WAIT lot 送 `lot.scrap`,(b) 對 BANK lot 送 `lot.release`  
Then: (a) 發佈 `order.lifecycle` failed,工單進入 `dispatch.dlq`,且沒有 `op_completed`;(b) 發佈 `lot.released`(by=manual),lot 回到待辦


- **AT-66** — 延後 bank/scrap 請求

Verifies: FR-SCHED-10  
Phase: 6  
Method: 自動測試  
Given: 一張加工中的 lot 與一張冷卻中的 lot  
When: (a) 對加工中 lot 送 bank 請求並等該站完工,(b) 對加工中 lot 送 bank 後於完工前送 `lot.hold_cancel`,(c) 對冷卻中 lot 送 bank,(d) 對 done 狀態的 lot 送 scrap  
Then: (a) 依序出現 `lot.hold`(requested、applied),之後 `lot.banked`(source=manual);(b) 發佈 `lot.hold`(cancelled),不產生 `lot.banked`;(c) 立即套用;(d) 請求被拒絕且不發佈 `lot.hold`


- **AT-67** — 配方 TOML 格式

Verifies: FR-STN-10  
Phase: 3  
Method: 自動測試  
Given: `src/pizza/recipes` 下的七個 TOML 檔  
When: 載入所有配方  
Then: 七站皆可載入,每站含 margherita、pepperoni、veggie 三品項,且秒數皆為正數


- **AT-68** — 比較回合結果事實

Verifies: FR-COMP-05  
Phase: 7  
Method: 自動測試  
Given: 一個進行中的比較回合  
When: 送出 `kpi.close_round`  
Then: 以新 consumer group 讀取 `kpi.snapshots`,恰好看到一筆 `comparison.result`,key 為 `cmp:<comparison_id>:<round>`,round 介於 1 與 3


- **AT-69** — 比較流程控制

Verifies: FR-COMP-06  
Phase: 7  
Method: 自動測試  
Given: 比較服務閒置  
When: (a) start 帶 `n_orders` 4,(b) start 使用預設參數後依序 next、end 完成三回合,(c) 第二回合中呼叫 cancel  
Then: (a) 回 422;(b) 三回合依序完成;(c) 剩餘回合中止,狀態回到閒置


- **AT-70** — REST 契約與路由一致

Verifies: FR-DASH-12  
Phase: 5  
Method: 自動測試  
Given: 儀表板 FastAPI 應用程式與 `API-SPEC.md` §2 端點表  
When: 比對應用程式的路由清單與端點表  
Then: 每條路由(不含靜態頁面 `/`)都列於端點表,端點表中每條也都存在於應用程式;不一致即測試失敗


- **AT-71** — 3D 產線與相機

Verifies: FR-DASH-10  
Phase: 5  
Method: 手動  
Given: 產線運行中  
When: 拖曳平移、滾輪縮放到極限、點選 Focus 聚焦某站,再點選 Follow 跟隨某 lot  
Then: 平移與縮放不超出限制;聚焦與跟隨正確;高緊急 lot(priority ≤ 10)顯示較大


- **AT-72** — 搜尋與事件 feed

Verifies: FR-DASH-11  
Phase: 5  
Method: 手動  
Given: 產線運行中且有 lot  
When: 按 `/`、輸入站名或工單號後按 Enter,再按 Esc;觀察事件 feed  
Then: 搜尋框取得焦點;Enter 選取第一筆符合的站或工單;Esc 清除搜尋;feed 只顯示最近 5 筆事件

## 10. 追溯矩陣


| REQ        | FR                       | AT           | Phase |
| ---------- | ------------------------ | ------------ | ----- |
| INFRA-01   | FR-INFRA-01, FR-INFRA-02 | AT-01, AT-02 | 1     |
| INFRA-02   | FR-INFRA-03, FR-INFRA-04 | AT-03, AT-04 | 1     |
| INFRA-03   | FR-INFRA-05, FR-INFRA-06 | AT-05, AT-06 | 1     |
| INFRA-04   | FR-INFRA-07              | AT-07        | 1     |
| ORDER-01   | FR-ORDER-01              | AT-14        | 3     |
| ORDER-02   | FR-ORDER-02              | AT-15        | 3     |
| ORDER-03   | FR-ORDER-05              | AT-28, AT-54 | 5     |
| ORDER-04   | FR-ORDER-03              | AT-16        | 3     |
| ORDER-05   | FR-ORDER-04              | AT-17        | 3     |
| ORDER-06   | FR-ORDER-06, FR-SCHED-06 | AT-49        | 07.1  |
| ORDER-07   | FR-ORDER-07              | AT-50        | 07.1  |
| STATION-01 | FR-STN-01, FR-STN-02     | AT-08, AT-09 | 2     |
| STATION-02 | FR-STN-06                | AT-18        | 3     |
| STATION-03 | FR-STN-02, FR-STN-03, FR-STN-07, FR-STN-08, FR-SCHED-05 | AT-09, AT-10, AT-44, AT-45 | 2     |
| STATION-04 | FR-STN-04, FR-STN-05     | AT-11, AT-12 | 2     |
| STATION-05 | FR-STN-06                | AT-18        | 3     |
| STATION-06 | FR-STN-09               | AT-47        | 8     |
| SCHED-01   | FR-SCHED-01, FR-SCHED-05 | AT-19, AT-44 | 3     |
| SCHED-02   | FR-SCHED-02              | AT-20        | 3     |
| SCHED-03   | FR-SCHED-04              | AT-39        | 7     |
| SCHED-04   | FR-SCHED-03              | AT-13        | 2     |
| SCHED-05   | FR-SCHED-07              | AT-53        | 07.2  |
| SCHED-06   | FR-SCHED-08              | AT-54        | 07.2  |
| FAIL-01    | FR-FAIL-01               | AT-34, AT-48 | 6     |
| FAIL-02    | FR-FAIL-02               | AT-35        | 6     |
| FAIL-03    | FR-FAIL-03, FR-STN-08    | AT-36, AT-45, AT-48 | 6     |
| FAIL-04    | FR-FAIL-04, FR-FAIL-05   | AT-37, AT-38 | 6     |
| KPI-01     | FR-KPI-01, FR-KPI-08     | AT-21, AT-46 | 4     |
| KPI-02     | FR-KPI-02                | AT-22        | 4     |
| KPI-03     | FR-KPI-03                | AT-23        | 4     |
| KPI-04     | FR-KPI-04                | AT-24        | 4     |
| KPI-05     | FR-KPI-05                | AT-25        | 4     |
| KPI-06     | FR-KPI-06                | AT-26        | 4     |
| KPI-07     | FR-KPI-07                | AT-27        | 7     |
| KPI-08     | FR-KPI-09                | AT-51        | 07.1  |
| DASH-01    | FR-DASH-01               | AT-29        | 5     |
| DASH-02    | FR-DASH-02               | AT-30        | 5     |
| DASH-03    | FR-DASH-03, FR-DASH-05   | AT-31, AT-33 | 5     |
| DASH-04    | FR-DASH-04               | AT-32        | 5     |
| DASH-05    | FR-DASH-06               | AT-52        | 07.1  |
| COMP-01    | FR-COMP-01               | AT-40        | 7     |
| COMP-02    | FR-COMP-02               | AT-41        | 7     |
| COMP-03    | FR-COMP-03               | AT-42        | 7     |
| COMP-04    | FR-COMP-04               | AT-43        | 7     |
| ORDER-08   | FR-ORDER-08              | AT-62        | 07.2  |
| ORDER-09   | FR-ORDER-09              | AT-63, AT-64 | 5     |
| STATION-07 | FR-STN-10                | AT-67        | 3     |
| SCHED-07   | FR-SCHED-09, FR-SCHED-10 | AT-65, AT-66 | 6     |
| KPI-09     | FR-KPI-10                | AT-56        | 09    |
| DASH-07    | FR-DASH-07               | AT-55, AT-57 | 09    |
| DASH-08    | FR-DASH-08               | AT-58, AT-59 | 5     |
| DASH-09    | FR-DASH-09               | AT-60, AT-61 | 5     |
| DASH-10    | FR-DASH-10               | AT-71        | 5     |
| DASH-11    | FR-DASH-11               | AT-72        | 5     |
| DASH-12    | FR-DASH-12               | AT-70        | 5     |
| COMP-05    | FR-COMP-05               | AT-68        | 7     |
| COMP-06    | FR-COMP-06               | AT-69        | 7     |


延後(deferred,無 FR/AT):ADV-01, ADV-02, ADV-03, ADV-04, ADV-05, TOOL-01, TOOL-02
