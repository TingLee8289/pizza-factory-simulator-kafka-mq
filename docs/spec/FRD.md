# FRD_Pizza Factory Simulator

## 0. 修訂歷史

| 版本 | 日期 | 修訂者 | 說明 |
| --- | -------- | ---------- | ---- |
| 1.0 | 2026-10-15 | Ting Lee | Init |
| 1.1 | 2026-10-15 | Ting Lee | 細項改為表格 |
| 1.2 | 2026-10-16 | Ting Lee | 新增 lot 狀態機、op_dispatched 與站內佇列時間 FR |
| 1.3 | 2026-10-20 | Ting Lee | 新增 recipe 變更事實與 run 開始快照 FR (FR-STN-09) |
| 1.4 | 2026-10-20 | Ting Lee | FR-DASH-04 改為 broker 指標由外部管理介面提供,儀表板不內嵌面板(見 quick 261020-u55) |
| 1.5 | 2026-10-20 | Ting Lee | FR-ORDER-04 生命週期補 failed(見 SYS-16) |
| 1.6 | 2026-10-20 | Ting Lee | Phase 06.1:加工中 Down 修復後判定失敗,`op_failed.cause` (FR-STN-08, FR-FAIL-01, FR-FAIL-03) |
| 1.7 | 2026-10-20 | Ting Lee | 06.2 rework credit release (FR-SCHED-01, FR-STN-08) |
| 1.8 | 2026-10-21 | Ting Lee | Phase 07.1:下貨 ETA(FR-ORDER-06、FR-ORDER-07、FR-SCHED-06、FR-KPI-09、FR-DASH-06) |
| 1.9 | 2026-10-21 | Ting Lee | Phase 07.2:數字優先權(FR-ORDER-05、FR-DASH-03 改寫;新增 FR-SCHED-07、FR-SCHED-08) |
| 1.10 | 2026-10-29 | Ting Lee | 補充規格缺漏(quick 261029-keo):FR-ORDER-08、FR-ORDER-09、FR-STN-10、FR-SCHED-09、FR-SCHED-10、FR-KPI-10、FR-DASH-07 至 FR-DASH-12、FR-COMP-05、FR-COMP-06;FR-DASH-03 的「停止」改為暫停/續跑 |

> 每條 FR 對應 PRD 的一個或多個 REQ(見 Satisfies 欄),並標示所屬 Phase(見 Phase 欄)。
> 訊息欄位與拓撲細節不在此重述:欄位以 `src/pizza/common/envelope.py` 為準,拓撲以 `src/pizza/common/topology.py` 與 SRD 的 SYS 條目為準。
> 驗收標準見 AT.md。DOMAIN:INFRA、ORDER、STN、SCHED、FAIL、KPI、DASH、COMP。
> 儀表板的 REST 與 SSE 端點契約(欄位、錯誤碼、狀態機轉移)見 `docs/spec/API-SPEC.md`。

## 1. 基礎設施 (INFRA) — Phase 1 已實作

| ID | 描述 | Satisfies | Phase |
| --- | --- | --- | --- |
| **FR-INFRA-01** | 已實作:執行 `docker compose up` 即啟動 Kafka、RabbitMQ、Kafka UI 與一次性 `init` 服務,無需手動步驟。服務布局見 SYS-11。 | INFRA-01 | 1 |
| **FR-INFRA-02** | 已實作:Kafka 與 RabbitMQ 具備 healthcheck,`init` 服務在兩者 healthy 後才執行。 | INFRA-01 | 1 |
| **FR-INFRA-03** | 已實作:`init` 服務透過 `bootstrap.py` 建立全部 Kafka topic 與 RabbitMQ exchange/queue/binding;重跑不改變任何東西且不報錯。 | INFRA-02 | 1 |
| **FR-INFRA-04** | 已實作:拓撲只在 `topology.py` 以資料宣告,不含 broker I/O;其他程式不得硬編 topic/queue 名稱。見 SYS-05。 | INFRA-02 | 1 |
| **FR-INFRA-05** | 已實作:所有訊息為 `Envelope` 子型別,缺少必要欄位、`ts` 無時區、`schema_version` 非 1、未知 `type` 時解析失敗並拋出 `InvalidEnvelope`。欄位定義見 SYS-12。 | INFRA-03 | 1 |
| **FR-INFRA-06** | 已實作:Producer 遇到無效訊息拒絕發送(Kafka 與 RabbitMQ 皆然);RabbitMQ 消費者收到無效訊息直接 reject 並經 `dlx` 進入 `dispatch.dlq`;Kafka 消費者記錄 log 並略過。指令只走 RabbitMQ、事實只走 Kafka(`Command`/`Fact` 角色標記強制)。 | INFRA-03 | 1 |
| **FR-INFRA-07** | 已實作:`docs/messaging-roles.md` 寫明「RabbitMQ 傳指令、Kafka 傳事實」,附指令與事實各一範例與流程圖。見 SYS-02。 | INFRA-04 | 1 |

## 2. 工單與品項 (ORDER)

| ID | 描述 | Satisfies | Phase |
| --- | --- | --- | --- |
| **FR-ORDER-01** | 工單包含品項(product)、批量 `qty` 與交期 `due_ts`,並有唯一 `order_id`;缺欄位的工單被拒絕。 | ORDER-01 | 3 |
| **FR-ORDER-02** | 系統支援多種披薩品項;每個品項在七站各有獨立的單位加工時間,批量加工時間為單位時間乘以 `qty`;品項之間無換線時間。 | ORDER-02 | 3 |
| **FR-ORDER-03** | 自動產生器依可設定的到達率產生工單,給定相同 seed 與到達率時,每次產生完全相同的工單序列(品項、`qty`、`due_ts`)。 | ORDER-04 | 3 |
| **FR-ORDER-04** | 每張工單的生命週期狀態(已建立、已釋放、加工中、完成、失敗)依序以事實發佈到 Kafka `order.lifecycle`,message key 為 `order_id`。 | ORDER-05 | 3 |
| **FR-ORDER-05** | 使用者可從儀表板表單手動下單(含優先權 1~99,數字越小越緊急,預設 50;優先權與 `due_ts` 無關,`due_ts` 仍由 ETA 或使用者覆寫決定),工單經 RabbitMQ `workorders` 進入系統,與自動產生的工單走相同路徑。 | ORDER-03 | 5 |
| **FR-ORDER-06** | 下貨當下(自動單於 generator 發佈前、手動單於 dashboard 送出時)以排程器唯讀估算逐站 ETA:由 `ordering_key` 判定排在前面的 lot,以瓶頸+WIP 排隊遞迴 `finish[s] = max(finish[s-1], free_at[s]) + proc[s]`。結果凍結為 `order.submitted` 與 `order.lifecycle`(created)的選填欄位 `eta_ts`、`station_eta`,之後不再改寫。估算失敗時不帶這兩個欄位。 | ORDER-06 | 07.1 |
| **FR-ORDER-07** | `due_ts` 預設為 `created_ts + ETA 領先時間 x buffer_k`;手動單可預填並覆寫,明確給定的 `due_ts` 優先;generator 自動單在 `due_mode=eta` 時同樣帶入,`due_mode=theory` 或估算失敗時回退為原本的理論 due。 | ORDER-07 | 07.1 |
| **FR-ORDER-08** | 自動產生器的工單依 `priority_high_ratio`(預設 0.10)以獨立 seed 串流(`priority`)分配優先權:命中者為 `priority_high_value`(預設 10),其餘為 50;同 seed 與參數下逐筆相同。 | ORDER-08 | 07.2 |
| **FR-ORDER-09** | 後端 `arrival_rate_per_min`(lots/min)上限為 12.0,超出回 422。儀表板以 pizzas/min(整數 1–12)呈現,送出前除以 2 換算為 lots/min。 | ORDER-09 | 5 |

## 3. 站點 (STN)

| ID | 描述 | Satisfies | Phase |
| --- | --- | --- | --- |
| **FR-STN-01** | 同一個程式映像可作為七站中任一站執行,站名由環境變數 `STATION_NAME` 指定;無效站名啟動失敗。 | STATION-01 | 2 |
| **FR-STN-02** | 站點收到工作後以真實時間(`asyncio.sleep` 對應加工時間)加工,過程中狀態由 idle 轉 busy,完成後回 idle。 | STATION-01, STATION-03 | 2 |
| **FR-STN-03** | 站點對每個工作發佈 `op_started`、`op_completed` 及狀態變更事件到 Kafka `station.events`,message key 為站名(`machine_id`)。排程器發佈的 `op_dispatched` 見 FR-SCHED-05。 | STATION-03 | 2 |
| **FR-STN-04** | 站點先 flush Kafka 事件,成功後才 ack RabbitMQ 訊息;在 ack 前被終止的訊息會重送而非遺失。見 SYS-07。 | STATION-04 | 2 |
| **FR-STN-05** | 站點使用 prefetch 1 與手動 ack,收到 SIGTERM 時完成或放回目前工作後乾淨結束。 | STATION-04 | 2 |
| **FR-STN-06** | 烘烤站(bake)的單位加工時間在所有品項上都是七站中最長,且站與站之間的佇列長度不設上限,上游不被下游阻塞。 | STATION-02, STATION-05 | 3 |
| **FR-STN-07** | lot 在單站依 WAIT、Track-In、RUN、Track-Out 順序轉移:排程器發佈 `op_dispatched` 使 WAIT 轉 Track-In,站點發出 `op_started` 轉 RUN,發出 `op_completed` 轉 Track-Out。狀態與事件對應見 SYS-16。 | STATION-03 | 3 |
| **FR-STN-08** | lot 的例外轉移符合 SYS-16:站點 Down 時 RUN 中的 lot 暫停(狀態維持 RUN、指令 unacked),修復後不續跑、判定失敗並發 `op_failed(cause=breakdown)`;Down 時閒置與站點 offline/crash 不判定(offline 重送沿用相同 `visit`);處理失敗時站點發 `op_failed` 後 ack(從不 reject),排程器隨即歸還站點 credit,將 lot 放入 `scheduler.rework.<station>.hold` 冷卻 TTL(期間為 WAIT,非 Track-In),之後以 `visit+1` 回待辦清單並於真實派工時發出 `op_dispatched`(`ready_ts` = 失敗 `op_failed` 的 ts);rework 用盡進 `dispatch.dlq`(HOLD)且不產生 `op_completed`;ack 前被終止則重送(第二次 `op_started` 帶 `attempt=2`,lot 不回 WAIT)。 | STATION-03, FAIL-03 | 6 |
| **FR-STN-09** | 修改站點 recipe 秒數時,站點在執行中的 run 內發佈 `recipe.changed` 事實到 Kafka `station.events`(key = 站名),帶站點、品項、舊值、新值與套用時間;run 開始時排程器發佈 `recipe.snapshot`(一次含七站生效表,key = `scheduler`)。使用者可在重播時以此重建該 run 當時的設定。見 SYS-19。 | STATION-06 | 8 |
| **FR-STN-10** | 七站配方由 `src/pizza/recipes/*.toml` 定義;每檔含 `station` 與 `[unit_time_s]` 下三品項(margherita、pepperoni、veggie)的正數秒數。可由 `recipes_dir` 設定覆寫檔案目錄。 | STATION-07 | 3 |

## 4. 排程派工 (SCHED)

| ID | 描述 | Satisfies | Phase |
| --- | --- | --- | --- |
| **FR-SCHED-01** | 排程器持有各站待辦清單;每站有一個 credit,同一時間最多一張指令在途,收到該站 `op_completed` 或 `op_failed`(rework 或 scrap)事實後歸還 credit。見 SYS-06。 | SCHED-01 | 3 |
| **FR-SCHED-02** | 站與站之間不直接傳遞工作;某站完成後,由排程器依目前派工規則決定並透過 RabbitMQ `dispatch` 釋放下一站指令。 | SCHED-02 | 3 |
| **FR-SCHED-03** | 消費者以指令 `event_id` 與 `(order_id, step, visit)` 去重:相同指令重送或重複發佈時只記錄一次加工結果;同一站的合法再次加工(新指令、`visit` 遞增)視為另一筆工作。`step` 為站名。見 SYS-08。 | SCHED-04 | 2 |
| **FR-SCHED-04** | 派工規則可在執行中切換 FIFO、EDD、SPT;切換後新釋放的順序依新規則,不需重啟服務。切換經控制通道傳達(SYS-13)。 | SCHED-03 | 7 |
| **FR-SCHED-05** | 排程器於派工指令的 RabbitMQ publisher confirm 成功後,發佈 `op_dispatched` 事實到 Kafka `station.events`,message key 為目標站 `machine_id`,並攜帶 `ready_ts`(lot 對該站變為可加工的時刻),使 WAIT 與 Track-In 時間可量測。契約見 SYS-17。 | SCHED-01, STATION-03 | 3 |
| **FR-SCHED-06** | 排程器經 RabbitMQ 控制通道提供唯讀 RPC `scheduler.estimate_eta`(單一假設工單)與 `scheduler.estimate_wip`(所有在製 lot 的目前預估完工時間);請求帶 `deadline_ts`,逾期丟棄;回覆為 `ok` 或 `{ok:false, code, detail}`;估算不改動排程器狀態。 | ORDER-06 | 07.1 |
| **FR-SCHED-07** | 排程器各站待辦的排序鍵為「優先權(1~99,小者先)→ 派工規則鍵(FIFO/EDD/SPT)→ `entered_ts`」,三種規則皆適用;不搶占加工中的 lot,也不改動 RabbitMQ 站佇列,只重排排程器待辦。 | SCHED-05 | 07.2 |
| **FR-SCHED-08** | 優先權可於下單後經控制通道 `lot.set_priority` 調整;排程器處理後發佈恰好一筆 `priority.changed` 事實到 Kafka `order.lifecycle`(key = `order_id`),待辦立即重排,`rebuild` 重播同一事實;值未變不發事實,REST 對未知工單回 404、已終結或尚未釋放的工單回 409、值超出 1~99 回 422。舊事件的 rush 重播為 10、normal 為 50。 | SCHED-06 | 07.2 |
| **FR-SCHED-09** | 手動報廢:對 WAIT 或 BANK lot 下 `lot.scrap`(reason:operator、quality、material、other),排程器以用盡 rework 相同方式結束該工單(dead-letter 並發 `order.lifecycle` failed),不可逆。手動放行:對 BANK lot 下 `lot.release`,lot 以 `lot.released`(by=manual)回到待辦。 | SCHED-07 | 6 |
| **FR-SCHED-10** | 延後手動 bank/scrap:加工中或已派工的 lot 接受 `lot.hold` 請求(state=requested),該站完工後套用(applied);冷卻中的 lot 立即套用;pre_release、done、failed 的 lot 拒絕請求。套用前可由 `lot.hold_cancel` 取消(state=cancelled)。 | SCHED-07 | 6 |

## 5. 故障與重試 (FAIL)

| ID | 描述 | Satisfies | Phase |
| --- | --- | --- | --- |
| **FR-FAIL-01** | 每站依可設定的 MTBF/MTTR 與 seed 產生停機與修復時程(以站點忙碌時間為基準的 seeded 指數分布);相同 seed 與參數下每次停機/恢復時間點相同;進行中的工作在停機時暫停,修復後不續跑,而是判定失敗(`op_failed`,`cause=breakdown`),隨機 MTBF 與手動 Break 皆同;Down 時閒置不判定。 | FAIL-01 | 6 |
| **FR-FAIL-02** | 站點停機與恢復各發佈一個事實到 Kafka `station.events`;站點定期發佈 `station.heartbeat`,排程器於 3 倍間隔未收到即判定 offline;排程器據此停止對該站派工,恢復後繼續;儀表板顯示該站為 down。 | FAIL-02 | 6 |
| **FR-FAIL-03** | 處理失敗時站點發 Kafka `op_failed` 後 ack(從不 reject);失敗來源有品質失敗(fail flag/poison)與機台故障(加工中 Down、修復後判定)兩種,`op_failed` 帶選用欄位 `cause`(`breakdown` 或 `quality`,缺省視為 `quality`);兩者共用同一張 rework 上限表;排程器以 `visit+1` 經 `dispatch.retry` 與 TTL 重試佇列重新派工,至每站 rework 上限;用盡後進入 `dispatch.dlq`,且可在 RabbitMQ 管理介面看到。scrap 即既有 DLQ/failed 路徑,不新增狀態或事件;rework 派工沿用既有 retry TTL。見 SYS-04。 | FAIL-03 | 6 |
| **FR-FAIL-04** | 儀表板提供 Break(依抽樣 MTTR 或 `duration_s` 自動修復)與 Repair 按鈕,可即時讓指定站點進入 down 狀態或修復。 | FAIL-04 | 6 |
| **FR-FAIL-05** | 提供 chaos 腳本強制終止指定站點容器;被中斷的工作由 RabbitMQ 重送並完成恰好一次(以去重達成)。 | FAIL-04 | 6 |

## 6. KPI

| ID | 描述 | Satisfies | Phase |
| --- | --- | --- | --- |
| **FR-KPI-01** | KPI 服務持續消費 Kafka 事實,對目前 `run_id` 定期發佈快照:throughput、WIP、cycle time(mean/p50/p95)、各站使用率。見 SYS-15。 | KPI-01 | 4 |
| **FR-KPI-02** | 瓶頸站辨識含遲滯:在高於瓶頸產能的負載下回報 bake 為瓶頸,且不會因小幅波動而在站之間來回跳動。 | KPI-02 | 4 |
| **FR-KPI-03** | 快照同時包含理論參考值(瓶頸產能、原始加工時間、臨界 WIP)與實測值,供並排顯示。 | KPI-03 | 4 |
| **FR-KPI-04** | Little's Law 檢查比較 L 與 λW,以容許誤差帶(參數見 NFR-04)判定 pass/fail,並排除暖機資料(前 `WARMUP_ORDERS` 張訂單)。 | KPI-04 | 4 |
| **FR-KPI-05** | KPI 支援暖機排除(排除前 `WARMUP_ORDERS` 張訂單,見 NFR-04);開始新 `run_id` 時所有 KPI 狀態重置,舊 run 的事件不污染新 run。 | KPI-05 | 4 |
| **FR-KPI-06** | KPI 計算為純函式 `fold(state, event) -> state`,不依賴 broker,可用錄製事件固件單元測試。見 NFR-03。 | KPI-06 | 4 |
| **FR-KPI-07** | KPI 快照包含交期指標:延遲張數、延遲比例、tardiness(以 `due_ts` 與完成時間計算)。 | KPI-07 | 7 |
| **FR-KPI-08** | KPI 快照包含各站站內佇列時間(WAIT、Track-In 與兩者合計,各含 mean 與 p95),僅由 `op_dispatched`、`op_started` 事件時間戳經純函式 `fold` 計算,排除暖機期,並以 `(order_id, step, visit)` 去重。公式見 SYS-17。 | KPI-01 | 4 |
| **FR-KPI-09** | ETA 誤差為純函式:`eta_error_s = completed_ts - eta_ts`(正值代表晚於預估),缺任一時間戳時為 None;另提供 n、平均誤差、平均絕對誤差與最大延遲的彙總。 | KPI-08 | 07.1 |
| **FR-KPI-10** | KPI 服務重播 `sim.timescale` 的完整倍速歷史,將每段牆鐘區間依當時倍速換算為模擬秒,使 cycle time 等時間型指標不受倍速切換影響。 | KPI-09 | 09 |

## 7. 儀表板 (DASH)

| ID | 描述 | Satisfies | Phase |
| --- | --- | --- | --- |
| **FR-DASH-01** | 瀏覽器顯示七站產線圖,每站顯示狀態(idle/busy/down)與佇列長度,即時更新。 | DASH-01 | 5 |
| **FR-DASH-02** | KPI 數值與圖表經 SSE 以 1-2 Hz 推送更新,長時間使用不造成瀏覽器負擔(圖表資料點有上限)。見 NFR-02。 | DASH-02 | 5 |
| **FR-DASH-03** | 頁面可手動下單(含優先權滑桿與數字顯示,lot 以 P<n> 標籤顯示,inspector 可編輯優先權)、設定到達率,並開始/暫停/續跑/重置一次 run,效果即時反映在產線上。 | DASH-03 | 5 |
| **FR-DASH-04** | broker 指標由外部管理介面提供:RabbitMQ 管理介面(:15672)顯示佇列深度與 DLQ 數量,Kafka UI 顯示 consumer lag。儀表板不內嵌此面板。 | DASH-04 | 5 |
| **FR-DASH-05** | 頁面提供派工規則選擇器,經控制通道(SYS-13)送出;Phase 5 僅 FIFO 生效,其餘規則於 Phase 7 起作用。 | DASH-03 | 5 |
| **FR-DASH-06** | lot 視窗顯示 Initial ETA、Current estimate(儀表板輪詢排程器,不是 Kafka 事實)、Remaining、完成後的 ETA error 與交期結果,stepper 各站顯示預估;下單表單的 Due 預填並標示 Auto/Manual,可重設為 Auto,Buffer 變更即重算;排程器不可用時顯示 Auto 不可用且仍可下單。 | DASH-05 | 07.1 |
| **FR-DASH-07** | 儀表板提供 1x/2x/4x/8x 倍速按鈕,送出 `POST /api/time_scale`;成功後發佈 `time_scale.changed` 到 `sim.timescale`。按鈕高亮依 `/api/state` 的 `time_scale`;動畫每幀推進的模擬時間上限為 0.4 秒。 | DASH-07 | 09 |
| **FR-DASH-08** | 儀表板提供暫停與續跑按鈕(`POST /api/run/pause`、`/api/run/resume`),排程器與站點發佈 `run.paused`、`run.resumed`;非法轉移回 409。原 FR-DASH-03 的「停止」由此取代。 | DASH-08 | 5 |
| **FR-DASH-09** | 生成器偵測主機睡眠(牆鐘與 monotonic 相鄰樣本差超過 `sleep_detect_s`,預設 5 秒)後發佈 `host.sleep_detected`;儀表板經暫停路徑自動停線並顯示橫幅。負向跳動忽略。 | DASH-09 | 5 |
| **FR-DASH-10** | 3D 產線以 Three.js 呈現。相機支援平移(X 限 ±30、Z 限 ±14)、縮放(0.6–3.2)、聚焦與跟隨;高緊急 lot(priority ≤ 10)以較大尺寸顯示。 | DASH-10 | 5 |
| **FR-DASH-11** | 搜尋框:`/` 聚焦、Enter 選取第一筆符合的站或工單、Esc 清除。事件 feed 顯示最近 5 筆站點事件。 | DASH-11 | 5 |
| **FR-DASH-12** | 儀表板 REST 與 SSE 契約由 `docs/spec/API-SPEC.md` 定義;變更端點須同步更新該文件。 | DASH-12 | 5 |

## 8. 規則比較 (COMP)

| ID | 描述 | Satisfies | Phase |
| --- | --- | --- | --- |
| **FR-COMP-01** | 一個動作即以同一組固定 seed 的工單與停機時程,依序跑 FIFO、EDD、SPT,每次執行以全新狀態(新 `run_id`、清空佇列)開始。 | COMP-01 | 7 |
| **FR-COMP-02** | 並排表格顯示各規則的平均 cycle time、tardiness、延遲比例與 makespan。 | COMP-02 | 7 |
| **FR-COMP-03** | 可用新的 consumer group 並設 `auto.offset.reset=earliest` 重播 Kafka,重算歷史 run 的 KPI,結果與原即時結果一致。 | COMP-03 | 7 |
| **FR-COMP-04** | 比較結果可匯出為 CSV 檔。 | COMP-04 | 7 |
| **FR-COMP-05** | KPI 服務收到 `kpi.close_round` 命令時發佈 `comparison.result`,內容含 comparison_id、回合(1–3)、規則、seed 與訂單數;key 為 `cmp:<comparison_id>:<round>`。 | COMP-05 | 7 |
| **FR-COMP-06** | 比較流程以 `/api/comparison/start`、`next`、`end`、`cancel` 控制三回合。start 參數 n_orders 介於 5–99、arrival_rate_per_min 上限 12.0、k ≥ 1.0;cancel 中止剩餘回合。 | COMP-06 | 7 |
