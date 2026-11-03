# 下貨 ETA 模型(Phase 07.1)

下貨當下,系統根據瓶頸與目前 WIP 排隊狀況逐站估算這張工單的完工時間(ETA),把結果凍結成 Kafka 事實,並用它預設 `due_ts`。對應需求 ORDER-06、ORDER-07、DASH-05、KPI-08。

## 1. 遞迴式(D-01)

純函式位於 `src/pizza/scheduler/eta.py`,所有數值為模擬秒,相對於「現在」:

```
finish[s] = max(finish[s-1], free_at[s]) + proc[s]      finish[-1] = 0
```

- `proc[s]` = 該站即時 recipe 單位時間 x `qty`。
- `free_at[s]` = 站點可開始這張 lot 的時間 = 修復剩餘時間 + 正在加工 lot 的剩餘時間 + 排在這張 lot 前面的 lot 的加工時間總和。
- 模擬秒轉牆鐘秒只在轉接層(`src/pizza/scheduler/estimate.py`)除以 timescale,遞迴本身不碰時間單位。
- 每個 offset 夾在 7 天(`MAX_ETA_SIM_S`)以內,非有限值視為壞輸入並標記 `degraded`。

## 2. 沿用派工比較器(D-02)

「排在前面」由派工用的同一個 `ordering_key` 決定:只有 key 嚴格小於新 lot 的 lot 才計入該站的等待。沒有第二套比較器,所以切換 FIFO/EDD/SPT 時估算跟著改變。新工單的 id 排序在所有既有 lot 之後,同 key 時既有 lot 先行。

## 3. 故障與降級(D-03, PD-3)

- 站點回報 down 且知道 `repair_until` 時,把剩餘修復時間加進該站的 `free_at`。
- 站點 offline、未回報狀態、down 但不知修復時間,或遇到非有限 recipe 時,估算仍然回傳,但 `degraded = true`。
- bank 中、failed、completed 的 lot 不計入 WIP。

## 4. 凍結的 ETA 是事實(D-04)

`eta_ts` 與 `station_eta`(站名到時間)是 `order.submitted` 與 `order.lifecycle`(created、released)上的選填欄位,下貨時計算一次,之後不再改寫。因此:

- 重播 Kafka 或重建排程器、儀表板狀態時,得到完全相同的初始 ETA。
- 缺欄位的舊事件仍可解析;估算失敗時下單照常,只是不帶這兩個欄位。
- ETA 誤差(KPI-08)為 `completed_ts - eta_ts`,由事實純函式計算(`src/pizza/kpi/eta.py`)。

## 5. 目前預估不是事實(PD-5)

「Current estimate」會隨 WIP 變動而每秒不同,若寫進 Kafka 會產生大量無重播價值的事件。因此儀表板在背景輪詢 `scheduler.estimate_wip`,把結果放在揮發的記憶體快取,併入 SSE 推送。重啟後快取為空,下一輪輪詢即補回。

## 6. RPC 契約

兩個命令走 RabbitMQ 控制通道(指令,見 `docs/messaging-roles.md`),以 reply-to 與 correlation id 回覆;請求帶 `deadline_ts`,逾期的請求被丟棄。

| 命令 | 請求欄位 | 成功回覆 |
| --- | --- | --- |
| `scheduler.estimate_eta` | `product`、`qty`、`priority`、`due_ts?`、`buffer_k`、`deadline_ts` | `{ok:true, eta_lead_s, station_lead_s, degraded}` |
| `scheduler.estimate_wip` | `deadline_ts` | `{ok:true, eta}`(在製 lot 到預估完成時間,最多 200 筆,依 `(entered_ts, order_id)`) |

失敗回覆為 `{ok:false, code, detail}`,code 為 `no_run`(沒有符合的執行中 run)、`expired`(已過 `deadline_ts`)、`invalid`(未知品項、非有限 recipe)。儀表板端點 `GET /api/order/estimate` 在逾時或排程器不可用時回應 HTTP 422,回覆欄位 `eta_ts`、`station_eta`、`due_ts`、`lead_s`、`buffer_k`、`degraded`。估算 RPC 唯讀,不建立工單、不發佈任何生命週期事實。

## 7. due 公式與備援(D-08, PD-1, PD-7)

```
due_ts = created_ts + eta_lead_s x buffer_k
```

- `buffer_k` 預設 1.0,手動單範圍 0.5 到 3,generator 範圍 0.5 到 10。
- 手動單:表單以 dry-run 預填 Due 並標示 Auto;使用者輸入後變 Manual,明確給定的 `due_ts` 優先。提交時由伺服器重算並凍結 ETA,所以表單上的「Estimated」與最終凍結值可能差幾秒。
- 自動單:`due_mode=eta`(預設)時 generator 於發佈前呼叫估算 RPC,帶入 `due_ts`、`eta_ts`、`station_eta`。
- 自動單的 `created_ts` 是實際送出當下的 UTC 牆鐘,不是計畫時間(t0 + 到達偏移);計畫偏移只負責節奏。主機睡眠時容器內的 monotonic 排程時鐘會停住,若以計畫時間為錨點,醒來後每張單都會一出生就 Late。實際送出與計畫時間(含 pause、倍速補償)的差記在 `order.lifecycle` created 事實的選填欄位 `release_lag_s`,單次跳升超過 `RELEASE_LAG_WARN_S`(預設 5 秒)時 generator 記錄 `generator_release_lag` 警告。
- 估算 RPC 的整體逾時涵蓋 reply queue 建立、交換器取得、發佈與等待回覆,broker 無聲卡住時拋 `RpcTimeout`,generator 退回理論 due。
- 備援:`due_mode=theory`,或 RPC 逾時、忙碌、回覆驗證失敗時,使用原本的理論 due(`k`、`k_max` 與理論總加工時間),不帶 ETA 欄位,下單不受影響。
- 有效 due:`due_ts` 事件本身不可變;暫停期間(手動或睡眠自動暫停)儀表板與 KPI 以「有效 due」判定遲交,有效 due = `due_ts` + 建單後重疊的暫停秒數,tardiness = active(created, completed) − 無暫停扣除的 active(created, due)。

## 8. D-09 取捨

規則比較的每一輪共用 seed、到達序列、品項與數量,但 `due_ts` 現在由 ETA 推得,而 ETA 取決於各規則造成的壅塞,所以各輪的 `due_ts` 不再相同。結果是遲交相關指標(延遲張數、延遲比例、tardiness)同時反映規則對派工順序與對 due 的影響。

- `plan_orders` 的決定性不變:同 seed 的品項、數量、到達偏移與理論 due 偏移完全一致,由 `tests/unit/test_eta_pipeline.py` 的 golden 測試守住。
- `RunParams.due_mode="theory"` 還原各輪完全相同的 due 工作負載,需要嚴格同工作負載比較時使用。
- `scripts/rule_oracle.py` 維持以理論 due 計算,不受影響。
- 比較測試中原本假設各輪 `due_ts` 相同的斷言,改為斷言各輪的 seed 衍生輸入(張數、品項、數量、到達偏移)相同。
