import test from "node:test";
import assert from "node:assert/strict";
import { feedLine, createFeedMemory, pushFeed, FEED_CAP } from "../../src/pizza/dashboard/static/js/feed.js";

const ts = "2026-01-01T10:00:05Z";
const f = (o) => ({ ts, station: "bake", order_id: "o-1", ...o });

test("failure-path lines", () => {
  assert.match(feedLine(f({ kind: "op_failed" })), /op_failed o-1 at bake$/);
  assert.match(feedLine(f({ kind: "op_dispatched", visit: 2 })), /rework -> bake \(visit 2\)$/);
  assert.match(feedLine(f({ kind: "order_lifecycle", state: "failed" })), /-> DLQ o-1$/);
  assert.match(feedLine(f({ kind: "op_started", redelivered: true })), /redelivered o-1 at bake$/);
  assert.equal(feedLine(f({ kind: "op_started" })), "10:00:05  bake  started  o-1");
});

test("status lines with UP memory", () => {
  const m = createFeedMemory();
  assert.match(feedLine(f({ kind: "station_status", status: "down" }), m), /bake DOWN$/);
  assert.match(feedLine(f({ kind: "station_status", status: "idle" }), m), /bake UP$/);
  assert.match(feedLine(f({ kind: "station_status", status: "busy" }), m), /status busy/);
  assert.match(feedLine(f({ kind: "station_status", status: "offline" }), m), /bake OFFLINE$/);
  assert.match(feedLine(f({ kind: "station_status", status: "busy" }), m), /bake UP$/);
});

test("stocker, bank, release lines", () => {
  assert.match(feedLine(f({ kind: "lot_stocked", stock_kind: "overflow" })), /STOCKER o-1 \(queue full\)$/);
  assert.match(feedLine(f({ kind: "lot_stocked", stock_kind: "station_down" })), /STOCKER o-1 \(bake down\)$/);
  assert.match(feedLine(f({ kind: "lot_stocked", stock_kind: "station_offline" })), /STOCKER o-1 \(bake offline\)$/);
  assert.match(feedLine(f({ kind: "lot_banked", reason: "capacity", source: "auto_down" })), /BANK o-1 \(capacity, auto\)$/);
  assert.match(feedLine(f({ kind: "lot_banked", reason: "customer", source: "manual" })), /BANK o-1 \(customer, manual\)$/);
  assert.match(feedLine(f({ kind: "lot_released" })), /RELEASE o-1 -> bake$/);
  assert.equal(feedLine(f({ kind: "heartbeat" })), "");
});

test("rule_changed line (Phase 7 SCHED-03)", () => {
  assert.match(feedLine(f({ kind: "rule_changed", rule: "spt" })), /RULE SPT$/);
  assert.match(feedLine(f({ kind: "rule_changed", rule: "edd" })), /RULE EDD$/);
});

test("feed cap", () => {
  let lines = [];
  for (let i = 0; i < 9; i += 1) lines = pushFeed(lines, `l${i}`);
  assert.equal(lines.length, FEED_CAP);
});

test("op_failed breakdown hint", () => {
  assert.match(feedLine(f({ kind: "op_failed", cause: "breakdown" })), /op_failed o-1 at bake \(breakdown\)$/);
  assert.match(feedLine(f({ kind: "op_failed", cause: "quality" })), /op_failed o-1 at bake$/);
});
