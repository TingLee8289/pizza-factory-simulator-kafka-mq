import test from "node:test";
import assert from "node:assert/strict";
import {
  postJson, errorReason, ApiError, getJson, breakStation, repairStation, bankLot, scrapLot, releaseLot, getFailureConfig, getLotDetail,
  startComparison, nextRound, endRound, cancelComparison, getComparison, getRuns, recomputeRun, csvUrl,
} from "../../src/pizza/dashboard/static/js/api.js";

function stubFetch(status, payload) {
  const calls = [];
  globalThis.fetch = async (path, init) => {
    calls.push({ path, init });
    return { ok: status >= 200 && status < 300, status, json: async () => payload };
  };
  return calls;
}

test("postJson sends JSON and returns parsed body", async () => {
  const calls = stubFetch(200, { ok: 1 });
  const out = await postJson("/api/rate", { arrival_rate_per_min: 2 });
  assert.deepEqual(out, { ok: 1 });
  assert.equal(calls[0].init.headers["Content-Type"], "application/json");
  assert.equal(calls[0].init.body, '{"arrival_rate_per_min":2}');
});

test("postJson throws ApiError with truncated plain detail", async () => {
  stubFetch(422, { detail: "<b>" + "x".repeat(500) + "</b>" });
  await assert.rejects(postJson("/api/order", {}), (err) => {
    assert.ok(err instanceof ApiError);
    assert.equal(err.status, 422);
    assert.ok(err.detail.length <= 200);
    assert.ok(!err.detail.includes("<"));
    return true;
  });
});

test("postJson maps network failure to status 0", async () => {
  globalThis.fetch = async () => { throw new TypeError("fail"); };
  await assert.rejects(postJson("/x", {}), (err) => err.status === 0);
});

test("errorReason maps statuses", () => {
  assert.equal(errorReason(new ApiError(409, "")), "No active run");
  assert.equal(errorReason(new ApiError(415, "")), "Request must be JSON");
  assert.equal(errorReason(new ApiError(503, "")), "Broker unavailable");
  assert.equal(errorReason(new ApiError(422, "qty too big")), "qty too big");
  assert.match(errorReason(new ApiError(0, "network")), /Cannot reach/);
});

test("failure helpers post to escaped paths", async () => {
  const calls = stubFetch(200, { ok: 1 });
  await breakStation("bake", 30);
  await breakStation("bake");
  await repairStation("bake");
  await bankLot("o 1", "capacity");
  await releaseLot("o-1");
  assert.equal(calls[0].path, "/api/station/bake/break");
  assert.equal(calls[0].init.body, '{"duration_s":30}');
  assert.equal(calls[1].init.body, "{}");
  assert.equal(calls[2].path, "/api/station/bake/repair");
  assert.equal(calls[3].path, "/api/lot/o%201/bank");
  assert.equal(calls[3].init.body, '{"reason":"capacity"}');
  assert.equal(calls[4].path, "/api/lot/o-1/release");
});

test("scrapLot posts the reason to the escaped scrap path", async () => {
  const calls = stubFetch(200, { accepted: true });
  await scrapLot("o 1", "quality");
  assert.equal(calls[0].path, "/api/lot/o%201/scrap");
  assert.equal(calls[0].init.method, "POST");
  assert.equal(calls[0].init.body, '{"reason":"quality"}');
});

test("getJson GETs same-origin and throws ApiError on non-2xx", async () => {
  const calls = stubFetch(200, { mtbf_s: 1 });
  assert.deepEqual(await getFailureConfig(), { mtbf_s: 1 });
  assert.equal(calls[0].path, "/api/failure-config");
  assert.equal(calls[0].init.method, "GET");
  stubFetch(500, { detail: "<i>bad</i>" });
  await assert.rejects(getJson("/x"), (err) => err instanceof ApiError && err.detail === "bad");
  assert.equal(errorReason(new ApiError(404, "")), "Unknown station or lot");
});

test("comparison client helpers hit the right method/path (COMP-01..04)", async () => {
  const calls = stubFetch(200, { state: "idle" });
  await startComparison({ n_orders: 30 });
  assert.equal(calls[0].path, "/api/comparison/start");
  assert.equal(calls[0].init.method, "POST");
  assert.equal(calls[0].init.body, '{"n_orders":30}');

  await nextRound();
  assert.equal(calls[1].path, "/api/comparison/next");
  assert.equal(calls[1].init.method, "POST");

  await endRound();
  assert.equal(calls[2].path, "/api/comparison/end");

  await cancelComparison();
  assert.equal(calls[3].path, "/api/comparison/cancel");

  await getComparison();
  assert.equal(calls[4].path, "/api/comparison");
  assert.equal(calls[4].init.method, "GET");

  await getRuns();
  assert.equal(calls[5].path, "/api/runs");

  await recomputeRun("r-20261020-010203-ab12");
  assert.equal(calls[6].path, "/api/runs/r-20261020-010203-ab12/recompute");
  assert.equal(calls[6].init.method, "POST");

  await recomputeRun("r with space");
  assert.equal(calls[7].path, "/api/runs/r%20with%20space/recompute");
});

test("getJson passes the abort signal and maps AbortError to ApiError(0, aborted)", async () => {
  const calls = stubFetch(200, { ok: 1 });
  const ctl = new AbortController();
  await getJson("/x", { signal: ctl.signal });
  assert.equal(calls[0].init.signal, ctl.signal);
  await getJson("/y");
  assert.equal(calls[1].init.signal, undefined);
  globalThis.fetch = async () => { const e = new Error("a"); e.name = "AbortError"; throw e; };
  await assert.rejects(getJson("/x", { signal: ctl.signal }), (err) => err instanceof ApiError && err.status === 0 && err.detail === "aborted");
  globalThis.fetch = async () => { throw new TypeError("fail"); };
  await assert.rejects(getJson("/x"), (err) => err.status === 0 && err.detail === "network");
});

test("csvUrl builds the comparison CSV path, with and without a comparison_id", () => {
  assert.equal(csvUrl(), "/api/comparison.csv");
  assert.equal(csvUrl(null), "/api/comparison.csv");
  assert.equal(csvUrl("c-ab 12"), "/api/comparison.csv?comparison_id=c-ab%2012");
});

test("getLotDetail requests the encoded lot URL", async () => {
  const calls = stubFetch(200, { order_id: "o 1", visits: [] });
  const out = await getLotDetail("o 1");
  assert.equal(calls[0].path, "/api/lot/o%201");
  assert.equal(out.order_id, "o 1");
});
