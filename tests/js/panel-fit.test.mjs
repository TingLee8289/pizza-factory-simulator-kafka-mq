import test from "node:test";
import assert from "node:assert/strict";
import { panelHeight, MIN_PANEL_PX } from "../../src/pizza/dashboard/static/js/panel-fit.js";

test("panelHeight: no inspector keeps the 40vh default", () => {
  assert.equal(panelHeight(900, 0), 360);
});

test("panelHeight: short inspector leaves the panel at 40vh", () => {
  assert.equal(panelHeight(900, 355), 360);
});

test("panelHeight: tall inspector shrinks the panel to fit it", () => {
  // 900 - 76 - 24 - 540 = 260
  assert.equal(panelHeight(900, 540), 260);
});

test("panelHeight: never below the minimum", () => {
  assert.equal(panelHeight(700, 600), MIN_PANEL_PX);
});
