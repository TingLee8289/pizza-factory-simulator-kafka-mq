import test from "node:test";
import assert from "node:assert/strict";
import { shippedLabel, TRUCK_LABEL_OPTS } from "../../src/pizza/dashboard/static/js/truck-label.js";

test("empty / undefined counts -> SHIPPED 0", () => {
  assert.equal(shippedLabel({}), "SHIPPED 0 lots / 0 pizzas");
  assert.equal(shippedLabel(undefined), "SHIPPED 0 lots / 0 pizzas");
});

test("uses completed only", () => {
  assert.equal(shippedLabel({ completed: 7, completed_pizzas: 15.9, failed: 3, bank: 2, wip: 5 }), "SHIPPED 7 lots / 15 pizzas");
});

test("invalid completed -> SHIPPED 0", () => {
  assert.equal(shippedLabel({ completed: -4 }), "SHIPPED 0 lots / 0 pizzas");
  assert.equal(shippedLabel({ completed: NaN }), "SHIPPED 0 lots / 0 pizzas");
  assert.equal(shippedLabel({ completed: "9" }), "SHIPPED 0 lots / 0 pizzas");
});

test("large count fits the canvas", () => {
  const t = shippedLabel({ completed: 9999, completed_pizzas: 99999 });
  assert.ok(t.length <= TRUCK_LABEL_OPTS.maxChars);
  assert.equal(t, "SHIPPED 9999 lots / 99999 pizzas");
  assert.ok(t.length * TRUCK_LABEL_OPTS.fontPx * 0.55 <= 240);
});
