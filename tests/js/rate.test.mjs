import test from "node:test";
import assert from "node:assert/strict";
import {
  PIZZAS_PER_LOT, toLotsPerMin, toPizzasPerMin, formatPizzaRate,
} from "../../src/pizza/dashboard/static/js/rate.js";

test("pizza rate converts to lots by E[qty] and back", () => {
  assert.equal(PIZZAS_PER_LOT, 2);
  assert.equal(toLotsPerMin(4), 2);
  assert.equal(toLotsPerMin(3), 1.5);
  assert.equal(toPizzasPerMin(1.5), 3);
  assert.equal(toPizzasPerMin(1.3), 3);
  assert.equal(formatPizzaRate(1), "2 pizzas/min");
});
