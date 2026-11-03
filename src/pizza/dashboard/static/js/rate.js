// Arrival rate is entered and shown in whole pizzas/min; the API and the
// generator take lots/min. The generator draws qty uniformly in
// [QTY_MIN, QTY_MAX] = [1, 3] (pizza/common/constants.py), so E[qty] = 2 and
// the pizza rate is an expected value, not an exact count.
export const PIZZAS_PER_LOT = 2;
export const MIN_PIZZA_RATE = 1;
export const MAX_PIZZA_RATE = 12;

export const toLotsPerMin = (pizzasPerMin) => pizzasPerMin / PIZZAS_PER_LOT;
export const toPizzasPerMin = (lotsPerMin) => Math.round(lotsPerMin * PIZZAS_PER_LOT);
export const formatPizzaRate = (lotsPerMin) => `${toPizzasPerMin(lotsPerMin)} pizzas/min`;
