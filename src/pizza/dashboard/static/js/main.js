import { createStore } from "./store.js";
import { connect } from "./sse.js";
import * as hud from "./hud.js";
import * as charts from "./charts.js";

const store = createStore();
const ctx = { store, el: (id) => document.getElementById(id), document };

connect(store);
hud.init(ctx);
charts.init(ctx);

const OPTIONAL = ["controls", "speed", "orders-ui", "scene", "inspector", "panel-fit", "lists", "search", "feed", "compare"];

// Each optional module fails soft: a missing file or missing WebGL never breaks the HUD.
for (const name of OPTIONAL) {
  try {
    const mod = await import(`./${name}.js`);
    if (typeof mod.init === "function") await mod.init(ctx);
  } catch (err) {
    console.warn(`optional module ${name}.js unavailable:`, err);
  }
}
