# HUD Layout & Components

## Design Decisions

HUD is plain HTML/CSS over the canvas (`#hud { position:absolute; inset:0; pointer-events:none }`, each panel `pointer-events:auto`). It never lives inside WebGL.

| Component | Position (1280x720) | Content |
|---|---|---|
| Top bar | top 12, left/right 12, h 52 | brand, search (`/` focuses), rule switch FIFO/SPT/EDD, speed 1x/2x/4x and pause, arrival slider, `+ Rush lot`, Live badge with clock |
| KPI cards | top 76, left 12, row of 4 | Throughput (pizzas/min), WIP (lots), Cycle time (s/lot), Bottleneck (name + utilization) |
| Little's Law chip | top 140, left 12 | `L x vs λ·W y  Δ n%`, green under 15% |
| Inspector card | top 76, **right 64**, w 320 | station or lot details, Break/Repair, Focus/Follow, **✕ closes it** |
| Tool strip | top 76, **right 12** | zoom in/out, rotate left/right, home |
| Stepper | bottom 12, left 12, w 440 | 7-step lot tracking with check marks and times |
| Event feed | bottom center, w 300 | last 5 `station.events` lines |
| Tabbed list | bottom 12, right 12, w 360 | Stations / Lots tabs with status chips and progress bars |

- **Tool strip far right, inspector to its left** (final decision after review). Earlier the strip sat left of the inspector, which pushed scene content further under the cards.
- **Inspector is closable.** Its ✕ hides the panel entirely (the hint "Click a station or a lot" has the same ✕). It reappears the next time anything is selected. Implementation: a `hintOff` flag; `select(x)` clears it when `x` is truthy.
- **Initial state:** Bake is selected on load so the first screen already shows the bottleneck story.
- **Glass style (theme `default.css`):** `--color-surface: rgba(255,255,255,.78)`, `backdrop-filter: blur(14px)`, `--radius-md: 14px`, `1px` border `rgba(30,50,90,.10)`, shadow `0 8px 28px rgba(40,70,130,.14)`; text `#16213a`, muted `#6a7794`, primary `#2f6bff`, success `#12a46a`, warn `#e08a00`, danger `#e5484d`.
- **Chips:** pill, 0.72rem bold; `busy` green tint, `idle` grey, `down` red, `warn` amber.
- **Progress bars:** 7px, rounded, primary fill, `transition: width .3s`.
- **Station HTML labels** projected each frame (`v.project(camera)`), white pill with status dot + name + queue badge, outline when selected.
- **Numbers:** tabular (`font-variant-numeric: tabular-nums`) so values do not jiggle at 5 Hz updates.
- HUD is refreshed at 5 Hz (`setInterval`-like accumulator), the 3D at display rate.

## HTML Structure (skeleton)

```html
<div id="stage"><canvas id="gl"></canvas><div id="labels"></div>
  <div id="hud">
    <header id="topbar" class="panel">…brand · search · #controls · .live</header>
    <section id="kpis"><div class="kpi">…</div>×4</section><div id="little"></div>
    <section id="feed" class="panel"></section>
    <div id="tools" class="panel"><button>＋</button>…</div>
    <aside id="detail" class="panel"></aside>
    <section id="stepper" class="panel"></section>
    <section id="lists" class="panel"><div class="tabs">…</div><div class="list"></div></section>
  </div></div>
```

## What to Avoid

- **Variant B (night control room) HUD:** uppercase labels, neon outlines and a vertical KPI column were harder to scan and fought the 3D scene.
- **Variant C (game board) HUD:** thick outlines, centered KPI chips and a bottom action bar crowded the bottom corners; the action bar also collided with the stepper and list, and `position: fixed` inside an element with `backdrop-filter` breaks (it becomes relative to that element). Keep controls in the top bar.
- **Panels that cover the line:** tool strip and inspector on the right must not overlap the last station; re-check after any size change.
- **Empty inspector that cannot be dismissed.**
- **Narrow screens:** the prototype is desktop-only (≥ 1000 px). Below that it hides the feed and Little's Law chip and the sketch toolbar; a real mobile layout is not designed yet.

## Origin
Synthesized from sketches: 001
Source files available in: sources/001-factory-scene-style/, sources/themes/default.css
