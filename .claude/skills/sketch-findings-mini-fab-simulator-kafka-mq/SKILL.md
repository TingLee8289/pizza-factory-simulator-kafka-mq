---
name: sketch-findings-mini-fab-simulator-kafka-mq
description: Validated design decisions, CSS patterns, and visual direction from sketch experiments. Auto-loaded during UI implementation on mini-fab-simulator-kafka-mq (Phase 5 Three.js dashboard).
---

<context>
## Project: mini-fab-simulator-kafka-mq

Phase 5 dashboard for the pizza-factory simulator is an interactive **Three.js isometric low-poly 3D "digital twin"** of the 7-station line under a glassy 2D HUD, in the spirit of the WareTrack reference video, with a strategy-game feel. KPIs on screen must still tie back to IE theory (a Little's Law check is part of the HUD).

Reference points: WareTrack warehouse demo video (user's Downloads folder), Threads post "warehouse dashboard turned into a strategy game".

Stack constraints (CLAUDE.md): static HTML/JS served by FastAPI, SSE for live data, **no Node build**, vendored JS only.

Sketch sessions wrapped: 2026-10-15
</context>

<design_direction>
## Overall Direction

**Variant A "Light Digital Twin" won** (B "Night Control Room" and C "Pizza Kitchen Game" were explored and not chosen).

- **Scene:** pale blue-white world, white/blue low-poly stations, grey belt, mint trees and white boxes as decor, soft shadows. Orthographic camera, ~17° azimuth, ~35° elevation, whole line visible at 1280x720.
- **HUD:** frosted-glass cards (white 78%, 14px blur, 14px radius, 1px hairline), single blue accent `#2f6bff`, Inter-style sans, tabular numbers. Layout copies the video: KPI cards top-left, inspector top-right, stepper bottom-left, tabbed list bottom-right, tool strip at the far right.
- **Interaction:** click station or lot in 3D or in the list; blue bracket box + floating label on the selection; inspector card with live numbers; Break/Repair on stations; FIFO/SPT/EDD rule switch; arrival slider; speed 1x/2x/4x; rush lot; search; pan/rotate/zoom.
- **Refinements decided after review:** the tool strip (+ - rotate home) sits at the far right and the inspector card sits to its left (so the whole line, including the last station and truck, stays visible); the inspector, including its empty "Click a station or a lot" hint, has a close button that hides it until something is selected again.
</design_direction>

<findings_index>
## Design Areas

| Area | Reference | Key Decision |
|------|-----------|--------------|
| 3D scene & camera | references/scene-and-camera.md | Ortho isometric camera, low-poly stations behind a belt, queue slots in front, selection bracket, whole line in frame |
| HUD layout & components | references/hud-layout-and-components.md | Glass cards in video layout; tool strip far right, inspector to its left and closable |
| Data & interaction mapping | references/data-and-interaction.md | Event-driven scene, Little's Law chip, controls, SSE/Kafka mapping, ES-module note |

## Theme

The winning theme file is at `sources/themes/default.css` (HUD CSS variables). Rejected themes (`night.css`, `pizza.css`) are not included in this repo.

## Source Files

`sources/001-factory-scene-style/index.html` is the complete working prototype (all three variants, fake simulation, camera rig, picking, HUD). It loads `../vendor/three.min.js` (Three.js r150.1 UMD, kept in `sources/vendor/`).
</findings_index>

<metadata>
## Processed Sketches

- 001-factory-scene-style
</metadata>
