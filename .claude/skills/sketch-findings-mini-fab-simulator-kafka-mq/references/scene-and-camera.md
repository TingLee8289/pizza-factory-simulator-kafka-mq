# 3D Scene & Camera

## Design Decisions

- **Camera: orthographic isometric.** `OrthographicCamera` with view height `VH = 36` world units (so a 16:9 canvas shows ~64 units across), azimuth `0.3` rad, elevation `0.62` rad, camera distance 80 (near -200, far 400). Perspective (variant C) looked fun but distorts the belt and was rejected.
- **Framing is a decision, not a detail.** Target `(tx=3, tz=-2.5)` places the whole 58-unit belt, intake gate and truck in frame at 1280x720 and keeps the line clear of the top KPI row and bottom panels. Verify framing again whenever HUD panels change size.
- **Line layout:** 7 stations along +x with `GAP = 6.2`; station `i` at `x = (i-3) * 6.2`. Belt at z = 0, top at y = 0.3. Intake gate at x = -27.5, shipping truck at x = 29.5.
- **Station = booth behind the belt:** machine block at z = -1.9 (2.6 wide, 3.4 for Bake), accent band on top, thin canopy over the belt, two slim posts at the front. Open front so pizzas on the belt are never hidden. Each station has a type-specific topper (dough drum, sauce tank, cheese hopper, toppings crates, Bake chimney + glowing door, slice blade, package box).
- **Status light** sphere on each station (green busy / grey idle / red blinking down), a floating thin progress bar, and an HTML label (name + queue badge).
- **Queue = pizzas lined up on the belt in front of the station.** Slot `k` for station `i`: `x = stX(i) - 2.0 - (k % 4) * 0.78`, `z = [0, .85, -.85][floor(k/4) % 3]`, extra height after 12. The queue badge turns red at 4+ lots, so the bottleneck is readable at a glance.
- **Pizza = group of stacked discs (qty)** that gains layers as the lot progresses: dough -> sauce -> cheese -> toppings -> baked colour -> slice lines -> cardboard box. Scale the group ~1.4x or it is unreadable at this zoom. Rush lots are slightly bigger.
- **Selection bracket:** `THREE.Box3Helper` around the selected group in the accent colour; hover uses a white `Box3Helper`. Cursor switches to pointer on hit.
- **Motion:** pizzas ease toward their target slot (`lerp(target, 1 - exp(-dt * 7 * speedFactor))`); belt texture scrolls with sim speed; Bake glow + point light only while processing; drum/blade spin only while busy.
- **Palette (A):** background `#dfe8f5`, ground `#eef3fb`, belt `#7c869c`, station body `#ffffff`, accent `#2f6bff`, light green `#12a46a`, down `#e5484d`.
- **Lighting:** hemisphere (white / `#b6c6e4`, 0.95) + one shadow-casting directional light (0.85), PCFSoft shadow map 2048, `fog` from 90 to 200.

## Camera rig (pattern)

```js
const cam = { az: .3, azT: .3, el: .62, zoom: 1, zoomT: 1, tx: 3, tz: -2.5, txT: 3, tzT: -2.5, VH: 36 };
function placeCam() {
  const d = 80; // ortho distance
  camera.position.set(cam.tx + d*Math.cos(cam.el)*Math.sin(cam.az), d*Math.sin(cam.el), cam.tz + d*Math.cos(cam.el)*Math.cos(cam.az));
  camera.lookAt(cam.tx, 0, cam.tz);
  camera.zoom = cam.zoom; camera.updateProjectionMatrix();
}
// every frame: ease az/zoom/tx/tz toward their *T targets (factor ~0.12-0.14)
```

- Left-drag pans (move target along camera right/forward vectors, clamp x to ±30, z to ±14), right-drag or the rotate buttons change azimuth, wheel zooms (0.6x to 3.2x).
- Picking: `Raycaster` against station groups and pizza groups (`intersectObjects(list, true)`), then walk `parent` until `userData.kind` is found. A click with < 5px movement selects; a drag never selects.

## What to Avoid

- **Perspective camera for the main view (variant C):** the near end of the belt looks huge and the far end shrinks, so the line does not read as one pipeline.
- **Letting the line run diagonally across the whole screen (az 0.5):** stations slide under the KPI cards and the bottom panels. Keep the line near-horizontal (az about 0.3) and shift the target so the belt sits in the free middle band.
- **Dark neon scene (variant B):** attractive but low contrast for pizzas and queue piles, and it hides the "calm engineering dashboard" feel the user wanted.
- **Closed tunnel stations:** pillars on both sides of the belt hid the pizzas from the camera; the open booth works.
- **Tiny pizzas:** at radius 0.34 they vanish; scale up or use highlight rings.

## Origin
Synthesized from sketches: 001
Source files available in: sources/001-factory-scene-style/
