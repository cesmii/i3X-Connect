# I3X Viz — Claude Context

## Project Purpose
A self-contained, single-file web visualization (`index.html`) for conference presentations explaining how the I3X API works. Target display: **1920×1080 fullscreen browser**. Layout uses CSS `vw/vh` units throughout so it scales to any screen.

## What It Is
An animated, step-reveal explainer (Space / Click / → to advance, ← to go back one step).
- **Left**: 4 pluggable data source nodes (cyan)
- **Center**: I3X orb (the standard/API nexus)
- **Right**: 5 API method nodes (green)
- SVG bezier paths with `<animateMotion>` particles show data flow
- Orb ripple (`#orb-ripple` / `rippleOut` keyframe) fires at key narrative moments

## Source API Project
`~/Projects/API` — Python / FastAPI / Pydantic v2, RFC 001 from CESMII.
Abstract `I3XDataSource` base class; pluggable backends; VQT data format; SSE subscriptions.

## Design System
- **Background**: `#080b12` (dark navy)
- **Green**: `#00c40a` — method side
- **Cyan**: `#02bbee` — backend/source side
- **Fonts**: Josefin Sans (headings), Lato (labels), Roboto (body) — Google Fonts
- **Orb icon**: `https://raw.githubusercontent.com/ace-technologies-inc/i3X-Explorer/refs/heads/main/build/icon-1024.png`

## Layout (vw/vh units)
### Backend nodes (LEFT) — `.backend-node`
- `left:3vw`, `width:22vw`, `height:15vh` → right edge at **25vw**
- tops: `16, 33, 50, 67 vh` → centers: `23.5, 40.5, 57.5, 74.5 vh`

### Method nodes (RIGHT) — `.method-node`
- `right:3vw`, `width:22vw`, `height:11vh` → left edge at **75vw**
- tops: `18, 31, 44, 57, 70 vh` → centers: `23.5, 36.5, 49.5, 62.5, 75.5 vh`

### Orb
- CSS: `left:50%; top:41vh`
- JS: `ORB = { x: W*0.50, y: H*0.48 }`, `ORB_R = H*0.07`

### JS layout (computed in `computeLayout()`)
```js
const BX = W * 0.25;  // backend right edges
BACKENDS = {
  mqtt:   { x:BX, y:H*0.235 },
  mock:   { x:BX, y:H*0.405 },
  opcua:  { x:BX, y:H*0.575 },
  custom: { x:BX, y:H*0.745 },
}
const MX = W * 0.75;  // method left edges
METHODS = {
  explore:   { x:MX, y:H*0.235 },
  query:     { x:MX, y:H*0.365 },
  history:   { x:MX, y:H*0.495 },
  subscribe: { x:MX, y:H*0.625 },
  update:    { x:MX, y:H*0.755 },
}
```

## Routing Table
```js
const ROUTING = {
  explore:   'opcua',   // CESMII Profiles in step 5
  query:     'custom',  // Streaming Adapter in step 5
  history:   'mock',    // At Rest Data
  subscribe: 'mqtt',
  update:    'mqtt',
};
```
`buildRouteHighlights()` creates bright SVG beam groups (`routeGroups[method]`) for each entry, hidden by default.

## Backend Node HTML Structure
```html
<div class="backend-node" id="b-mqtt" style="top:16vh">
  <div class="backend-main">
    <div class="backend-icon">📡</div>
    <div class="backend-text"><h3>MQTT Broker</h3><p>…</p></div>
  </div>
  <div class="spec-tags" id="tags-mqtt">
    <span class="spec-tag">…</span>
  </div>
</div>
```
Column flex layout; tags are normal-flow children inside the box.

## Step Sequence (7 steps, index 0–6)

| Step | What happens |
|------|-------------|
| 0 | Orb glows, nothing else |
| 1 | Backend nodes slide in → backend→orb lines appear → orb **ripple** |
| 2 | Method nodes slide in → orb→method lines appear |
| 3 | MQTT-only scenario: dim all except MQTT (b-mqtt) and Subscribe/Update methods; bright routing beams for subscribe+update |
| 4 | At Rest Data (b-mock) un-dims → History (m-history) un-dims → routing beam mock→history |
| 5 | OPC UA → renames to **CESMII Profiles** (favicon-cesmii.png) → Explore lights up; then Your Platform → **Streaming Adapter** (favicon-litmus.ico) → Query lights up; **ripple** fires once after all sources lit |
| 6 | Product swap (all 4 backends swap to real products with favicons); callout appears; **ripple** fires once then every **15s** via `pulseInterval` |

## Orb Ripple Strategy
- Step 1: after all backend lines appear
- Step 5: once, after Streaming Adapter's routing beam fires (all sources now lit)
- Step 6: once 600ms after callout appears, then `setInterval(fireRipple, 15000)`
- `pulseInterval` is cleared in `UNDO[6]` when navigating back

## Favicon Files (in project root)
```
favicon-highbyte.png   → HighByte (b-mqtt in step 6)
favicon-inductive.png  → Ignition (b-mock in step 6)
favicon-timebase.ico   → Flow Timebase (b-opcua in step 6)
favicon-postgres.ico   → PostgreSQL (b-custom in step 6)
favicon-cesmii.png     → CESMII Profiles (b-opcua in step 5)
favicon-litmus.ico     → Streaming Adapter (b-custom in step 5)
```
All rendered via `IMG(src)` helper → `<img style="width:max(2.6vw,18px);height:max(2.6vw,18px);object-fit:contain">`.

## Mobile Support
- **Portrait**: `#rotate-prompt` overlay shown via `@media (orientation:portrait)` — animated SVG phone icon, hidden in landscape
- **Touch nav**: `touchend` handler — swipe left=advance, swipe right=retreat, tap=advance; `e.preventDefault()` suppresses synthetic click; click handler guards with 500ms timestamp
- **SVG resize**: `rebuildSVG()` snapshots group opacities, tears down and rebuilds all SVG paths for new viewport, restores state with `transition:none` then re-enables via `requestAnimationFrame`. Called on `window resize` with 150ms debounce.
- **Font minimums**: `@media (max-width:1024px)` block uses `max(Xvw, Ypx)` on all text so nothing shrinks below readable size on phone landscape

## Deployment
- This repo (`jwise-mfg/i3x-explainer`) is a **git submodule** inside `cesmii/i3x-dev-site` at path `viz/`
- After pushing here, bump the parent repo: `git submodule update --remote viz && git add viz && git commit && git push`
- Server update: `git pull --recurse-submodules` (or `git pull` + `git submodule update --init --recursive`)

## Key JS Objects / Functions
- `backendGroups` / `methodGroups` — SVG `<g>` elements for base (dim) lines+particles
- `routeGroups` — SVG `<g>` elements for bright routing beams, revealed per-step
- `originalBackends` — captured at load time (icon innerHTML, title, desc, tagsHTML) for undo of step 6
- `STEP5_NODES` — intermediate names/icons for step 5 (opcua → CESMII Profiles, custom → Streaming Adapter); used by `UNDO[6]` to restore step-5 state
- `PRODUCTS` — array of product definitions for step 6 swap
- `swapBackend(product, delay)` — fades node out, swaps content, fades back in
- `fireRipple()` — fires the orb ripple animation (restarts cleanly)
- `STEPS[n]()` — executes step n forward
- `UNDO[n]()` — reverses step n (returns to step n-1 state)
- `advance()` / `retreat()` — navigation; retreat clears `pulseInterval`

## CSS State Classes
| Class | Effect |
|-------|--------|
| `.visible` | Node slid in (opacity 1, translateX 0) |
| `.lit` | Cyan/green border glow (base connected state) |
| `.routed` | Bright white border + strong glow (active routing) |
| `.dimmed` | opacity 0.15 (used in steps 3–5 for non-active nodes) |
| `.swapping` | opacity 0 + slight translate (content-swap animation) |
| `.spec-tags.show` | Fade-in the capability tags inside a backend node |

## Navigation
- **Space / Click / →**: advance (or restart from step 6)
- **←**: retreat one step (calls `UNDO[currentStep]()`)
- **Swipe left / tap**: advance; **swipe right**: retreat
- Hint text at bottom updates contextually

## Technical Notes
- Fully self-contained single HTML file — no build tools, no npm
- SVG `<animateMotion>` + `<mpath xlink:href>` for particles (xlink namespace required)
- JS layout coordinates derived from `window.innerWidth/innerHeight` — recomputed on resize via `rebuildSVG()`
- Dim effect on SVG line groups: set `g.style.opacity = '0.04'` — particles become invisible
- Google Fonts require internet; system sans-serif fallback is acceptable offline
