# V2.2.3 Direction Lab — self-audit and handoff

## Scope
Research-only crypto direction engine. No real orders, no paper trades, no TP/SL, no leverage, no fee/funding/slippage gates, and no ROI logic. V2.1.10 remains untouched.

## Live evidence from V2.2.2 before this patch
The BingX-native V2.2.2 runtime was healthy after start: 766-symbol universe, 8/8 WebSocket connections open, 1,342,017 received messages, 2,858,571 parsed aggTrades, 1,560 detected events, 299 fully labeled events, and no runtime error. This live run exposed correctness issues that would have biased longer research if left unchanged.

## Audit findings and V2.2.3 fixes

### 1. Forward-label retention contamination — FIXED
V2.2.2 retained only 1,000 indexed events while the live run already had 1,261 events still waiting for forward labels. Because 15m labels had not completed yet, FIFO eviction could delete an event before its full label set was finished, then later re-write it without re-indexing it. This could bias `/events` and `/summary` toward newer samples.

V2.2.3 protects every pending event from retention eviction. The nominal raw index cap is raised to 5,000 with trim slack. If the cap is exceeded and only pending events remain removable, the index is allowed to exceed the cap rather than lose a forward label.

### 2. Unsafe duplicate suppression — FIXED
The documented BingX swap trade payload exposes T/s/p/q/m and does not guarantee a unique trade id. V2.2.2 synthesized a duplicate key from timestamp/price/quantity/side when no id existed. Two legitimate identical fills could therefore be collapsed.

V2.2.3 never synthesizes a trade id for dedupe. Provider-id dedupe is used only if an actual id field is present. For no-id rows, the engine preserves the data and explicitly reports the caveat rather than deleting potentially real trades.

### 3. Timestamp leakage / stale-message contamination — FIXED
V2.2.2 bucketed trades by local receive time without rejecting delayed exchange timestamps. A delayed trade could therefore enter the current 5s bucket and contaminate a current event.

V2.2.3 records both `marketEventAt` and `detectionAt` and rejects rows more than 5s late or more than 2s implausibly in the future. Dropped stale/future rows are counted separately.

### 4. Batched trade ordering / label price contamination — FIXED
A BingX push may contain multiple trade rows. V2.2.3 groups rows by symbol, sorts them by exchange timestamp for deterministic first/last bucket prices, and uses the newest observed trade in that push for detection price and any forward label reached at that receive timestamp.

### 5. Direction-label vs return-metric contamination — FIXED
V2.2.2 stored `rawDirection`, `rawReturnPct`, and evaluation fields together inside one label object.

V2.2.3 stores them in separate structures:
- `directionLabels` — raw UP/DOWN/FLAT label and timing only.
- `returnMetrics` — raw price return and observed/detection prices only; explicitly no fees/funding/slippage/leverage.
- `evaluation` — follow/reverse correctness helpers only.

`/summary` reads direction labels plus evaluation and does not use return metrics as a direction label.

### 6. Stale state migration — FIXED/ISOLATED
V2.2.3 uses fresh `v223:*` Durable Object state keys and the object name `v223:lab`. It does not migrate V2.2.2 state and cannot touch V2.1.10 state.

### 7. Confirmation-window overlap — AUDITED
The gated event window is a completed 5s window. Its volume baseline ends exactly where that gated window begins. The 10s/30s features remain descriptive only and are explicitly marked as such. Per-symbol event purge remains 30s.

### 8. Rate-limit / connection behavior — AUDITED
The engine keeps the conservative configuration of at most 100 subscriptions per WebSocket, at most 8 WebSockets, and 25ms pacing between subscriptions. The V2.2.2 live run reached 8/8 open connections with no `lastError`, supporting the current connection layout. V2.2.3 does not add extra market-data connections.

## Required forward labels
Exactly: 10s, 30s, 1m, 3m, 5m, 15m from `detectionAt`.

## Data lineage
Universe: BingX public `/openApi/swap/v2/quote/contracts`.
Trades: BingX public `{symbol}@trade` WebSocket.
No Binance dependency.

## Self-tests
- `node --check v22_direction_lab_v2_2_3.js` — PASS.
- `node test_v22_2_3_audit.mjs` — PASS.
- Static audit checks: version/state isolation, exact horizons, timestamp guards, pending-safe retention, separated direction/return/evaluation data, unsafe synthetic dedupe absent, BingX-only feed, overlap policy present, no trading endpoint, no paper/ROI execution logic — PASS.

## Deployment acceptance checks
After deployment:
1. `/health` must show `DIRECTION_LAB_V2.2.3`, `running: false`, `startStage: IDLE` on first fresh load.
2. Open `/start` once.
3. After 15–30 seconds `/health` should show `running: true`, `startStage: RUNNING`, `universeCount > 0`, WebSocket status OPEN, `openConnections == expectedConnections`, `messages > 0`, `aggTrades > 0`, and `lastError: null`.
4. After at least 16 minutes, `/summary` should have fully completed 15m samples.
