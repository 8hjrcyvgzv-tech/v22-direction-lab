# V2.2 Direction Lab — self-audit report

Audit target: `DIRECTION_LAB_V2.2.0`

## PASS — trading isolation

- No BingX/Binance order endpoint exists in the source.
- No real-order function exists.
- No paper-position state exists.
- No TP/SL engine exists.
- No leverage/fee/ROI calculation is used to create or score directional labels.

## PASS — rolling-volume bug removed

V2.2 does not derive short-window volume from 24h ticker `quoteVolume` differences. It consumes actual Binance USDⓈ-M `aggTrade` messages and sums trade notional in short receive-time buckets.

## PASS — actionable timestamping

Each event stores `marketEventAt` separately from `detectionAt`. Forward targets are `detectionAt + horizon`, and labels are assigned only by a market message received at or after that target. `labelDelayMs` is stored.

## PASS — no stale state migration

Only exact `DIRECTION_LAB_V2.2.0` / schema version 1 state is restored. No V2.1.x epoch is referenced. A restart creates a fresh session boundary.

## PASS — overlap control

The gated 5s event window is a completed bucket. The baseline ends exactly at the start of that 5s window. Deterministic unit test verified zero overlap. A 30s per-symbol purge prevents repeated events from the same immediate impulse. 10s/30s features are descriptive rather than confirmation gates.

## PASS — duplicate handling

Binance aggregate-trade IDs are tracked per symbol. Duplicate or out-of-order replay IDs are dropped within a session. Reconnect count and session ID are exposed in `/health`.

## PASS — direction/return separation

Raw future direction (`UP`/`DOWN`/`FLAT`) is stored separately from raw percentage return. Follow/reverse correctness is calculated from raw price sign only. FLAT is never counted as correct. No cost-adjusted win-rate metric exists.

## PASS — label contamination guard

`/summary` uses only fully completed events. Session-interrupted partial events cannot enter accuracy summaries.

## PASS — scalability improvements

Raw trades are not retained for minutes. Trades are aggregated into 1-second buckets for feature computation; only the pending forward label logic sees each incoming trade price. Pending events are indexed by symbol, avoiding scans over unrelated symbols.

## Known operational limitations (not methodological leakage)

1. Cloudflare outbound WebSocket runtime availability and Durable Object billing/quota depend on the user's Cloudflare plan/runtime settings.
2. Binance WebSocket connections are expected to disconnect periodically (including the documented 24-hour connection lifetime); V2.2 reconnects and records reconnect/session lineage.
3. `detectionAt` uses the Cloudflare runtime clock while `marketEventAt` uses Binance's exchange clock. Their difference is recorded as latency, not assumed to be perfectly synchronized.
4. The first deployment must be treated as a fresh-data boundary; historical V2.1.10 observations are not imported.

## Tests executed

- `node --check v22_direction_lab.js` — PASS.
- Banned order-endpoint/source scan — PASS.
- Old V2.1 state-epoch scan — PASS.
- Deterministic bucket/baseline test — PASS: event 5s quote=2000, first five baseline buckets=500 each with no overlap.
- Direction-label sign test (`UP`, `DOWN`, `FLAT`) — PASS.
