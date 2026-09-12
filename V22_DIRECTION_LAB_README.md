# V2.2 Direction Lab — Research-only deployment

Version: `DIRECTION_LAB_V2.2.0`

## What this is

A separate Cloudflare Worker/Durable Object for directional-edge research. It does not migrate, modify, or call the existing V2.1.10 Worker.

It contains **no order endpoint, no paper trade engine, no TP/SL, no leverage, no fee model, and no ROI gate**.

## Data design

- Research feed: Binance USDⓈ-M perpetual `aggTrade` WebSocket.
- Universe: only symbols simultaneously active as BingX USDT perpetuals and Binance USDⓈ-M perpetuals.
- Aggressor side: `buyerMaker=true` is treated as seller-aggressor; `false` as buyer-aggressor.
- The old V2.1.10 24h rolling `quoteVolume` delta radar is not used.
- Flow is aggregated from actual received aggregate trades into 1-second buckets.
- Gated event window: one **completed** 5-second bucket.
- Baseline: preceding completed 5-second buckets; it ends exactly where the gated event window begins, so there is zero gate/baseline overlap.
- 10s and 30s flow windows are stored only as descriptive features; they are not repeated confirmation gates.
- One event maximum per symbol per 30 seconds.

## Time semantics

Every event records both:

- `marketEventAt`: exchange trade timestamp from the source message.
- `detectionAt`: local Worker time when the event became actionable.

The event's actionable entry/reference price is `detectionPrice`, the trade price available at detection. Forward labels begin from **detectionAt**, not from a backdated exchange timestamp.

Each forward label records:

- target horizon and target timestamp,
- first observed message at/after the target,
- `labelDelayMs`,
- raw future price,
- raw return percentage,
- raw direction: `UP`, `DOWN`, or `FLAT`,
- whether following or reversing the flow candidate was directionally correct.

There are no trading-cost or ROI calculations in label accuracy.

## Horizons

`10s`, `30s`, `1m`, `3m`, `5m`, `15m`.

## Session integrity

V2.2 has its own state epoch and does not restore V2.1.x state. A runtime restart creates a new `sessionId`. Short-horizon events interrupted by a restart are left incomplete and are excluded from `/summary` accuracy calculations.

## Deployment with Wrangler

Place these files in a fresh directory:

- `v22_direction_lab.js`
- `wrangler.toml` (rename `wrangler.v22.toml` to `wrangler.toml`)

Then deploy:

```bash
npx wrangler deploy
```

## Deployment in Cloudflare dashboard

Create a **new Worker**, not a new version of V2.1.10. Use the source in `v22_direction_lab.js`. Add a Durable Object binding:

- Variable name: `DIRECTION_LAB`
- Class: `DirectionLabStore`

Use a new Durable Object class/migration for this Worker. Do not point the binding to the V2.1.10 Durable Object namespace.

## Start and verify

After deployment:

1. Open `/start` once.
2. Open `/health`.
3. Verify:
   - `version = DIRECTION_LAB_V2.2.0`
   - `mode = RESEARCH_ONLY_NO_ORDERS_NO_PAPER_NO_ROI`
   - `running = true`
   - `websocket.status = OPEN`
   - `universeCount > 0`
   - `lastError = null`
4. After data begins accumulating, use `/events?limit=300` and `/summary`.

Do not interpret the first day's event count as proof of edge. Directional confirmation is done across independent UTC days/5-minute episodes and symbols after enough fresh data accumulates.
Deploy trigger
