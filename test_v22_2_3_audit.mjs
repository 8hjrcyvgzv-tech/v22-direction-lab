import fs from 'node:fs';
import assert from 'node:assert/strict';

const path = new URL('./v22_direction_lab_v2_2_3.js', import.meta.url);
const src = fs.readFileSync(path, 'utf8');

function has(x, msg) { assert.ok(src.includes(x), msg); }
function lacks(x, msg) { assert.ok(!src.includes(x), msg); }

has('const VERSION = "DIRECTION_LAB_V2.2.3"', 'version');
has('const STATE_KEY = "v223:state"', 'fresh state namespace');
has('horizonsMs: [10_000, 30_000, 60_000, 180_000, 300_000, 900_000]', 'required horizons');
has('marketEventAt: triggerTrade.marketEventAt', 'market event timestamp lineage');
has('detectionAt,', 'detection timestamp');
has('sourceVersion: VERSION', 'sourceVersion lineage');
has('directionLabels: {}', 'direction labels separated');
has('returnMetrics: {}', 'return metrics separated');
has('evaluation: {}', 'evaluation separated');
lacks('ev.labels[', 'legacy mixed label object removed');
has('transportLatencyMs > CFG.maxMarketEventLagMs', 'stale timestamp guard');
has('transportLatencyMs < -CFG.maxFutureEventSkewMs', 'future timestamp guard');
has('duplicateTradeIdsDropped', 'provider-id dedupe');
has('noIdDuplicateCaveat', 'no-id duplicate uncertainty is explicit');
lacks('`${symbol}:${marketEventAt}:${price}:${qty}:${aggressorDir}`', 'unsafe synthetic trade dedupe removed');
has('!this.pendingEvents.has(id)', 'pending labels protected during retention trim');
has('GATED_5S_WINDOW_IS_COMPLETED_AND_NON_OVERLAPPING_WITH_BASELINE', 'non-overlap policy');
has('priceAcceptanceUsedAsGate: false', 'no extra gate introduced');
has('ordersEnabled: false', 'orders disabled');
has('paperTradingEnabled: false', 'paper disabled');
has('roiLogicPresent: false', 'ROI logic disabled');
lacks('fapi.binance.com', 'no Binance dependency');
lacks('/trade/order', 'no trading endpoint');
has('maxSubscriptionsPerConnection: 100', 'WS subscription cap');
has('maxWebSocketConnections: 8', 'WS connection cap');
has('subscribePaceMs: 25', 'subscription pacing');
has('trades.sort((a, b) => a.marketEventAt - b.marketEventAt)', 'batch trade ordering');
has('const latestTrade = trades[trades.length - 1]', 'latest batch trade used for detection/label');

// Independent retention simulation: pending ids must survive trim even when over cap.
function trim(index, pending, max) {
  let needRemove = index.length - max;
  const keep = [];
  const remove = [];
  for (const id of index) {
    if (needRemove > 0 && !pending.has(id)) { remove.push(id); needRemove--; }
    else keep.push(id);
  }
  return { keep, remove, needRemove };
}
const t = trim(['c1','p1','c2','p2','c3'], new Set(['p1','p2']), 3);
assert.deepEqual(t.keep, ['p1','p2','c3']);
assert.deepEqual(t.remove, ['c1','c2']);

console.log('V2.2.3 static/self-audit tests: PASS');
