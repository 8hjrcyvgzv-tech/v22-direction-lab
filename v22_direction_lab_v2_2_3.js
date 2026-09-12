import { DurableObject } from "cloudflare:workers";

/**
 * V2.2 DIRECTION LAB — RESEARCH ONLY
 * -----------------------------------
 * Purpose: collect clean, actionable short-horizon direction labels.
 * NO orders. NO paper trading. NO TP/SL. NO leverage. NO fees. NO ROI gates.
 *
 * Data source: BingX USDT-M perpetual real-time trade WebSocket.
 * Universe and trade stream both come from BingX public market data.
 * This avoids Binance REST/WS dependency and keeps V2.2 research isolated.
 */

const VERSION = "DIRECTION_LAB_V2.2.3";
const SCHEMA_VERSION = 1;
const STATE_KEY = "v223:state";
const INDEX_KEY = "v223:eventIndex";
const EVENT_PREFIX = "v223:event:";

const BINGX_REST = "https://open-api.bingx.com";
const BINGX_WS = "wss://open-api-swap.bingx.com/swap-market";

const CFG = Object.freeze({
  // Research windows
  horizonsMs: [10_000, 30_000, 60_000, 180_000, 300_000, 900_000],
  featureWindowsMs: [5_000, 10_000, 30_000],
  baselineBucketMs: 5_000,
  baselineBuckets: 24,               // 2 minutes of completed 5s buckets
  minBaselineBuckets: 12,
  bucketKeepMs: 3 * 60_000,

  // Frozen event gate. Price acceptance is intentionally NOT a gate.
  minFlowVsMedianX: 2.0,
  minAbsImbalance5s: 0.30,
  minAggTrades5s: 5,
  minTopSideShare5s: 0.55,

  // Independence / overlap control
  perSymbolPurgeMs: 30_000,
  episodeMs: 5 * 60_000,

  // Retention. Never evict an event that is still waiting for a forward label.
  // 5,000 is a browsing/raw-retention cap, not a label cap; pending events are protected.
  maxEvents: 5_000,
  retentionTrimSlack: 250,
  exportDefaultLimit: 500,
  exportMaxLimit: 5_000,

  // Data-integrity windows
  maxMarketEventLagMs: 5_000,
  maxFutureEventSkewMs: 2_000,
  tradeIdDedupeKeepMs: 15_000,

  // Connection/persistence
  alarmMs: 30_000,
  websocketReconnectMs: 5_000,
  maxSubscriptionsPerConnection: 100, // conservative cap for swap WS stability
  maxWebSocketConnections: 8,
  subscribePaceMs: 25,
});

function j(v, status = 200) {
  return new Response(JSON.stringify(v, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });
}
function n(v, d = 0) { const x = Number(v); return Number.isFinite(x) ? x : d; }
function median(values) {
  const a = values.filter(Number.isFinite).sort((x, y) => x - y);
  if (!a.length) return 0;
  const m = Math.floor(a.length / 2);
  return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2;
}
function pct(a, b) { return a > 0 && b > 0 ? ((b / a) - 1) * 100 : null; }
function signDirection(ret) { return ret > 0 ? "UP" : ret < 0 ? "DOWN" : "FLAT"; }
function sideName(dir) { return dir > 0 ? "LONG" : "SHORT"; }
function horizonName(ms) { return ms < 60_000 ? `${ms / 1000}s` : `${ms / 60_000}m`; }
function episodeId(ts) { return `E5M-${Math.floor(ts / CFG.episodeMs)}`; }
function utcDay(ts) { return new Date(ts).toISOString().slice(0, 10); }
function uid() { return `${VERSION}-${Date.now().toString(36)}-${crypto.randomUUID().slice(0, 8)}`; }
function eventKey(id) { return `${EVENT_PREFIX}${id}`; }
function safeIso(ts) { return ts ? new Date(ts).toISOString() : null; }

async function fetchJson(url) {
  const r = await fetch(url, {
    headers: {
      accept: "application/json",
      "user-agent": "direction-lab-v2.2",
      "X-SOURCE-KEY": "BX-AI-SKILL",
    },
  });
  const body = await r.json().catch(() => null);
  if (!r.ok) throw new Error(`${url} HTTP ${r.status}`);
  return body;
}

async function fetchResearchUniverse() {
  const bx = await fetchJson(`${BINGX_REST}/openApi/swap/v2/quote/contracts?timestamp=${Date.now()}`);
  const bxRows = Array.isArray(bx?.data) ? bx.data : Array.isArray(bx) ? bx : [];
  const symbols = bxRows
    .filter(x => String(x?.symbol || "").endsWith("-USDT"))
    .filter(x => x?.status === 1 || x?.status === "1" || x?.status == null)
    .filter(x => String(x?.apiStateOpen ?? "true") !== "false")
    .map(x => String(x.symbol).toUpperCase());

  const unique = [...new Set(symbols)].sort();
  if (!unique.length) throw new Error("No active BingX USDT perpetual symbols");

  const maxUniverse = CFG.maxSubscriptionsPerConnection * CFG.maxWebSocketConnections;
  if (unique.length > maxUniverse) {
    throw new Error(`Universe ${unique.length} exceeds configured WS safety cap ${maxUniverse}`);
  }
  return unique;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function decodeBingXFrame(data) {
  if (typeof data === "string") return data;

  let bytes;
  if (data instanceof ArrayBuffer) bytes = new Uint8Array(data);
  else if (ArrayBuffer.isView(data)) bytes = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  else if (data instanceof Blob) bytes = new Uint8Array(await data.arrayBuffer());
  else return String(data ?? "");

  try {
    const ds = new DecompressionStream("gzip");
    const stream = new Blob([bytes]).stream().pipeThrough(ds);
    return await new Response(stream).text();
  } catch {
    return new TextDecoder().decode(bytes);
  }
}

function chunkArray(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

function updateSecondBucket(series, trade) {
  const sec = Math.floor(trade.receivedAt / 1000) * 1000;
  let b = series.get(sec);
  if (!b) {
    b = { sec, buyQuote: 0, sellQuote: 0, count: 0, firstPrice: trade.price, lastPrice: trade.price, topTrades: [] };
    series.set(sec, b);
  }
  if (trade.aggressorDir > 0) b.buyQuote += trade.quote; else b.sellQuote += trade.quote;
  b.count++;
  b.lastPrice = trade.price;
  b.topTrades.push({ quote: trade.quote, aggressorDir: trade.aggressorDir });
  b.topTrades.sort((a, z) => z.quote - a.quote);
  if (b.topTrades.length > 10) b.topTrades.length = 10;
}

function pruneSecondBuckets(series, now) {
  const cutoff = now - CFG.bucketKeepMs;
  for (const k of series.keys()) if (k < cutoff) series.delete(k);
}

function aggregateBucketsCompleted(series, endExclusive, windowMs) {
  const start = endExclusive - windowMs;
  const buckets = [...series.values()].filter(b => b.sec >= start && b.sec < endExclusive).sort((a, b) => a.sec - b.sec);
  if (!buckets.length) return { count: 0, quote: 0, buyQuote: 0, sellQuote: 0, imbalance: 0, firstPrice: null, lastPrice: null, priceReturnPct: null, topSideShare: 0.5 };
  let buyQuote = 0, sellQuote = 0, count = 0;
  const tops = [];
  for (const b of buckets) {
    buyQuote += b.buyQuote; sellQuote += b.sellQuote; count += b.count;
    tops.push(...b.topTrades);
  }
  const quote = buyQuote + sellQuote;
  const imbalance = quote > 0 ? (buyQuote - sellQuote) / quote : 0;
  const dir = imbalance >= 0 ? 1 : -1;
  tops.sort((a, b) => b.quote - a.quote);
  let topSame = 0, topTotal = 0;
  for (const t of tops.slice(0, 10)) { topTotal += t.quote; if (t.aggressorDir === dir) topSame += t.quote; }
  const firstPrice = buckets[0].firstPrice, lastPrice = buckets[buckets.length - 1].lastPrice;
  return {
    count, quote, buyQuote, sellQuote, imbalance, firstPrice, lastPrice,
    priceReturnPct: pct(firstPrice, lastPrice),
    topSideShare: topTotal > 0 ? topSame / topTotal : 0.5,
  };
}

function completedBaselineQuotes(series, eventWindowStart) {
  const out = [];
  const bucket = CFG.baselineBucketMs;
  const oneSec = [...series.values()];
  // Baseline ends exactly where the gated event window begins: zero overlap.
  for (let i = 1; i <= CFG.baselineBuckets; i++) {
    const end = eventWindowStart - (i - 1) * bucket;
    const start = end - bucket;
    let q = 0;
    for (const b of oneSec) if (b.sec >= start && b.sec < end) q += b.buyQuote + b.sellQuote;
    out.push(q);
  }
  return out;
}

export class DirectionLabStore extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.env = env;
    this.running = false;
    this.sessionId = null;
    this.startedAt = 0;
    this.universe = [];
    this.wsConnections = [];
    this.wsStatus = "DISCONNECTED";
    this.wsOpenedAt = 0;
    this.wsLastMessageAt = 0;
    this.wsReconnects = 0;
    this.wsExpectedConnections = 0;
    this.wsOpenConnections = 0;
    this.lastError = null;
    this.startStage = "IDLE";
    this.bucketSeries = new Map();
    this.lastFeatureCutoff = new Map();
    this.lastEventAt = new Map();
    this.recentTradeKeys = new Map();
    this.pendingEvents = new Map();
    this.pendingBySymbol = new Map();
    this.eventIndex = [];
    this.stats = {
      messages: 0,
      aggTrades: 0,
      events: 0,
      labeled: 0,
      duplicatesDropped: 0,
      duplicateTradeIdsDropped: 0,
      purged: 0,
      staleMessages: 0,
      futureTimestampMessages: 0,
      retentionProtectedPending: 0,
    };

    this.ctx.blockConcurrencyWhile(async () => {
      const s = await this.ctx.storage.get(STATE_KEY);
      const idx = await this.ctx.storage.get(INDEX_KEY);
      // No migration from V2.1.x or any prior state. Exact version/schema only.
      if (s?.version === VERSION && s?.schemaVersion === SCHEMA_VERSION) {
        this.running = !!s.running;
        this.sessionId = s.sessionId || null;
        this.startedAt = n(s.startedAt);
        this.universe = Array.isArray(s.universe) ? s.universe : [];
        this.wsReconnects = n(s.wsReconnects);
        this.stats = { ...this.stats, ...(s.stats || {}) };
      }
      this.eventIndex = Array.isArray(idx) ? idx : [];
      // A runtime restart creates a clean session boundary. Pending events from the previous
      // isolate are intentionally not resumed because their short-horizon trade context is gone.
      if (this.running) {
        this.newSession("RUNTIME_RESTART");
        await this.ctx.storage.setAlarm(Date.now() + 1_000);
      }
    });
  }

  newSession(reason) {
    this.sessionId = `${VERSION}:${Date.now()}:${crypto.randomUUID().slice(0, 8)}`;
    this.startedAt = Date.now();
    this.bucketSeries.clear();
    this.lastFeatureCutoff.clear();
    this.lastEventAt.clear();
    this.recentTradeKeys.clear();
    this.pendingEvents.clear();
    this.pendingBySymbol.clear();
    this.sessionReason = reason;
  }

  async fetch(req) {
    const u = new URL(req.url);
    if (u.pathname === "/start" && req.method === "POST") {
      if (!this.running) {
        this.lastError = null;
        this.startStage = "FETCH_UNIVERSE";
        try {
          this.universe = await fetchResearchUniverse();
          this.startStage = "INIT_SESSION";
          this.running = true;
          this.newSession("MANUAL_START");
          await this.persistState();

          this.startStage = "CONNECT_WEBSOCKET";
          await this.connect();

          this.startStage = "SET_ALARM";
          await this.ctx.storage.setAlarm(Date.now() + CFG.alarmMs);
          this.startStage = "RUNNING";
          await this.persistState();
        } catch (e) {
          const message = String(e?.message || e);
          const stack = typeof e?.stack === "string" ? e.stack.slice(0, 2000) : null;
          this.running = false;
          for (const c of this.wsConnections) { try { c.ws?.close(1011, "startup failed"); } catch {} }
          this.wsConnections = [];
          this.wsOpenConnections = 0;
          this.wsStatus = "START_FAILED";
          this.lastError = {
            at: Date.now(),
            atIso: new Date().toISOString(),
            stage: this.startStage,
            name: String(e?.name || "Error"),
            message,
            stack,
          };
          this.startStage = "FAILED";
          try { await this.persistState(); } catch {}
          return j({ ...this.health(), ok: false, startupError: this.lastError }, 503);
        }
      }
      return j(this.health());
    }
    if (u.pathname === "/stop" && req.method === "POST") {
      this.running = false;
      for (const c of this.wsConnections) { try { c.ws?.close(1000, "manual stop"); } catch {} }
      this.wsConnections = [];
      this.wsOpenConnections = 0;
      this.wsStatus = "STOPPED";
      await this.persistState();
      try { await this.ctx.storage.deleteAlarm(); } catch {}
      return j(this.health());
    }
    if (u.pathname === "/health") return j(this.health());
    if (u.pathname === "/summary") return j(await this.summary());
    if (u.pathname === "/events") {
      const limit = Math.min(CFG.exportMaxLimit, Math.max(1, n(u.searchParams.get("limit"), CFG.exportDefaultLimit)));
      return j(await this.readEvents(limit));
    }
    return j({ ok: true, version: VERSION, mode: "RESEARCH_ONLY", routes: ["POST /start", "POST /stop", "/health", "/summary", "/events?limit=300"] });
  }

  async alarm() {
    if (!this.running) return;
    try {
      const anyUsable = this.wsConnections.some(c =>
        c.ws && (c.ws.readyState === WebSocket.OPEN || c.ws.readyState === WebSocket.CONNECTING)
      );
      if (!anyUsable || this.wsOpenConnections < this.wsExpectedConnections) await this.connect();
      await this.persistState();
    } catch (e) {
      this.lastError = {
        at: Date.now(),
        atIso: new Date().toISOString(),
        stage: "ALARM_RECONNECT",
        message: String(e?.message || e),
      };
    }
    if (this.running) await this.ctx.storage.setAlarm(Date.now() + CFG.alarmMs);
  }

  async connect() {
    if (!this.running) return;
    if (!this.universe.length) this.universe = await fetchResearchUniverse();

    const hasUsable = this.wsConnections.some(c =>
      c.ws && (c.ws.readyState === WebSocket.OPEN || c.ws.readyState === WebSocket.CONNECTING)
    );
    if (hasUsable && this.wsOpenConnections >= this.wsExpectedConnections) return;

    for (const c of this.wsConnections) { try { c.ws?.close(1000, "rebuild connections"); } catch {} }
    this.wsConnections = [];
    this.wsOpenConnections = 0;

    const chunks = chunkArray(this.universe, CFG.maxSubscriptionsPerConnection);
    this.wsExpectedConnections = chunks.length;
    this.wsStatus = "CONNECTING";

    if (chunks.length > CFG.maxWebSocketConnections) {
      throw new Error(`Need ${chunks.length} BingX WS connections; cap is ${CFG.maxWebSocketConnections}`);
    }

    chunks.forEach((symbols, index) => this.openBingXSocket(symbols, index));
  }

  openBingXSocket(symbols, index) {
    const ws = new WebSocket(BINGX_WS);
    ws.binaryType = "arraybuffer";
    const conn = { ws, symbols, index, opened: false };
    this.wsConnections.push(conn);

    ws.addEventListener("open", () => {
      if (!this.running || !this.wsConnections.includes(conn)) return;
      conn.opened = true;
      this.wsOpenConnections++;
      this.wsOpenedAt = Date.now();
      this.wsReconnects++;
      this.wsStatus = this.wsOpenConnections === this.wsExpectedConnections
        ? "OPEN"
        : `OPEN_${this.wsOpenConnections}_OF_${this.wsExpectedConnections}`;

      this.ctx.waitUntil((async () => {
        for (const symbol of symbols) {
          if (!this.running || ws.readyState !== WebSocket.OPEN) break;
          ws.send(JSON.stringify({
            id: crypto.randomUUID(),
            reqType: "sub",
            dataType: `${symbol}@trade`,
          }));
          await sleep(CFG.subscribePaceMs);
        }
      })());
    });

    ws.addEventListener("message", ev => {
      if (!this.running || !this.wsConnections.includes(conn)) return;
      this.ctx.waitUntil(this.onBingXWsMessage(ws, ev.data));
    });

    ws.addEventListener("close", ev => {
      if (!this.wsConnections.includes(conn)) return;
      if (conn.opened) this.wsOpenConnections = Math.max(0, this.wsOpenConnections - 1);
      conn.opened = false;
      this.wsStatus = `CLOSED_${ev.code}_${this.wsOpenConnections}_OF_${this.wsExpectedConnections}`;
      if (this.running) this.ctx.waitUntil(this.ctx.storage.setAlarm(Date.now() + CFG.websocketReconnectMs));
    });

    ws.addEventListener("error", () => {
      if (this.wsConnections.includes(conn)) this.wsStatus = `ERROR_${index}`;
    });
  }

  async onBingXWsMessage(ws, data) {
    const receivedAt = Date.now();
    this.wsLastMessageAt = receivedAt;
    this.stats.messages++;

    let text;
    try { text = await decodeBingXFrame(data); } catch { return; }

    if (text === "Ping") {
      try { ws.send("Pong"); } catch {}
      return;
    }

    let x;
    try { x = JSON.parse(text); } catch { return; }

    if (x?.code != null && n(x.code) !== 0) {
      this.lastError = {
        at: Date.now(),
        atIso: new Date().toISOString(),
        stage: "WS_MESSAGE",
        message: `BingX WS code ${x.code}: ${String(x.msg || "unknown")}`,
        dataType: x.dataType || null,
      };
      return;
    }

    const rows = Array.isArray(x?.data) ? x.data : x?.data ? [x.data] : [];
    if (!rows.length) return;

    const groupedTrades = new Map();
    for (const d of rows) {
      const rawSymbol = String(d?.s || "").toUpperCase();
      if (!rawSymbol) continue;
      const symbol = rawSymbol.includes("-") ? rawSymbol : rawSymbol.replace(/USDT$/, "-USDT");
      const price = n(d?.p);
      const qty = n(d?.q);
      const marketEventAt = n(d?.T);
      if (!(price > 0 && qty > 0 && marketEventAt > 0)) continue;

      // Timestamp-leakage guard: a delayed exchange trade must not be inserted into
      // the current local 5s bucket, and implausibly future timestamps are rejected.
      const transportLatencyMs = receivedAt - marketEventAt;
      if (transportLatencyMs > CFG.maxMarketEventLagMs) {
        this.stats.staleMessages++;
        continue;
      }
      if (transportLatencyMs < -CFG.maxFutureEventSkewMs) {
        this.stats.futureTimestampMessages++;
        continue;
      }

      const aggressorDir = d?.m === true ? -1 : 1;
      const rawId = d?.i ?? d?.a ?? null;

      // Only dedupe by trade id if BingX actually supplies one. The documented swap
      // trade payload has no mandatory id; using T/p/q/m as a synthetic id can delete
      // legitimate identical fills and contaminate flow.
      if (rawId != null) {
        let recent = this.recentTradeKeys.get(symbol);
        if (!recent) { recent = new Map(); this.recentTradeKeys.set(symbol, recent); }
        const cutoff = receivedAt - CFG.tradeIdDedupeKeepMs;
        for (const [k, t] of recent) if (t < cutoff) recent.delete(k);
        const dedupeKey = `${symbol}:id:${rawId}`;
        if (recent.has(dedupeKey)) {
          this.stats.duplicateTradeIdsDropped++;
          this.stats.duplicatesDropped++;
          continue;
        }
        recent.set(dedupeKey, receivedAt);
      }

      const trade = {
        id: rawId != null ? String(rawId) : `${symbol}:${marketEventAt}:${price}:${qty}:${crypto.randomUUID().slice(0, 8)}`,
        symbol,
        price,
        qty,
        quote: price * qty,
        aggressorDir,
        marketEventAt,
        exchangeEventAt: marketEventAt,
        receivedAt,
        transportLatencyMs,
      };
      this.stats.aggTrades++;
      let batch = groupedTrades.get(symbol);
      if (!batch) { batch = []; groupedTrades.set(symbol, batch); }
      batch.push(trade);
    }

    // A BingX push can contain several trades. Process them in exchange-time order so
    // 1s bucket first/last prices are deterministic, then use the newest observed trade
    // for the detection price and forward label at this receive timestamp.
    for (const [symbol, trades] of groupedTrades) {
      trades.sort((a, b) => a.marketEventAt - b.marketEventAt);
      let series = this.bucketSeries.get(symbol);
      if (!series) { series = new Map(); this.bucketSeries.set(symbol, series); }
      for (const trade of trades) updateSecondBucket(series, trade);
      pruneSecondBuckets(series, receivedAt);

      const latestTrade = trades[trades.length - 1];
      await this.labelPending(symbol, latestTrade);

      const featureCutoff = Math.floor(receivedAt / CFG.baselineBucketMs) * CFG.baselineBucketMs;
      if (this.lastFeatureCutoff.get(symbol) === featureCutoff) continue;
      this.lastFeatureCutoff.set(symbol, featureCutoff);
      await this.maybeCreateEvent(symbol, series, latestTrade, featureCutoff);
    }
  }

  async maybeCreateEvent(symbol, series, triggerTrade, featureCutoff) {
    const detectionAt = Date.now();
    const eventWindowStart = featureCutoff - CFG.baselineBucketMs;
    const f5 = aggregateBucketsCompleted(series, featureCutoff, 5_000);
    if (f5.count < CFG.minAggTrades5s) return;

    const baseline = completedBaselineQuotes(series, eventWindowStart);
    const usable = baseline.filter(x => x > 0);
    if (usable.length < CFG.minBaselineBuckets) return;
    const baseMedian = median(usable);
    if (!(baseMedian > 0)) return;
    const flowVsMedianX = f5.quote / baseMedian;
    if (flowVsMedianX < CFG.minFlowVsMedianX) return;
    if (Math.abs(f5.imbalance) < CFG.minAbsImbalance5s) return;
    if (f5.topSideShare < CFG.minTopSideShare5s) return;

    const priorAt = n(this.lastEventAt.get(symbol));
    if (detectionAt - priorAt < CFG.perSymbolPurgeMs) { this.stats.purged++; return; }

    const f10 = aggregateBucketsCompleted(series, featureCutoff, 10_000);
    const f30 = aggregateBucketsCompleted(series, featureCutoff, 30_000);
    const dir = f5.imbalance >= 0 ? 1 : -1;
    const id = uid();
    const ev = {
      id,
      version: VERSION,
      schemaVersion: SCHEMA_VERSION,
      sourceVersion: VERSION,
      dataSource: "BINGX_USDTM_TRADE_WEBSOCKET",
      referenceUniverse: "ACTIVE_BINGX_USDT_PERPETUAL",
      sessionId: this.sessionId,
      sessionReason: this.sessionReason,
      symbol,
      eventType: "FLOW_IMPULSE",
      candidateSide: sideName(dir),
      candidateDir: dir,

      // Critical lineage/timing separation
      marketEventAt: triggerTrade.marketEventAt,
      marketEventAtIso: safeIso(triggerTrade.marketEventAt),
      detectionAt,
      detectionAtIso: safeIso(detectionAt),
      detectionLatencyMs: detectionAt - triggerTrade.marketEventAt,
      detectionPrice: triggerTrade.price,

      episodeId: episodeId(detectionAt),
      utcDay: utcDay(detectionAt),
      purgeWindowMs: CFG.perSymbolPurgeMs,
      featureCutoff,
      featureCutoffIso: safeIso(featureCutoff),
      eventWindowStart,
      eventWindowStartIso: safeIso(eventWindowStart),
      overlapPolicy: "GATED_5S_WINDOW_IS_COMPLETED_AND_NON_OVERLAPPING_WITH_BASELINE; ONE_EVENT_PER_SYMBOL_PER_30S",

      gate: {
        flowVsMedianX,
        baselineMedian5sQuote: baseMedian,
        baselineNonZeroBuckets: usable.length,
        absImbalance5s: Math.abs(f5.imbalance),
        topSideShare5s: f5.topSideShare,
        aggTrades5s: f5.count,
        priceAcceptanceUsedAsGate: false,
      },
      features: {
        w5s: f5,
        w10s: f10,
        w30s: f30,
      },
      // Keep direction labels structurally separate from return/evaluation metrics.
      directionLabels: {},
      returnMetrics: {},
      evaluation: {},
      complete: false,
      researchOnly: true,
      ordersEnabled: false,
      paperTradingEnabled: false,
      roiLogicPresent: false,
    };

    this.lastEventAt.set(symbol, detectionAt);
    this.pendingEvents.set(id, ev);
    let ps = this.pendingBySymbol.get(symbol); if (!ps) { ps = new Set(); this.pendingBySymbol.set(symbol, ps); } ps.add(id);
    this.stats.events++;
    await this.writeEvent(ev);
  }

  async labelPending(symbol, trade) {
    const now = trade.receivedAt;
    const toDelete = [];
    const ids = this.pendingBySymbol.get(symbol);
    if (!ids?.size) return;
    for (const id of [...ids]) {
      const ev = this.pendingEvents.get(id);
      if (!ev) { ids.delete(id); continue; }
      let changed = false;
      let done = 0;
      for (const h of CFG.horizonsMs) {
        const key = horizonName(h);
        if (ev.directionLabels[key]) { done++; continue; }
        const targetAt = ev.detectionAt + h;
        if (now < targetAt) continue;
        const ret = pct(ev.detectionPrice, trade.price);
        const rawDirection = signDirection(ret);

        ev.directionLabels[key] = {
          horizonMs: h,
          targetAt,
          targetAtIso: safeIso(targetAt),
          observedAt: now,
          observedAtIso: safeIso(now),
          marketEventAt: trade.marketEventAt,
          marketEventAtIso: safeIso(trade.marketEventAt),
          labelDelayMs: now - targetAt,
          rawDirection,
        };
        ev.returnMetrics[key] = {
          horizonMs: h,
          detectionPrice: ev.detectionPrice,
          observedPrice: trade.price,
          rawReturnPct: ret,
          feesIncluded: false,
          fundingIncluded: false,
          slippageIncluded: false,
          leverageIncluded: false,
        };
        ev.evaluation[key] = {
          followCandidateCorrect: ret === 0 ? false : Math.sign(ret) === ev.candidateDir,
          reverseCandidateCorrect: ret === 0 ? false : Math.sign(ret) === -ev.candidateDir,
          tie: ret === 0,
        };
        changed = true;
        done++;
      }
      if (done === CFG.horizonsMs.length) {
        ev.complete = true;
        ev.completedAt = now;
        ev.completedAtIso = safeIso(now);
        toDelete.push(id);
        this.stats.labeled++;
        changed = true;
      }
      if (changed) await this.writeEvent(ev, false);
    }
    for (const id of toDelete) { this.pendingEvents.delete(id); ids.delete(id); }
    if (!ids.size) this.pendingBySymbol.delete(symbol);
  }

  async writeEvent(ev, addToIndex = true) {
    await this.ctx.storage.put(eventKey(ev.id), ev);
    if (!addToIndex) return;
    this.eventIndex.push(ev.id);

    // Forward labels extend to 15m. Never evict an event while it is pending, even
    // if event velocity temporarily pushes the index above its nominal cap.
    if (this.eventIndex.length > CFG.maxEvents + CFG.retentionTrimSlack) {
      let needRemove = this.eventIndex.length - CFG.maxEvents;
      const keep = [];
      const remove = [];
      for (const id of this.eventIndex) {
        if (needRemove > 0 && !this.pendingEvents.has(id)) {
          remove.push(id);
          needRemove--;
        } else {
          keep.push(id);
        }
      }
      if (needRemove > 0) this.stats.retentionProtectedPending += needRemove;
      this.eventIndex = keep;
      if (remove.length) await Promise.all(remove.map(id => this.ctx.storage.delete(eventKey(id))));
    }
    await this.ctx.storage.put(INDEX_KEY, this.eventIndex);
  }

  async readEvents(limit) {
    const ids = this.eventIndex.slice(-limit).reverse();
    const vals = await this.ctx.storage.get(ids.map(eventKey));
    const events = [];
    for (const id of ids) {
      const v = vals.get(eventKey(id));
      if (v) events.push(v);
    }
    return { ok: true, version: VERSION, count: events.length, events };
  }

  async summary() {
    const data = await this.readEvents(Math.min(this.eventIndex.length, CFG.exportMaxLimit));
    const horizons = {};
    for (const h of CFG.horizonsMs) {
      const key = horizonName(h);
      horizons[key] = { n: 0, up: 0, down: 0, flat: 0, followCorrect: 0, reverseCorrect: 0, avgLabelDelayMs: 0 };
    }
    const days = new Set(), episodes = new Set(), symbols = new Set();
    for (const e of data.events) {
      if (!e.complete) continue; // Session-disrupted partial events never enter accuracy summaries.
      days.add(e.utcDay); episodes.add(e.episodeId); symbols.add(e.symbol);
      for (const [key, l] of Object.entries(e.directionLabels || {})) {
        const o = horizons[key]; if (!o) continue;
        const evalRow = e.evaluation?.[key] || {};
        o.n++;
        if (l.rawDirection === "UP") o.up++; else if (l.rawDirection === "DOWN") o.down++; else o.flat++;
        if (evalRow.followCandidateCorrect) o.followCorrect++;
        if (evalRow.reverseCandidateCorrect) o.reverseCorrect++;
        o.avgLabelDelayMs += n(l.labelDelayMs);
      }
    }
    for (const o of Object.values(horizons)) {
      o.followAccuracyPct = o.n ? 100 * o.followCorrect / o.n : 0;
      o.reverseAccuracyPct = o.n ? 100 * o.reverseCorrect / o.n : 0;
      o.avgLabelDelayMs = o.n ? o.avgLabelDelayMs / o.n : 0;
      delete o.followCorrect; delete o.reverseCorrect;
    }
    return {
      ok: true,
      version: VERSION,
      metricDefinition: "directionLabels are raw UP/DOWN/FLAT from detectionAt; returnMetrics are separate; no fees/funding/slippage/leverage/ROI",
      independence: { uniqueUtcDays: days.size, unique5mEpisodes: episodes.size, uniqueSymbols: symbols.size, perSymbolPurgeMs: CFG.perSymbolPurgeMs },
      horizons,
      retainedEvents: this.eventIndex.length,
      exportedForSummary: data.events.length,
    };
  }

  async persistState() {
    await this.ctx.storage.put(STATE_KEY, {
      version: VERSION,
      schemaVersion: SCHEMA_VERSION,
      running: this.running,
      sessionId: this.sessionId,
      sessionReason: this.sessionReason,
      startedAt: this.startedAt,
      universe: this.universe,
      wsReconnects: this.wsReconnects,
      stats: this.stats,
      savedAt: Date.now(),
    });
    await this.ctx.storage.put(INDEX_KEY, this.eventIndex);
  }

  health() {
    return {
      ok: true,
      version: VERSION,
      schemaVersion: SCHEMA_VERSION,
      mode: "RESEARCH_ONLY_NO_ORDERS_NO_PAPER_NO_ROI",
      startStage: this.startStage,
      running: this.running,
      sessionId: this.sessionId,
      sessionReason: this.sessionReason,
      startedAt: this.startedAt || null,
      startedAtIso: safeIso(this.startedAt),
      universeCount: this.universe.length,
      dataSource: "BingX USDT-M real-time trade WebSocket",
      referenceUniverse: "Active BingX USDT perpetuals",
      websocket: {
        status: this.wsStatus,
        expectedConnections: this.wsExpectedConnections,
        openConnections: this.wsOpenConnections,
        openedAt: this.wsOpenedAt || null,
        lastMessageAt: this.wsLastMessageAt || null,
        reconnects: this.wsReconnects,
      },
      eventGate: {
        minFlowVsMedianX: CFG.minFlowVsMedianX,
        minAbsImbalance5s: CFG.minAbsImbalance5s,
        minAggTrades5s: CFG.minAggTrades5s,
        minTopSideShare5s: CFG.minTopSideShare5s,
        priceAcceptanceUsedAsGate: false,
      },
      labelHorizons: CFG.horizonsMs.map(horizonName),
      timeSemantics: "detectionAt=local receive/decision clock; marketEventAt=exchange trade clock; labels trigger on receivedAt>=detectionAt+horizon; stale/future exchange timestamps are rejected",
      sourceLineage: {
        universe: "BingX /openApi/swap/v2/quote/contracts",
        trades: "BingX {symbol}@trade WebSocket",
        externalBinanceDependency: false,
      },
      staleStateMigration: false,
      labelSeparation: "directionLabels separate from returnMetrics and evaluation",
      timestampGuard: {
        maxMarketEventLagMs: CFG.maxMarketEventLagMs,
        maxFutureEventSkewMs: CFG.maxFutureEventSkewMs,
      },
      duplicatePolicy: "provider trade-id dedupe only when an id is present; no synthetic/frame dedupe because documented swap trade rows have no mandatory unique id",
      retentionPolicy: `pending forward-label events are protected; nominal raw index cap ${CFG.maxEvents}`,
      noIdDuplicateCaveat: "BingX documented swap trade fields expose no mandatory trade id; preserve no-id rows rather than risk deleting legitimate identical fills",
      overlapPolicy: "Gated 5s event window is a completed bucket and has zero overlap with baseline; per-symbol event purge 30s; 10s/30s features descriptive only",
      stats: this.stats,
      retainedEventCount: this.eventIndex.length,
      pendingEventCount: this.pendingEvents.size,
      lastError: this.lastError,
    };
  }
}

function store(env) {
  if (env.DIRECTION_LAB?.getByName) return env.DIRECTION_LAB.getByName("v223:lab");
  const id = env.DIRECTION_LAB.idFromName("v223:lab");
  return env.DIRECTION_LAB.get(id);
}

async function proxy(env, path, method = "GET") {
  try {
    const r = await store(env).fetch(`https://lab.local${path}`, { method });
    return new Response(await r.text(), { status: r.status, headers: { "content-type": r.headers.get("content-type") || "application/json", "cache-control": "no-store" } });
  } catch (e) {
    return j({
      ok: false,
      version: VERSION,
      proxyError: {
        name: String(e?.name || "Error"),
        message: String(e?.message || e),
        stack: typeof e?.stack === "string" ? e.stack.slice(0, 2000) : null,
      },
    }, 503);
  }
}

export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    const p = u.pathname;
    if (p === "/start") return proxy(env, "/start", "POST");
    if (p === "/stop") return proxy(env, "/stop", "POST");
    if (p === "/health" || p === "/summary") return proxy(env, p);
    if (p === "/events") return proxy(env, `${p}${u.search}`);
    return j({
      ok: true,
      version: VERSION,
      mode: "RESEARCH_ONLY",
      note: "Separate V2.2 Direction Lab. Does not migrate or touch V2.1.10 state.",
      routes: ["/start", "/stop", "/health", "/summary", "/events?limit=300"],
    });
  },
};
