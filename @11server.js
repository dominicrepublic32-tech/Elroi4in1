// ═══════════════════════════════════════════════
// EL ROI — 4-in-1 Downtrend Bot
// 4 fully independent bots, one server
// ═══════════════════════════════════════════════
'use strict';

const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const fetch     = require('node-fetch');
const path      = require('path');
const fs        = require('fs');
const crypto    = require('crypto');

const app     = express();
const server  = http.createServer(app);
const dashWss = new WebSocket.Server({ server, path: '/dashboard' });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const PORT          = process.env.PORT || 3000;
const APP_ID_LIVE   = '33PHI8AUHOgLipbqlauBg';
const REDIRECT_URI  = process.env.REDIRECT_URI || 'https://elroi4in1-vtf9.onrender.com/callback';

// ── SHARED DERIV LOGIN ────────────────────────────
// One OAuth login for the whole dashboard — all 4 bots trade on this same
// account. Replaces the old per-bot login + demo API-token entry entirely.
let liveAuth = { accessToken:null, accountId:null, currency:null, balance:null, loggedIn:false };
// Holds the account list between "logged in with Deriv" and "user picked
// which account to trade on" (mirrors the picker flow used elsewhere).
let pendingLiveAuth = null;

// ── PKCE HELPERS ─────────────────────────────────
function base64url(buf) {
  return buf.toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,'');
}
function generateCodeVerifier() {
  return base64url(crypto.randomBytes(32));
}
function generateCodeChallenge(verifier) {
  return base64url(crypto.createHash('sha256').update(verifier).digest());
}

// ── OAUTH STATE STORE ─────────────────────────────
// Maps state => { botId, codeVerifier }
const oauthPending = new Map();

// ── PERSISTENT STORAGE ───────────────────────────
const DATA_FILE = path.join(__dirname, 'data.json');

function loadData() {
  try { if(fs.existsSync(DATA_FILE)) return JSON.parse(fs.readFileSync(DATA_FILE,'utf8')); }
  catch(e) { console.log('Load error:',e.message); }
  return { bots:{} };
}

function saveData() {
  try {
    const d = { bots:{} };
    bots.forEach(b=>{ d.bots[b.id]={ tradeLog:b.tradeLog, cfg:b.cfg }; });
    fs.writeFileSync(DATA_FILE,JSON.stringify(d,null,2));
  } catch(e) { console.log('Save error:',e.message); }
}

const savedData = loadData();

// ══════════════════════════════════════════════════════════════════════════
// DATA ENGINE — persistent tick/candle store + historical downloader,
// ported from bot 2 (Zone Touch 10-in-1) for backtesting Elroi's own
// strategy. The storage layer below is untouched from bot 2: it buckets
// ticks into candles purely by wall-clock epoch (Math.floor(epoch/60)*60),
// so it never assumed "1 tick = 1 second" the way V100 (1s) ticks happen to
// behave — it works the same for forex's irregular real-world tick spacing.
// Only the configured market list changed (V100/V75 -> the 5 forex pairs).
// ══════════════════════════════════════════════════════════════════════════
const Database = require('better-sqlite3');

// FIX: this was silently limited to just the 5 forex pairs. Every market
// Elroi can actually trade should be downloadable/backtestable — synthetic
// indices were never meant to be excluded, they were just the first thing
// added when forex support was built. Matches the full MARKETS list in
// index.html exactly.
const DATAENGINE_MARKETS = [
  '1HZ100V','R_100','1HZ75V','R_75','1HZ50V','R_50','1HZ25V','1HZ10V',
  'BOOM1000','BOOM500','CRASH1000','CRASH500','stpRNG',
  'frxEURUSD','frxGBPUSD','frxUSDJPY','frxUSDCHF','frxAUDUSD',
  'frxXAUUSD','cryBTCUSD','cryETHUSD',
];
const DATAENGINE_CANDLE_INTERVAL_SECONDS = 60;
const DATAENGINE_DB_PATH = process.env.DATAENGINE_DB_PATH || path.join(__dirname, 'data', 'dataengine.db');

// ─── DataEngine: schema (merged from lib/dataengine/schema.js) ─────────────────
/**
 * lib/dataengine/schema.js
 *
 * Schema for the DataEngine's persistent market database. Completely
 * separate concern from the bot's state.json (slot configs/stats/history):
 * this only ever stores raw ticks (source of truth) and derived candles.
 * No trading state, no account state, no slot state lives here.
 */

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS ticks (
  symbol TEXT    NOT NULL,
  epoch  INTEGER NOT NULL,
  quote  REAL    NOT NULL,
  PRIMARY KEY (symbol, epoch)
);

CREATE INDEX IF NOT EXISTS idx_ticks_symbol_epoch
  ON ticks (symbol, epoch);

CREATE TABLE IF NOT EXISTS candles (
  symbol TEXT    NOT NULL,
  epoch  INTEGER NOT NULL, -- candle OPEN epoch (start of interval)
  open   REAL    NOT NULL,
  high   REAL    NOT NULL,
  low    REAL    NOT NULL,
  close  REAL    NOT NULL,
  PRIMARY KEY (symbol, epoch)
);

CREATE INDEX IF NOT EXISTS idx_candles_symbol_epoch
  ON candles (symbol, epoch);
`;

function applySchema(db) {
  db.exec(SCHEMA_SQL);
}

// ─── DataEngine: database access layer (merged from lib/dataengine/db.js) ──────
/**
 * lib/dataengine/db.js
 *
 * All SQLite access for the DataEngine lives here. Ticks are the source of
 * truth and are never discarded; candles are derived, queryable convenience
 * data. Every insert uses INSERT OR IGNORE against a composite primary key
 * of (symbol, epoch), which makes duplicate prevention automatic.
 */
class DataEngineDB {
  constructor(dbPath) {
    const dir = path.dirname(dbPath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    this.dbPath = dbPath;
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');

    applySchema(this.db);
    this._prepare();
  }

  _prepare() {
    this.stmts = {
      insertTick: this.db.prepare(`INSERT OR IGNORE INTO ticks (symbol, epoch, quote) VALUES (?, ?, ?)`),
      getLatestTick: this.db.prepare(`SELECT symbol, epoch, quote FROM ticks WHERE symbol = ? ORDER BY epoch DESC LIMIT 1`),
      getOldestTick: this.db.prepare(`SELECT symbol, epoch, quote FROM ticks WHERE symbol = ? ORDER BY epoch ASC LIMIT 1`),
      countTicksForSymbol: this.db.prepare(`SELECT COUNT(*) AS c FROM ticks WHERE symbol = ?`),
      getTicksBetween: this.db.prepare(`SELECT symbol, epoch, quote FROM ticks WHERE symbol = ? AND epoch >= ? AND epoch <= ? ORDER BY epoch ASC`),
      deleteTicksBetween: this.db.prepare(`DELETE FROM ticks WHERE symbol = ? AND epoch >= ? AND epoch <= ?`),

      insertCandle: this.db.prepare(`INSERT OR IGNORE INTO candles (symbol, epoch, open, high, low, close) VALUES (?, ?, ?, ?, ?, ?)`),
      getLatestCandle: this.db.prepare(`SELECT symbol, epoch, open, high, low, close FROM candles WHERE symbol = ? ORDER BY epoch DESC LIMIT 1`),
      countCandlesForSymbol: this.db.prepare(`SELECT COUNT(*) AS c FROM candles WHERE symbol = ?`),
      getCandlesBetween: this.db.prepare(`SELECT symbol, epoch, open, high, low, close FROM candles WHERE symbol = ? AND epoch >= ? AND epoch <= ? ORDER BY epoch ASC`),
      deleteCandlesBetween: this.db.prepare(`DELETE FROM candles WHERE symbol = ? AND epoch >= ? AND epoch <= ?`),

      distinctSymbols: this.db.prepare(`SELECT DISTINCT symbol FROM ticks`),
    };

    this._insertTicksBatch = this.db.transaction((rows) => {
      const inserted = [];
      for (const row of rows) {
        const info = this.stmts.insertTick.run(row.symbol, row.epoch, row.quote);
        if (info.changes > 0) inserted.push(row);
      }
      return inserted;
    });
  }

  // ---- Ticks ----
  saveTick(symbol, epoch, quote) {
    return this.stmts.insertTick.run(symbol, epoch, quote).changes > 0;
  }

  saveTicksBatch(rows) {
    if (!rows || rows.length === 0) return [];
    return this._insertTicksBatch(rows);
  }

  getLatestTick(symbol) {
    return this.stmts.getLatestTick.get(symbol) || null;
  }

  getOldestTick(symbol) {
    return this.stmts.getOldestTick.get(symbol) || null;
  }

  getTicksBetween(symbol, start, end) {
    return this.stmts.getTicksBetween.all(symbol, start, end);
  }

  countTicks(symbol) {
    return this.stmts.countTicksForSymbol.get(symbol).c;
  }

  deleteTicksBetween(symbol, start, end) {
    return this.stmts.deleteTicksBetween.run(symbol, start, end).changes;
  }

  // ---- Candles ----
  saveCandle(symbol, epoch, open, high, low, close) {
    return this.stmts.insertCandle.run(symbol, epoch, open, high, low, close).changes > 0;
  }

  /**
   * FIX: CandleBuilder (used during live incremental tick processing) assumes
   * ticks always arrive in forward chronological order — it closes a candle
   * bucket the instant it sees a LATER timestamp. That assumption is true for
   * live streaming, but historical downloads paginate BACKWARD (newest page
   * first, then progressively older pages) — so every tick from any page
   * after the first looked "out of order" to CandleBuilder and was silently
   * dropped from candle-building (the raw tick was still saved fine, just
   * never turned into a candle). Net effect: only the most recent ~1 page's
   * worth of candles ever got built correctly for any multi-page download.
   *
   * This rebuilds candles for a range directly from the ticks table (which
   * was never affected by the bug — only candle-building was), reading them
   * back out in guaranteed ascending order via getTicksBetween's ORDER BY,
   * so the result is correct regardless of what order they were originally
   * downloaded/inserted in. Existing candles in the range are replaced.
   */
  rebuildCandlesForRange(symbol, start, end) {
    const ticks = this.getTicksBetween(symbol, start, end); // already ORDER BY epoch ASC
    this.deleteCandlesBetween(symbol, start, end);
    if (!ticks.length) return 0;
    let built = 0;
    const rebuild = this.db.transaction((rows) => {
      let cur = null;
      for (const t of rows) {
        const bucketEpoch = Math.floor(t.epoch / 60) * 60;
        if (!cur) { cur = { bucketEpoch, open: t.quote, high: t.quote, low: t.quote, close: t.quote }; continue; }
        if (bucketEpoch === cur.bucketEpoch) {
          cur.high = Math.max(cur.high, t.quote); cur.low = Math.min(cur.low, t.quote); cur.close = t.quote;
          continue;
        }
        this.stmts.insertCandle.run(symbol, cur.bucketEpoch, cur.open, cur.high, cur.low, cur.close); built++;
        cur = { bucketEpoch, open: t.quote, high: t.quote, low: t.quote, close: t.quote };
      }
      if (cur) { this.stmts.insertCandle.run(symbol, cur.bucketEpoch, cur.open, cur.high, cur.low, cur.close); built++; }
    });
    rebuild(ticks);
    return built;
  }

  getLatestCandle(symbol) {
    return this.stmts.getLatestCandle.get(symbol) || null;
  }

  getCandlesBetween(symbol, start, end) {
    return this.stmts.getCandlesBetween.all(symbol, start, end);
  }

  countCandles(symbol) {
    return this.stmts.countCandlesForSymbol.get(symbol).c;
  }

  deleteCandlesBetween(symbol, start, end) {
    return this.stmts.deleteCandlesBetween.run(symbol, start, end).changes;
  }

  // ---- Management ----
  listSymbols() {
    return this.stmts.distinctSymbols.all().map((r) => r.symbol);
  }

  /**
   * One-time self-heal for data downloaded before rebuildCandlesForRange()
   * existed — candle-building during backward-paginated downloads was
   * corrupted (see rebuildCandlesForRange's comment), and that corruption
   * sits permanently in the candles table until something happens to
   * rebuild that exact range. Rather than leave it to chance (only fixed
   * when a backtest or new download happens to touch that range), this
   * rebuilds EVERY symbol's full tick-covered range once, so anything
   * downloaded before this fix gets corrected on the next server start
   * regardless of whether it's ever backtested.
   */
  rebuildAllCandles(logger = console) {
    const symbols = this.listSymbols();
    let totalRebuilt = 0;
    for (const symbol of symbols) {
      const oldest = this.getOldestTick(symbol);
      const newest = this.getLatestTick(symbol);
      if (!oldest || !newest) continue;
      const built = this.rebuildCandlesForRange(symbol, oldest.epoch, newest.epoch);
      totalRebuilt += built;
      logger.info && logger.info(`[startup] rebuilt ${built} candles for ${symbol} [${oldest.epoch}..${newest.epoch}]`);
    }
    return totalRebuilt;
  }

  /** Full stats block used by the Database Manager panel. */
  getStats(configuredSymbols) {
    const symbols = Array.from(new Set([...(configuredSymbols || []), ...this.listSymbols()]));
    const perSymbol = symbols.map((symbol) => {
      const tickCount = this.countTicks(symbol);
      const candleCount = this.countCandles(symbol);
      const oldest = this.getOldestTick(symbol);
      const newest = this.getLatestTick(symbol);
      return {
        symbol,
        tickCount,
        candleCount,
        oldestEpoch: oldest ? oldest.epoch : null,
        newestEpoch: newest ? newest.epoch : null,
      };
    });
    return {
      symbols: perSymbol,
      dbSizeBytes: this.getDatabaseSizeBytes(),
      dbPath: this.dbPath,
    };
  }

  /**
   * Cheap integrity check: confirms the SQLite file itself is not corrupt,
   * and reports any symbol whose tick coverage has gaps larger than
   * `gapThresholdSeconds` (informational only — large gaps are expected
   * around weekends/maintenance and are not necessarily an error).
   */
  verifyIntegrity(configuredSymbols, gapThresholdSeconds = 300) {
    const pragmaResult = this.db.pragma('integrity_check');
    const sqliteOk = Array.isArray(pragmaResult) && pragmaResult.length === 1 && pragmaResult[0].integrity_check === 'ok';

    const gapReports = [];
    for (const symbol of configuredSymbols || this.listSymbols()) {
      const rows = this.db
        .prepare(`SELECT epoch FROM ticks WHERE symbol = ? ORDER BY epoch ASC`)
        .all(symbol);
      let gaps = 0;
      let largestGap = 0;
      for (let i = 1; i < rows.length; i++) {
        const diff = rows[i].epoch - rows[i - 1].epoch;
        if (diff > gapThresholdSeconds) {
          gaps += 1;
          if (diff > largestGap) largestGap = diff;
        }
      }
      gapReports.push({ symbol, tickCount: rows.length, gapsOverThreshold: gaps, largestGapSeconds: largestGap });
    }

    return { sqliteIntegrityOk: sqliteOk, gapThresholdSeconds, symbols: gapReports };
  }

  getDatabaseSizeBytes() {
    let total = 0;
    for (const suffix of ['', '-wal', '-shm']) {
      try {
        total += fs.statSync(this.dbPath + suffix).size;
      } catch (_) {
        /* file may not exist yet */
      }
    }
    return total;
  }

  close() {
    try {
      this.db.pragma('wal_checkpoint(TRUNCATE)');
    } catch (_) {}
    this.db.close();
  }
}

// ─── DataEngine: candle builder (merged from lib/dataengine/candleBuilder.js) ──
/**
 * lib/dataengine/candleBuilder.js
 *
 * Builds persisted OHLC candles from ticks for the DataEngine's own storage.
 * Completely separate from server.js's existing in-memory CandleManager
 * (which feeds live strategy execution) — this one writes durable candles
 * to SQLite for future backtesting. Historical backfill and live collection
 * both funnel through this exact same class so candle construction is
 * identical regardless of source, per spec.
 */
class CandleBuilder {
  constructor(db, intervalSeconds, logger = console) {
    this.db = db;
    this.intervalSeconds = intervalSeconds;
    this.logger = logger;
    this.openCandles = new Map(); // symbol -> { bucketEpoch, open, high, low, close }
  }

  _bucketStart(epoch) {
    return Math.floor(epoch / this.intervalSeconds) * this.intervalSeconds;
  }

  processTick(symbol, epoch, quote) {
    const bucketEpoch = this._bucketStart(epoch);
    let current = this.openCandles.get(symbol);

    if (!current) {
      this.openCandles.set(symbol, { bucketEpoch, open: quote, high: quote, low: quote, close: quote });
      return;
    }

    if (bucketEpoch === current.bucketEpoch) {
      current.high = Math.max(current.high, quote);
      current.low = Math.min(current.low, quote);
      current.close = quote;
      return;
    }

    if (bucketEpoch > current.bucketEpoch) {
      this.db.saveCandle(symbol, current.bucketEpoch, current.open, current.high, current.low, current.close);
      this.openCandles.set(symbol, { bucketEpoch, open: quote, high: quote, low: quote, close: quote });
      return;
    }
    // out-of-order tick for an already-closed bucket — tick itself is still
    // safely stored by the caller; only candle aggregation skips it.
  }

  flushOpenCandle(symbol) {
    const current = this.openCandles.get(symbol);
    if (current) {
      this.db.saveCandle(symbol, current.bucketEpoch, current.open, current.high, current.low, current.close);
    }
  }

  flushAll() {
    for (const symbol of this.openCandles.keys()) this.flushOpenCandle(symbol);
  }
}

// ─── DataEngine: historical downloader (merged from lib/dataengine/historyDownloader.js) ──
/**
 * lib/dataengine/historyDownloader.js
 *
 * Downloads historical TICKS from Deriv's ticks_history endpoint and
 * manages user-triggered download jobs (pause/resume/cancel/progress).
 *
 * PAGINATION — backward via `end`+`count`, WITH an explicit `start` on every
 * page (fixed after confirming with Deriv support — see chat transcript).
 * Earlier assumption here was that `start` is always ignored by Deriv and
 * only `end`+`count` matter; that was wrong. What was actually happening:
 * without an explicit `start`, Deriv silently defaults `start` to ~1 day
 * ago and returns only recent ticks regardless of how far back `end`
 * points — it does NOT error, so it looked like `start` was "ignored".
 * Deriv confirmed raw tick data is retained for about a month, but only
 * reachable by sending `start` explicitly. So every page now sends the
 * segment's lower bound as `start`, and still walks backward within that
 * bounded range: request a page ending "now" (or the current cursor), note
 * the OLDEST epoch received, set the next request's `end` to (that epoch -
 * 1), and repeat until the segment's `start` is reached.
 *
 * CONNECTION HANDLING — fixed after a real stall observed on a 7-day
 * download (progress would repeatedly stick around ~10% and not
 * continue). Root cause: the previous version opened a BRAND NEW
 * WebSocket connection for every single page — for a multi-day download
 * needing 100+ pages, that's 100+ rapid connect/disconnect cycles with no
 * delay between them, which very likely triggered connection throttling
 * or drops on Deriv's side. Fixed by:
 *   - Reusing ONE persistent connection for the life of a download job
 *     (opened once, kept alive, reconnected automatically if it drops).
 *   - Adding a small delay between page requests (PAGE_DELAY_MS) so the
 *     downloader isn't hammering the server back-to-back.
 *   - Correlating requests/responses by req_id instead of just "the next
 *     message", so a dropped/reconnected socket can't cause a response
 *     mismatch.
 *   - Logging every page fetch and every connection event via the
 *     provided logger, so a stall shows up clearly in the server console
 *     instead of silently freezing the dashboard's progress bar.
 *
 * NOTE: a ping/pong keepalive heartbeat was attempted here (per Deriv's
 * own stated best practice) but was found via testing to cause a severe
 * reconnect storm under certain timing conditions and was reverted. The
 * connection is instead kept alive implicitly by the natural cadence of
 * page requests every PAGE_DELAY_MS; genuine drops are still caught by
 * the socket's native 'close'/'error' events and trigger a clean
 * reconnect on the next request.
 *
 * Historical downloads NEVER start automatically — this module only ever
 * runs when explicitly triggered via the dashboard's Download Manager.
 */

const PAGE_DELAY_MS = 250;
const REQUEST_TIMEOUT_MS = 20000;
const RECONNECT_DELAY_MS = 4000;

class HistoryDownloader {
  /**
   * @param {DataEngineDB} db
   * @param {CandleBuilder} candleBuilder
   * @param {string} wsUrl e.g. wss://ws.binaryws.com/websockets/v3?app_id=1089
   * @param {object} logger
   * @param {number} pageSize ticks requested per call via `count` (default 5000, confirmed accepted by Deriv)
   */
  constructor(db, candleBuilder, wsUrl, logger = console, pageSize = 5000) {
    this.db = db;
    this.candleBuilder = candleBuilder;
    this.wsUrl = wsUrl;
    this.logger = logger;
    this.pageSize = pageSize;
    this.jobs = new Map();
    this._jobCounter = 1;

    // Single shared, persistent, reconnecting connection reused across
    // every download job (historical fetches only — never touches the
    // trading account socket).
    this.ws = null;
    this.wsConnecting = null; // in-flight connect promise, if any
    this.reqIdCounter = 1;
    this.pending = new Map(); // req_id -> {resolve, reject, timeout}
  }

  listJobs() {
    return Array.from(this.jobs.values()).map((j) => this._publicJob(j));
  }

  getJob(jobId) {
    const j = this.jobs.get(jobId);
    return j ? this._publicJob(j) : null;
  }

  _publicJob(j) {
    const elapsedMs = Date.now() - j.startedAt;
    const wallClockSecElapsed = Math.max(0.001, elapsedMs / 1000);

    let secondsCovered = j.completedSegmentSeconds;
    if (j.currentSegment) {
      const [segStart, segEnd] = j.currentSegment;
      const covered = j.cursor != null ? Math.max(0, segEnd - j.cursor) : 0;
      secondsCovered += Math.min(covered, segEnd - segStart);
    }
    const totalSeconds = Math.max(1, j.totalRequestedSeconds);
    const percent = j.status === 'completed' ? 100 : Math.min(100, (secondsCovered / totalSeconds) * 100);

    const rateMultiplier = secondsCovered > 0 ? secondsCovered / wallClockSecElapsed : 0;
    const remainingSeconds = Math.max(0, totalSeconds - secondsCovered);
    const etaSeconds = rateMultiplier > 0 ? Math.round(remainingSeconds / rateMultiplier) : null;
    const ticksPerSecond = j.ticksDownloaded > 0 ? +(j.ticksDownloaded / wallClockSecElapsed).toFixed(1) : 0;

    return {
      id: j.id,
      symbol: j.symbol,
      status: j.status,
      ticksDownloaded: j.ticksDownloaded,
      pagesDownloaded: j.pagesDownloaded,
      percent: +percent.toFixed(2),
      currentPeriod: j.cursor ? new Date(j.cursor * 1000).toISOString() : null,
      fromPeriod: new Date(j.targetStart * 1000).toISOString(),
      toPeriod: new Date(j.targetEnd * 1000).toISOString(),
      etaSeconds,
      downloadSpeedTicksPerSec: ticksPerSecond,
      error: j.error || null,
      note: j.note || null,
    };
  }

  startDownload(symbol, seconds, onProgress) {
    const existing = Array.from(this.jobs.values()).find(j => j.symbol === symbol && (j.status === 'running' || j.status === 'paused'));
    if (existing) {
      throw new Error(`A download for ${symbol} is already ${existing.status} (job ${existing.id}) — wait for it to finish or cancel it first.`);
    }
    const now = Math.floor(Date.now() / 1000);
    const targetStart = now - seconds;
    const targetEnd = now;
    const oldest = this.db.getOldestTick(symbol);
    const newest = this.db.getLatestTick(symbol);

    const segments = [];
    if (!oldest) {
      segments.push([targetStart, targetEnd]);
    } else {
      if (newest.epoch < targetEnd) segments.push([newest.epoch + 1, targetEnd]);
      if (oldest.epoch > targetStart) segments.push([targetStart, oldest.epoch - 1]);
    }

    const totalRequestedSeconds = segments.reduce((sum, [s, e]) => sum + Math.max(0, e - s + 1), 0);

    const job = {
      id: `dl${this._jobCounter++}`,
      symbol,
      targetStart,
      targetEnd,
      segments,
      segmentQueueIndex: 0,
      currentSegment: null,
      completedSegmentSeconds: 0,
      totalRequestedSeconds,
      cursor: null,
      status: 'running',
      ticksDownloaded: 0,
      pagesDownloaded: 0,
      startedAt: Date.now(),
      error: null,
      note: segments.length === 0 ? 'Requested range is already fully covered by existing data.' : null,
      _paused: false,
      _cancelled: false,
      _resumeWaiters: [],
    };
    this.jobs.set(job.id, job);
    this.logger.info && this.logger.info(`[download:${job.id}] starting: symbol=${symbol} segments=${JSON.stringify(segments)} totalRequestedSeconds=${totalRequestedSeconds}`);

    this._runJob(job, onProgress).catch((err) => {
      job.status = 'error';
      job.error = err.message;
      this.logger.error && this.logger.error(`[download:${job.id}] fatal error: ${err.message}`);
      if (onProgress) onProgress(this._publicJob(job));
    });

    return this._publicJob(job);
  }

  pauseJob(jobId) {
    const job = this.jobs.get(jobId);
    if (!job || job.status !== 'running') return null;
    job._paused = true;
    job.status = 'paused';
    this.logger.info && this.logger.info(`[download:${job.id}] paused by user`);
    return this._publicJob(job);
  }

  resumeJob(jobId, onProgress) {
    const job = this.jobs.get(jobId);
    if (!job || job.status !== 'paused') return null;
    job._paused = false;
    job.status = 'running';
    this.logger.info && this.logger.info(`[download:${job.id}] resumed by user`);
    const waiters = job._resumeWaiters;
    job._resumeWaiters = [];
    waiters.forEach((resolve) => resolve());
    if (onProgress) onProgress(this._publicJob(job));
    return this._publicJob(job);
  }

  cancelJob(jobId) {
    const job = this.jobs.get(jobId);
    if (!job) return null;
    job._cancelled = true;
    job.status = 'cancelled';
    this.logger.info && this.logger.info(`[download:${job.id}] cancelled by user`);
    if (job._paused) {
      job._paused = false;
      const waiters = job._resumeWaiters;
      job._resumeWaiters = [];
      waiters.forEach((resolve) => resolve());
    }
    return this._publicJob(job);
  }

  async _waitIfPaused(job) {
    if (!job._paused) return;
    await new Promise((resolve) => job._resumeWaiters.push(resolve));
  }

  async _runJob(job, onProgress) {
    if (job.segments.length === 0) {
      job.status = 'completed';
      if (onProgress) onProgress(this._publicJob(job));
      return;
    }

    for (job.segmentQueueIndex = 0; job.segmentQueueIndex < job.segments.length; job.segmentQueueIndex++) {
      if (job._cancelled) break;
      const [segStart, segEnd] = job.segments[job.segmentQueueIndex];
      job.currentSegment = [segStart, segEnd];
      job.cursor = segEnd;

      this.logger.info && this.logger.info(
        `[download:${job.id}] === starting segment ${job.segmentQueueIndex + 1}/${job.segments.length}: ` +
        `[${segStart}..${segEnd}] (span=${segEnd - segStart}s, ${new Date(segStart*1000).toISOString()} .. ${new Date(segEnd*1000).toISOString()}) ===`
      );

      const ok = await this._runSegment(job, segStart, segEnd, onProgress);
      job.completedSegmentSeconds += Math.max(0, segEnd - segStart + 1);
      this.logger.info && this.logger.info(
        `[download:${job.id}] === segment ${job.segmentQueueIndex + 1}/${job.segments.length} finished (ok=${ok}, cursor ended at ${job.cursor}) ===`
      );
      job.currentSegment = null;
      if (!ok) break;
    }

    this.candleBuilder.flushOpenCandle(job.symbol);
    // FIX: candles built incrementally DURING a backward-paginated download
    // get corrupted (see rebuildCandlesForRange() for the full explanation) —
    // so once the job's done, throw away whatever candles the incremental
    // builder produced for this range and rebuild them properly from the
    // ticks table, which was never affected by the ordering bug.
    if (job.ticksDownloaded > 0) {
      const rebuilt = this.db.rebuildCandlesForRange(job.symbol, job.targetStart, job.targetEnd);
      this.logger.info && this.logger.info(`[download:${job.id}] rebuilt ${rebuilt} candles for [${job.targetStart}..${job.targetEnd}] from raw ticks`);
    }
    if (job._cancelled && job.status !== 'error') job.status = 'cancelled';
    else if (!job._cancelled && job.status !== 'error') job.status = 'completed';
    this.logger.info && this.logger.info(`[download:${job.id}] finished: status=${job.status} ticks=${job.ticksDownloaded} pages=${job.pagesDownloaded}`);
    if (onProgress) onProgress(this._publicJob(job));
  }

  async _runSegment(job, segStart, segEnd, onProgress) {
    const segmentSpanSeconds = Math.max(1, segEnd - segStart);
    const maxPages = Math.max(500, Math.ceil(segmentSpanSeconds / this.pageSize) + 100);

    while (job.cursor >= segStart) {
      if (job._cancelled) return false;
      await this._waitIfPaused(job);
      if (job._cancelled) return false;

      if (job.pagesDownloaded >= maxPages * (job.segmentQueueIndex + 1)) {
        job.status = 'error';
        job.error = 'Safety limit reached without completing — investigate before retrying.';
        this.logger.error && this.logger.error(`[download:${job.id}] ${job.error}`);
        return false;
      }

      let page;
      const requestedEnd = job.cursor;
      try {
        page = await this._fetchPage(job.symbol, requestedEnd, segStart);
      } catch (err) {
        job.status = 'error';
        job.error = err.message;
        this.logger.error && this.logger.error(`[download:${job.id}] page fetch failed at cursor=${requestedEnd}: ${err.message}`);
        return false;
      }

      job.pagesDownloaded += 1;

      if (page.times.length === 0) {
        const note = `No ticks available at or before ${new Date(requestedEnd * 1000).toISOString()} — ` +
          `this is likely the retention limit for raw tick data on this symbol.`;
        job.note = job.note ? job.note : note;
        this.logger.info && this.logger.info(`[download:${job.id}] page ${job.pagesDownloaded}: empty response — ${note}`);
        return true;
      }

      const earliestEpoch = page.times[0];
      const latestEpoch = page.times[page.times.length - 1];

      // CRITICAL: detect a real, confirmed Deriv API quirk found via live
      // testing — when `end` is requested further back than the server's
      // raw-tick retention window, it does NOT return empty or an error.
      // It silently ignores our `end` and returns its most recent ticks
      // instead (as if `end` were "latest"). Without this check, that looks
      // like a normal page and the loop keeps walking back to the same
      // wall, getting redirected to "now" again, forever — which is
      // exactly the "stuck at ~24h" symptom this was built to catch.
      // Detected by: the newest tick in the page is newer than the `end`
      // we actually asked for (with a small tolerance for tick-rate jitter).
      const RETENTION_TOLERANCE_SECONDS = 5;
      if (latestEpoch > requestedEnd + RETENTION_TOLERANCE_SECONDS) {
        const note = `Reached Deriv's raw-tick retention limit at ${new Date(requestedEnd * 1000).toISOString()} — ` +
          `requests for data further back than that are being silently answered with the most recent ticks ` +
          `instead of the requested range (a confirmed Deriv API quirk), so no further historical data is ` +
          `retrievable for this symbol beyond this point.`;
        job.note = job.note ? job.note : note;
        this.logger.warn && this.logger.warn(
          `[download:${job.id}] seg${job.segmentQueueIndex + 1}[${segStart}..${segEnd}] page ${job.pagesDownloaded}: ` +
          `RETENTION WALL detected — requested end=${requestedEnd} but got latestEpoch=${latestEpoch} (newer than requested). ${note}`
        );
        return true;
      }

      const ticks = page.times.map((t, i) => ({ symbol: job.symbol, epoch: t, quote: page.prices[i] }));
      const insertedRows = this.db.saveTicksBatch(ticks);
      for (const row of insertedRows) this.candleBuilder.processTick(row.symbol, row.epoch, row.quote);
      job.ticksDownloaded += insertedRows.length;

      this.logger.info && this.logger.info(
        `[download:${job.id}] seg${job.segmentQueueIndex + 1}[${segStart}..${segEnd}] page ${job.pagesDownloaded}: received=${page.times.length} new=${insertedRows.length} ` +
        `range=[${earliestEpoch}..${latestEpoch}] cursor->${earliestEpoch - 1}`
      );

      if (onProgress) onProgress(this._publicJob(job));

      if (earliestEpoch <= segStart) return true;

      job.cursor = earliestEpoch - 1;

      if (PAGE_DELAY_MS > 0) {
        await this._sleep(PAGE_DELAY_MS);
      }
    }
    return true;
  }

  _sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  // =======================================================================
  // Persistent, reused, auto-reconnecting connection
  // =======================================================================

  /** Ensures a live, open WebSocket exists, connecting/reconnecting as needed. */
  async _ensureConnected() {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) return;
    if (this.wsConnecting) return this.wsConnecting;

    this.wsConnecting = new Promise((resolve, reject) => {
      this.logger.info && this.logger.info(`[history-socket] connecting to ${this.wsUrl}`);
      const ws = new WebSocket(this.wsUrl);

      const onOpen = () => {
        this.logger.info && this.logger.info('[history-socket] connected');
        this.ws = ws;
        this.wsConnecting = null;
        resolve();
      };
      const onError = (err) => {
        this.logger.warn && this.logger.warn(`[history-socket] connection error: ${err.message}`);
      };
      const onClose = (code) => {
        this.logger.warn && this.logger.warn(`[history-socket] connection closed (code=${code}) — will reconnect on next request`);
        if (this.ws === ws) this.ws = null;
        this.wsConnecting = null;
        // Reject any requests that were pending on this now-dead socket.
        for (const [reqId, pending] of this.pending) {
          clearTimeout(pending.timeout);
          pending.reject(new Error('Connection closed before response arrived'));
          this.pending.delete(reqId);
        }
      };

      ws.once('open', onOpen);
      ws.once('error', (err) => { onError(err); reject(err); });
      ws.on('close', onClose);
      ws.on('message', (raw) => this._handleMessage(raw));
    });

    return this.wsConnecting;
  }

  _handleMessage(raw) {
    let msg;
    try { msg = JSON.parse(raw); } catch (_) { return; }
    if (msg.req_id != null && this.pending.has(msg.req_id)) {
      const pending = this.pending.get(msg.req_id);
      clearTimeout(pending.timeout);
      this.pending.delete(msg.req_id);
      if (msg.error) pending.reject(new Error(msg.error.message));
      else pending.resolve(msg);
    }
  }

  async _send(payload) {
    // Reconnect with growing backoff if the connection is currently down —
    // a rate-limit response needs real cool-down time, not a rapid retry.
    let attempts = 0;
    while (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      attempts += 1;
      try {
        await this._ensureConnected();
      } catch (err) {
        if (attempts >= 5) throw new Error(`Unable to connect to Deriv after ${attempts} attempts: ${err.message}`);
        await this._sleep(RECONNECT_DELAY_MS * attempts);
      }
    }

    const req_id = this.reqIdCounter++;
    const body = { ...payload, req_id };

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(req_id);
        reject(new Error(`Request timed out after ${REQUEST_TIMEOUT_MS}ms`));
      }, REQUEST_TIMEOUT_MS);

      this.pending.set(req_id, { resolve, reject, timeout });

      try {
        this.ws.send(JSON.stringify(body));
      } catch (err) {
        clearTimeout(timeout);
        this.pending.delete(req_id);
        reject(err);
      }
    });
  }

  /**
   * Backward-pagination request: `end` + `count`, PLUS an explicit `start`.
   *
   * FIX (confirmed with Deriv support): requests using only `count` + `end`
   * are silently capped to roughly the last 24 hours — Deriv defaults
   * `start` to ~1 day ago internally and ignores how far back `end` points,
   * which is exactly the "retention wall" behavior detected below. Deriv
   * confirmed raw tick data is actually available for about a month, but
   * only if every request carries an explicit `start` epoch. So every page
   * request now also sends `start` (the lower bound of the segment being
   * downloaded) — `count` + `end` still control which page within that
   * bounded range comes back, but `start` is what unlocks data older than
   * ~24h instead of it being silently discarded.
   */
  async _fetchPage(symbol, end, start) {
    const payload = { ticks_history: symbol, end, count: this.pageSize, style: 'ticks' };
    if (start != null) payload.start = start;
    const resp = await this._send(payload);
    if (!resp.history) {
      throw new Error(`Unexpected ticks_history response shape: ${JSON.stringify(resp).slice(0, 200)}`);
    }
    return { times: resp.history.times || [], prices: resp.history.prices || [] };
  }
}

// ─── DataEngine: live tick collector (adapted for Elroi) ────────────────────
// Bot 2 tapped a shared tick multiplexer (candleManager) that doesn't exist
// in Elroi — each Elroi bot opens its own direct WebSocket instead. Rather
// than rewire Elroi's trading connections, this opens its own lightweight,
// public (no-auth) ticks subscription per configured forex pair, completely
// independent of the 4 trading bots, so history keeps accumulating for
// backtesting whether or not any bot is actively trading that symbol.
// FIX: this was `wss://ws.derivws.com/websockets/v3?app_id=${APP_ID_LIVE}` —
// the old-style endpoint, using our OAuth login app's client_id where it
// isn't authorized, which is exactly what caused the 401. Bot 2 uses
// Deriv's newer public Options API endpoint for this, which needs no
// app_id at all — same endpoint family the live OAuth/OTP trading
// connection already uses elsewhere in this file.
const DATAENGINE_WS_URL = 'wss://api.derivws.com/trading/v1/options/ws/public';

// FIX (401 → then 429): first the URL was wrong (see above). Once fixed,
// this opened 5 separate WebSocket connections simultaneously at boot (one
// per forex pair) — a burst of 5 handshakes at once, repeated on every
// Render restart, is exactly the kind of thing that trips a per-IP
// connection-rate limit and got a 429 back. Deriv's API supports
// subscribing to multiple symbols' ticks over ONE connection, so this now
// uses a single shared socket for all 5 pairs instead of one each, and
// backs off with growing delay + jitter on reconnect rather than hammering
// every 5 seconds.
class LiveTickCollector {
  constructor(db, candleBuilder, wsUrl, logger = console) {
    this.db = db;
    this.candleBuilder = candleBuilder;
    this.wsUrl = wsUrl;
    this.logger = logger;
    this.symbols = new Set();
    this.running = new Set();   // symbols currently subscribed on the shared socket
    this.stats = new Map();     // symbol -> { ticksPersisted }
    this.ws = null;
    this.connecting = false;
    this.reconnectTimer = null;
    this.reconnectAttempt = 0;
  }

  start(symbols) {
    for (const symbol of symbols) this.startSymbol(symbol);
  }

  startSymbol(symbol) {
    this.symbols.add(symbol);
    if (!this.stats.has(symbol)) this.stats.set(symbol, { ticksPersisted: 0 });
    this._ensureConnected();
  }

  _ensureConnected() {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this._subscribeAll();
      return;
    }
    if (this.connecting) return;
    this.connecting = true;

    const ws = new WebSocket(this.wsUrl);
    this.ws = ws;

    ws.on('open', () => {
      this.connecting = false;
      this.reconnectAttempt = 0;
      this.logger.info && this.logger.info(`[dataengine] live tick socket connected (${this.symbols.size} pairs)`);
      this._subscribeAll();
    });

    ws.on('message', (raw) => {
      let d; try { d = JSON.parse(raw); } catch (e) { return; }
      if (d.msg_type === 'tick' && d.tick) {
        const symbol = d.tick.symbol;
        const epoch = d.tick.epoch;
        const quote = parseFloat(d.tick.quote);
        const inserted = this.db.saveTick(symbol, epoch, quote);
        if (inserted) {
          this.candleBuilder.processTick(symbol, epoch, quote);
          const s = this.stats.get(symbol);
          if (s) s.ticksPersisted += 1;
        }
      }
    });

    ws.on('close', () => {
      this.connecting = false;
      if (this.ws === ws) this.ws = null;
      this.running.clear();
      if (this.symbols.size === 0) return; // stopAll() already cleaned up
      this.reconnectAttempt++;
      const delay = Math.min(60000, 5000 * Math.pow(1.6, this.reconnectAttempt - 1)) + Math.random() * 1000;
      this.logger.warn && this.logger.warn(`[dataengine] live tick socket closed — reconnecting in ${Math.round(delay / 1000)}s (attempt ${this.reconnectAttempt})`);
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = setTimeout(() => this._ensureConnected(), delay);
    });

    ws.on('error', (e) => {
      this.connecting = false;
      this.logger.warn && this.logger.warn(`[dataengine] live tick socket error: ${e.message}`);
    });
  }

  _subscribeAll() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    for (const symbol of this.symbols) {
      if (this.running.has(symbol)) continue;
      this.ws.send(JSON.stringify({ ticks: symbol, subscribe: 1 }));
      this.running.add(symbol);
    }
  }

  stopSymbol(symbol) {
    if (!this.symbols.has(symbol)) return false;
    this.symbols.delete(symbol);
    this.running.delete(symbol);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ forget_all: 'ticks' }));
      this._subscribeAll(); // re-subscribe to whatever's left after forgetting everything
    }
    this.candleBuilder.flushOpenCandle(symbol);
    this.logger.info && this.logger.info(`[dataengine] live tick collection stopped for ${symbol}`);
    return true;
  }

  isRunning(symbol) {
    return this.running.has(symbol);
  }

  stopAll() {
    const had = this.symbols.size > 0;
    this.symbols.clear();
    this.running.clear();
    clearTimeout(this.reconnectTimer);
    if (this.ws) { try { this.ws.terminate(); } catch (_) {} this.ws = null; }
    this.candleBuilder.flushAll();
    return had;
  }

  getStats(symbol) {
    return this.stats.get(symbol) || { ticksPersisted: 0 };
  }
}

// ── MKT NAMES ─────────────────────────────────────
const MKT_NAMES = {
  '1HZ100V':'Volatility 100 (1s)','R_100':'Volatility 100',
  '1HZ75V':'Volatility 75 (1s)','R_75':'Volatility 75',
  '1HZ50V':'Volatility 50 (1s)','R_50':'Volatility 50',
  '1HZ25V':'Volatility 25 (1s)','1HZ10V':'Volatility 10 (1s)',
  'frxEURUSD':'EUR/USD','frxGBPUSD':'GBP/USD','frxXAUUSD':'Gold/USD',
  'frxUSDJPY':'USD/JPY','frxUSDCHF':'USD/CHF','frxAUDUSD':'AUD/USD',
  'cryBTCUSD':'BTC/USD','cryETHUSD':'ETH/USD','stpRNG':'Step Index',
  'BOOM1000':'Boom 1000','BOOM500':'Boom 500','CRASH1000':'Crash 1000','CRASH500':'Crash 500',
};

// ── BOT FACTORY ──────────────────────────────────
function createBot(id) {
  const saved = savedData.bots?.[id] || {};
  return {
    id,
    cfg: {
      market:'1HZ100V', command:'NOTOUCH',
      stake:1.00, durationMins:5, barrierOffset:'+2.1',
      multiplier:10, takeProfit:4.00, stopLoss:2.00,
      multTPDistance:0, multSLDistance:0, // price-distance TP/SL for multiplier backtests (e.g. 0.0050 = 50 pips)
      scanTFs:['M1','M5'], minTFConfirm:2, smallTol:10, bigTol:15,
      smallConfirm:1, bigConfirm:2, proximityPct:90,
      maxTrades:0, maxConsecLosses:2, cooldownSecs:1800, cooldownEnabled:true,
      vanillaStrike:null, vanillaTakeProfit:5.00,
      teleToken:'', teleChatId:'',
      htfClosePct:20, htfPassPct:30,
      ...(saved.cfg||{}),
    },

    derivWs:null, botActive:false, userStarted:false,
    reconnectTimer:null, scanInterval:null,
    currentPrice:0,
    candles:{ M1:[],M5:[],M15:[],M30:[],H1:[],H4:[] },
    trendStatus:{ M1:null,M5:null,M15:null },
    confirmedTrend:false,
    activeStructures:[],
    ignoredLevels:new Set(),
    doNotTradeZones:[],
    htfZones:[],          // all active HTF zones — auto + manual, each: {a,b,source,id,label,cancelled}
    htfZonePaused:false,
    autoHtfStructures:[], // detected swing low structures for display
    inTrade:false, currentContractId:null,
    activeContracts:{},
    activeTradeTimers:{}, // contractId -> {stake, command} for multi-trade tracking
    entryTargets:[],
    pendingTrades:[],
    tradeCount:0, wins:0, losses:0, sessionPnl:0,
    tradeLog: saved.tradeLog || [],
    consecutiveLosses:0, lossCountdownPaused:false,
    lossCountdownTimer:null, lossCountdownRemaining:0, lossCountdownTotal:0,
    timeOffPaused:false, timeOffTimer:null, timeOffRemaining:0, timeOffTotal:0,
    tickerMsg:`— BOT ${id} READY —`, statusText:'IDLE',
  };
}

const bots = [createBot(1),createBot(2),createBot(3),createBot(4)];

// ── BROADCAST ─────────────────────────────────────
function broadcast(data) {
  const json=JSON.stringify(data);
  dashWss.clients.forEach(c=>{ if(c.readyState===WebSocket.OPEN) c.send(json); });
}

function broadcastBotState(b) {
  if(b.backtest) return;
  broadcast({
    type:'bot_state', id:b.id,
    botActive:b.botActive, currentPrice:b.currentPrice,
    trendStatus:b.trendStatus, confirmedTrend:b.confirmedTrend,
    activeStructures:b.activeStructures.map(s=>({
      peaks:s.peaks,baseDiff:s.baseDiff,type:s.type,tf:s.tf,
      projectedLevels:s.projectedLevels,tradedLevels:[...s.tradedLevels],id:s.id
    })),
    ignoredLevels:[...b.ignoredLevels],
    doNotTradeZones:b.doNotTradeZones,
    htfZones:b.htfZones,
    htfZonePaused:b.htfZonePaused,
    htfPauseReason:b.htfPauseReason||'',
    activeHtfZoneId:b.activeHtfZoneId||null,
    autoHtfStructures:b.autoHtfStructures||[],
    activeContractsList:Object.entries(b.activeContracts||{}).map(([cid,i])=>({contractId:cid,level:i.level,structType:i.structType,command:i.command,market:i.market,stake:i.stake})),
    tradeCount:b.tradeCount,wins:b.wins,losses:b.losses,sessionPnl:b.sessionPnl,
    consecutiveLosses:b.consecutiveLosses,
    lossCountdownPaused:b.lossCountdownPaused,
    lossCountdownRemaining:b.lossCountdownRemaining,
    lossCountdownTotal:b.lossCountdownTotal,
    timeOffPaused:b.timeOffPaused,
    timeOffRemaining:b.timeOffRemaining,
    timeOffTotal:b.timeOffTotal,
    tickerMsg:b.tickerMsg, statusText:b.statusText,
    cfg:b.cfg,
    liveLoggedIn:liveAuth.loggedIn,
    liveAccountId:liveAuth.accountId,
    tradeLog:b.tradeLog.slice(0,100),
  });
}

function log(b,msg) {
  if(b.backtest) return;
  const t=new Date().toISOString().replace('T',' ').slice(0,19);
  const full=`[${t}][Bot${b.id}] ${msg}`;
  console.log(full);
  broadcast({type:'log',id:b.id,msg:full});
}

function setTicker(b,msg){ if(b.backtest) return; b.tickerMsg=msg; broadcast({type:'ticker',id:b.id,msg}); }
function setStatus(b,s,t){ if(b.backtest) return; b.statusText=t; broadcast({type:'status',id:b.id,status:s,text:t}); }

// ── TELEGRAM ──────────────────────────────────────
async function telegram(b,msg) {
  if(b.backtest) return;
  if(!b.cfg.teleToken||!b.cfg.teleChatId) return;
  try {
    await fetch(`https://api.telegram.org/bot${b.cfg.teleToken}/sendMessage`,{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({chat_id:b.cfg.teleChatId,text:`⚡ EL ROI [Bot${b.id}]\n${msg}`,parse_mode:'HTML'})
    });
  } catch(e){ log(b,'Telegram error: '+e.message); }
}

// ── TREND ─────────────────────────────────────────
function analyzeTrend(b,tf) {
  const data=b.candles[tf];
  if(!data||data.length<10) return;
  const recent=data.slice(-20);
  const highs=recent.map(c=>c.high),lows=recent.map(c=>c.low);
  let lh=0,ll=0,hh=0,hl=0;
  for(let i=1;i<highs.length;i++){
    if(highs[i]<highs[i-1])lh++;else hh++;
    if(lows[i]<lows[i-1])ll++;else hl++;
  }
  const total=highs.length-1;
  const ds=(lh+ll)/(total*2),us=(hh+hl)/(total*2);
  b.trendStatus[tf]=ds>=0.6?'down':us>=0.6?'up':'neutral';
  const dc=Object.values(b.trendStatus).filter(t=>t==='down').length;
  b.confirmedTrend=dc>=b.cfg.minTFConfirm;
  if(!b.backtest) broadcast({type:'trend',id:b.id,trendStatus:b.trendStatus,confirmedTrend:b.confirmedTrend});
}

// ── PRICE SCALE (forex vs synthetic indices) ──────
// The structure-detection code below compares/rounds prices using small
// absolute numbers (0.05, 0.01, .toFixed(2)) that were tuned for Volatility
// indices, which trade in the hundreds/thousands. Forex pairs trade around
// 1.0–1.5 (or ~100-150 for JPY pairs), so those same absolute numbers are
// either enormous (0.05 on EUR/USD is a 500-pip "same level" tolerance) or
// meaninglessly coarse (.toFixed(2) throws away everything past the pip).
// These helpers scale those numbers to the traded market instead — for any
// synthetic index they return the exact original numbers, unchanged.
function getMarketPrecisionDigits(market) {
  if (market && market.startsWith('frx')) return market.includes('JPY') ? 3 : 5; // ~tenth-of-a-pip
  if (market === 'cryBTCUSD') return 1;
  if (market === 'cryETHUSD') return 2;
  return 2; // synthetic indices — original behavior, unchanged
}
function levelKey(market, price) { return price.toFixed(getMarketPrecisionDigits(market)); }
function getMarketTolerance(market, kind) {
  // kind: 'struct' = "is this the same structure as an existing one" (was 0.05)
  //       'level'  = "is this the same traded/ignored level"          (was 0.01)
  if (market && market.startsWith('frx')) {
    const pip = market.includes('JPY') ? 0.01 : 0.0001;
    return kind === 'struct' ? pip * 5 : pip * 2; // 5 pips / 2 pips
  }
  if (market === 'frxXAUUSD') return kind === 'struct' ? 0.5 : 0.2;
  if (market === 'cryBTCUSD') return kind === 'struct' ? 50 : 20;
  if (market === 'cryETHUSD') return kind === 'struct' ? 5 : 2;
  return kind === 'struct' ? 0.05 : 0.01; // synthetic indices — original behavior, unchanged
}

// ── STRUCTURE DETECTION — DO NOT MODIFY ──────────
function findStructuresInData(b,data) {
  if(data.length<10) return {smallStruct:null,bigStruct:null};
  const LR=2,peaks=[];
  for(let i=LR;i<data.length-LR;i++){
    let top=true;
    for(let j=i-LR;j<=i+LR;j++){
      if(j!==i&&data[j].high>=data[i].high){top=false;break;}
    }
    if(top) peaks.push({price:data[i].high,index:i});
  }
  if(peaks.length<2) return {smallStruct:null,bigStruct:null};

  function findBestGroup(minSpan,maxSpan){
    let best=null;
    for(let s=0;s<peaks.length-1;s++){
      const sp0=peaks[s+1].index-peaks[s].index;
      if(sp0<minSpan||sp0>maxSpan) continue;
      if(peaks[s+1].price>=peaks[s].price) continue;
      const bd=peaks[s].price-peaks[s+1].price;
      if(bd<=0) continue;
      const grp=[peaks[s],peaks[s+1]];
      for(let j=s+2;j<peaks.length;j++){
        const prev=grp[grp.length-1];
        const sp=peaks[j].index-prev.index;
        if(sp<minSpan||sp>maxSpan) continue;
        if(peaks[j].price>=prev.price) continue;
        const diff=prev.price-peaks[j].price;
        if(Math.abs(diff-bd)/bd<=0.10) grp.push(peaks[j]);
      }
      if(grp.length>=2){
        const tol=maxSpan===5?b.cfg.smallTol:b.cfg.bigTol;
        const cs=data.length-1-grp[grp.length-1].index;
        if(cs>tol) continue;
        const lp=grp[grp.length-1].price;
        let broken=false;
        for(let k=grp[grp.length-1].index+1;k<data.length;k++){
          if(Math.max(data[k].open,data[k].close)>lp+getMarketTolerance(b.cfg.market,'struct')){broken=true;break;}
        }
        if(broken) continue;
        if(!best||grp.length>best.peaks.length) best={peaks:grp,baseDiff:bd};
      }
    }
    return best;
  }
  return {smallStruct:findBestGroup(2,5),bigStruct:findBestGroup(5,15)};
}

// ── ZONE HELPERS ─────────────────────────────────
function isLevelInDoNotTradeZone(b,level) {
  return b.doNotTradeZones.some(z=>{
    const lo=Math.min(z.a,z.b),hi=Math.max(z.a,z.b);
    return level>=lo&&level<=hi;
  });
}

// HTF Zone: price inside zone (A is top, B is bottom)
function isPriceInHTFZone(b,price) {
  return b.htfZones.find(z=>{
    if(z.cancelled) return false;
    const hi=Math.max(z.a,z.b), lo=Math.min(z.a,z.b);
    return price<=hi && price>=lo;
  });
}

// ── AUTO HTF DETECTION ────────────────────────────
// Runs on H1, H4, D1. Detects swing lows, classifies structure,
// builds zone using htfClosePct / htfPassPct.
function detectAutoHTFZones(b) {
  const HTF_TFS  = ['M30','H1','H4'];
  const closePct = (b.cfg.htfClosePct||20) / 100;
  const passPct  = (b.cfg.htfPassPct||30)  / 100;
  const ALMOST_EQUAL_TOL = 0.005; // 0.5%
  const LR = 3; // swing low lookback radius
  const digits = getMarketPrecisionDigits(b.cfg.market);

  // ── RECENCY LIMITS — how many candles back to search per TF ──────────
  // H1:  50 candles = ~2 days   (recent market memory)
  // H4:  30 candles = ~5 days
  // D1:  20 candles = ~4 weeks
  // M30: 48 candles = ~1 day | H1: 48 candles = ~2 days | H4: 30 candles = ~5 days
  const RECENCY = { M30:48, H1:48, H4:30 };

  const autoZones      = [];
  const autoStructures = [];

  for(const tf of HTF_TFS){
    const data = b.candles[tf];
    if(!data||data.length < LR*2+2) continue;

    // Only look at recent candles — market has forgotten old swing lows
    const lookback = RECENCY[tf] || 50;
    const recent   = data.slice(-lookback);

    // ── Find swing lows (troughs) within recent window ────────────────────
    const troughs = [];
    for(let i=LR; i<recent.length-LR; i++){
      let isTrough=true;
      for(let j=i-LR; j<=i+LR; j++){
        if(j!==i && recent[j].low<=recent[i].low){ isTrough=false; break; }
      }
      if(isTrough) troughs.push({
        low:   recent[i].low,
        high:  recent[i].high,
        range: recent[i].high - recent[i].low,
        idx:   i,
      });
    }
    if(troughs.length < 1) continue;

    // ── VALIDITY CHECK — most recent swing low must not be too old
    const sl1Recency = recent.length - 1 - troughs[troughs.length-1].idx;
    if(sl1Recency > 15) continue;

    // ── ALL VALID ZONES — no priority, no limit per TF ─────────────────
    // Every valid structure gets a zone. All protect you from losing.

    // ── PATTERN 1 & 2 — single swing low, price may return ───────────────
    const sl1 = troughs[troughs.length-1];
    const range12   = sl1.range > 0 ? sl1.range : sl1.low * 0.002;
    const zA_12     = parseFloat((sl1.low + closePct * range12).toFixed(digits));
    const zB_12     = parseFloat((sl1.low - passPct  * range12).toFixed(digits));
    const id12      = `auto_12_${tf}_${sl1.low.toFixed(4)}`;
    const existCancel12 = b.htfZones.find(z=>z.id===id12&&z.cancelled);
    const zone12Broken  = recent.some(c => c.close < zB_12);
    if(!existCancel12 && !zone12Broken){
      autoZones.push({ a:zA_12, b:zB_12, source:'auto', id:id12,
        label:`${tf} SL1+2 (${sl1.low.toFixed(digits)})`, cancelled:false });
      autoStructures.push({ tf, type:'12', sl1:sl1.low, sl1high:sl1.high,
        zoneA:zA_12, zoneB:zB_12, id:id12 });
    }

    // ── STRUCTURES FROM PAIRS OF SWING LOWS ──────────────────────────────
    // Check every consecutive pair — each valid pair gets its own zone
    for(let t=troughs.length-1; t>=1; t--){
      const slA = troughs[t];     // more recent
      const slB = troughs[t-1];   // older

      const diff          = slA.low - slB.low; // positive = ascending
      const absDiff       = Math.abs(diff);
      const avgLow        = (slA.low + slB.low) / 2;
      const isAlmostEqual = absDiff / avgLow < ALMOST_EQUAL_TOL;

      let zoneA, zoneB, structType, nextLevel;

      if(isAlmostEqual){
        const r = slA.range > 0 ? slA.range : slA.low * 0.002;
        nextLevel  = slA.low;
        zoneA      = parseFloat((nextLevel + closePct * r).toFixed(digits));
        zoneB      = parseFloat((nextLevel - passPct  * r).toFixed(digits));
        structType = 'equal';
      } else if(diff > 0){
        // Ascending — next bounce expected higher
        const baseDiff = diff;
        nextLevel  = parseFloat((slA.low + baseDiff).toFixed(digits));
        zoneA      = parseFloat((nextLevel + closePct * baseDiff).toFixed(digits));
        zoneB      = parseFloat((nextLevel - passPct  * baseDiff).toFixed(digits));
        structType = 'ascending';
      } else {
        // Descending — next bounce expected lower
        const baseDiff = absDiff;
        nextLevel  = parseFloat((slA.low - baseDiff).toFixed(digits));
        zoneA      = parseFloat((nextLevel + closePct * baseDiff).toFixed(digits));
        zoneB      = parseFloat((nextLevel - passPct  * baseDiff).toFixed(digits));
        structType = 'descending';
      }

      const idFull      = `auto_${structType}_${tf}_${slA.low.toFixed(4)}_${slB.low.toFixed(4)}`;
      const existCancel = b.htfZones.find(z=>z.id===idFull&&z.cancelled);
      const zoneBroken  = recent.some(c => c.close < zoneB);
      if(!existCancel && !zoneBroken){
        autoZones.push({ a:zoneA, b:zoneB, source:'auto', id:idFull,
          label:`${tf} ${structType} NEXT:${nextLevel.toFixed(digits)}`, cancelled:false });
        autoStructures.push({ tf, type:structType,
          sl1:slA.low, sl2:slB.low, next:nextLevel,
          zoneA, zoneB, id:idFull });
      }
    }
  }

  // Keep manual zones and cancelled markers, replace auto zones
  const keepZones    = b.htfZones.filter(z=>z.source==='manual'||z.cancelled);
  b.htfZones         = [...autoZones, ...keepZones];
  b.autoHtfStructures = autoStructures;
}

// ── FIND LEVELS — EVERY SECOND ────────────────────
function findLevels(b) {
  // Use scanTFs array — set in settings, can be any combo of M1,M5,M15,M30,H1,H4
  const tfs=Array.isArray(b.cfg.scanTFs)&&b.cfg.scanTFs.length>0?b.cfg.scanTFs:['M1','M5'];
  const newStructures=[];

  for(const tf of tfs){
    const data=b.candles[tf];
    if(!data||data.length<10) continue;
    const result=findStructuresInData(b,data);

    if(result.smallStruct){
      const existing=b.activeStructures.find(s=>s.type==='small'&&s.tf===tf&&Math.abs(s.peaks[0].price-result.smallStruct.peaks[0].price)<getMarketTolerance(b.cfg.market,'struct'));
      const tradedLevels=existing?existing.tradedLevels:new Set();
      newStructures.push({
        ...result.smallStruct, type:'small',tf,tradedLevels,
        projectedLevels:computeProjectedLevels(b,result.smallStruct,tradedLevels),
        id:`small_${tf}_${levelKey(b.cfg.market,result.smallStruct.peaks[0].price)}`
      });
    }
    if(result.bigStruct){
      const existing=b.activeStructures.find(s=>s.type==='big'&&s.tf===tf&&Math.abs(s.peaks[0].price-result.bigStruct.peaks[0].price)<getMarketTolerance(b.cfg.market,'struct'));
      const tradedLevels=existing?existing.tradedLevels:new Set();
      newStructures.push({
        ...result.bigStruct, type:'big',tf,tradedLevels,
        projectedLevels:computeProjectedLevels(b,result.bigStruct,tradedLevels),
        id:`big_${tf}_${levelKey(b.cfg.market,result.bigStruct.peaks[0].price)}`
      });
    }
  }

  b.activeStructures=newStructures;

  if(b.activeStructures.length>0){
    setTicker(b,`📐 ${b.activeStructures.length} struct(s) | ${b.activeStructures.map(s=>`${s.type}(${s.tf})`).join(', ')}`);
  } else {
    setTicker(b,'⏳ Scanning for structures...');
  }

  const downCount=Object.values(b.trendStatus).filter(t=>t==='down').length;
  if(downCount>=2){
    b.activeStructures.forEach(s=>{
      if(s.projectedLevels&&s.projectedLevels.length>0){
        const np=s.projectedLevels[0];
        if(!s._lastTeleLevel||Math.abs(s._lastTeleLevel-np)>getMarketTolerance(b.cfg.market,'level')){
          s._lastTeleLevel=np;
          const pd=getMarketPrecisionDigits(b.cfg.market);
          const r1=s.peaks.length>=2?s.peaks[s.peaks.length-2].price:null;
          const r2=s.peaks[s.peaks.length-1].price;
          const diff=r1?Math.abs(r1-r2).toFixed(pd):s.baseDiff.toFixed(pd);
          const mkt=MKT_NAMES[b.cfg.market]||b.cfg.market;
          telegram(b,`🎯 <b>NEXT LEVEL ACTIVE</b>\nLevel: <b>${np.toFixed(pd)}</b>\n${r1?`R1: ${r1.toFixed(pd)} | R2: ${r2.toFixed(pd)}\n`:''}Diff: ${diff}\nMarket: ${mkt}\nCommand: ${b.cfg.command}\nStruct: ${s.type.toUpperCase()} (${s.tf})`);
        }
      }
    });
  }

  broadcastBotState(b);
}

function computeProjectedLevels(b,struct,tradedLevels) {
  if(!struct||!struct.peaks||struct.peaks.length<1) return [];
  const digits=getMarketPrecisionDigits(b.cfg.market);
  const lastLevel=struct.peaks[struct.peaks.length-1].price;
  let np=parseFloat((lastLevel-struct.baseDiff).toFixed(digits));
  let safety=0;
  while(safety<20){
    if(
      !tradedLevels.has(levelKey(b.cfg.market,np)) &&
      !isLevelInDoNotTradeZone(b,np) &&
      !b.ignoredLevels.has(levelKey(b.cfg.market,np))
    ){
      return [np];
    }
    np=parseFloat((np-struct.baseDiff).toFixed(digits));
    safety++;
  }
  return [];
}

// ── HTF ZONE UPTREND DETECTION ────────────────────
function checkHTFZoneUptrend(b) {
  const activeZones = b.htfZones.filter(z=>!z.cancelled);
  if(!b.currentPrice) return;

  // ── RESUME CHECKS — run regardless of whether price is in a zone ─────
  if(b.htfZonePaused){
    const triggerZone = b.activeHtfZoneId
      ? activeZones.find(z=>z.id===b.activeHtfZoneId)
      : null;

    // Resume condition 1: price broke below zone B of the zone that triggered pause
    if(triggerZone){
      const zoneB = Math.min(triggerZone.a, triggerZone.b);
      if(b.currentPrice < zoneB){
        log(b,'✅ Price broke below HTF zone B — erasing zone and resuming');
        // Erase the zone completely — market proved it doesn't care
        b.htfZones = b.htfZones.filter(z=>z.id!==triggerZone.id);
        b.htfZonePaused  = false;
        b.htfPauseReason = '';
        b.activeHtfZoneId = null;
        setStatus(b,'running','RUNNING');
        setTicker(b,'✅ HTF zone broken — resuming...');
        broadcastBotState(b);
        return;
      }
    }

    // Resume condition 2: at least 2 of M1,M5,M15 turned uptrend
    ['M1','M5','M15'].forEach(tf=>{ if(b.candles[tf]&&b.candles[tf].length>=10) analyzeTrend(b,tf); });
    const upCount = ['M1','M5','M15'].filter(tf=>b.trendStatus[tf]==='up').length;
    if(upCount >= 2){
      log(b,`✅ ${upCount}/3 TFs uptrend — erasing HTF zone and resuming`);
      // Erase the zone that triggered pause
      if(b.activeHtfZoneId){
        b.htfZones = b.htfZones.filter(z=>z.id!==b.activeHtfZoneId);
      }
      b.htfZonePaused   = false;
      b.htfPauseReason  = '';
      b.activeHtfZoneId = null;
      setStatus(b,'running','RUNNING');
      setTicker(b,'✅ 2+ TFs uptrend — HTF resolved, resuming...');
      broadcastBotState(b);
      return;
    }
    // Still paused — do nothing more this tick
    return;
  }

  // ── NOT PAUSED — check if price entered a zone ───────────────────────
  if(!activeZones.length) return;
  const nearZone = isPriceInHTFZone(b, b.currentPrice);
  if(!nearZone) return;

  // ── UPTREND STRUCTURE DETECTION on M1, M5, M15 ───────────────────────
  const TFS_TO_CHECK = ['M1','M5','M15'];
  let uptrendDetected = false;
  let pauseReason = '';

  for(const tf of TFS_TO_CHECK){
    const data = b.candles[tf];
    if(!data||data.length<20) continue;
    const recent = data.slice(-40);

    const sLows=[], sHighs=[];
    for(let i=2;i<recent.length-2;i++){
      if(recent[i].low < recent[i-1].low && recent[i].low < recent[i-2].low &&
         recent[i].low < recent[i+1].low && recent[i].low < recent[i+2].low)
        sLows.push({price:recent[i].low, high:recent[i].high, idx:i});
      if(recent[i].high > recent[i-1].high && recent[i].high > recent[i-2].high &&
         recent[i].high > recent[i+1].high && recent[i].high > recent[i+2].high)
        sHighs.push({price:recent[i].high, idx:i});
    }

    if(sLows.length>=2 && sHighs.length>=1){
      const lastLow  = sLows[sLows.length-1];
      const prevLow  = sLows[sLows.length-2];
      const lastHigh = sHighs[sHighs.length-1];

      // Condition 1: Higher Low + Break Above Swing High
      if(lastLow.price > prevLow.price && lastLow.idx > prevLow.idx){
        if(b.currentPrice > lastHigh.price && lastHigh.idx > prevLow.idx){
          uptrendDetected = true;
          pauseReason = `Higher low + break above swing high on ${tf}`;
          break;
        }
      }
      // Condition 2: Lower Low then price breaks above that candle's high
      if(lastLow.price < prevLow.price && lastLow.idx > prevLow.idx){
        if(b.currentPrice > lastLow.high){
          uptrendDetected = true;
          pauseReason = `Lower low + break above its high on ${tf}`;
          break;
        }
      }
    }
  }

  if(uptrendDetected){
    log(b,`⚠ HTF pause: ${pauseReason} in zone ${nearZone.id}`);
    b.htfZonePaused   = true;
    b.htfPauseReason  = pauseReason;
    b.activeHtfZoneId = nearZone.id; // track WHICH zone triggered pause
    // Mark the zone as active for color change on dashboard
    nearZone.active   = true;
    setStatus(b,'scanning','PAUSED — HTF ZONE');
    const lo=Math.min(nearZone.a,nearZone.b), hi=Math.max(nearZone.a,nearZone.b);
    const pd=getMarketPrecisionDigits(b.cfg.market);
    setTicker(b,`⚠ HTF zone ${lo.toFixed(pd)}–${hi.toFixed(pd)} — ${pauseReason}`);
    telegram(b,`⚠ <b>Bot paused — HTF Zone</b>\n${pauseReason}\nZone: ${lo.toFixed(pd)}–${hi.toFixed(pd)}\nResumes: price below zone B OR 2+ TFs uptrend`);
    broadcastBotState(b);
  }
}

// ── ENTRY CHECK ───────────────────────────────────
function checkEntry(b) {
  if(!b.botActive||!b.confirmedTrend) return;
  // For multiplier — only one trade at a time (needs contractId to sell)
  const isMulti=b.cfg.command==='CALL_MULT'||b.cfg.command==='PUT_MULT'||b.cfg.command==='VANILLA_CALL'||b.cfg.command==='VANILLA_PUT';
  if(isMulti&&b.inTrade) return;
  if(b.lossCountdownPaused||b.timeOffPaused||b.htfZonePaused) return;
  if(!b.activeStructures.length) return;
  if(b.cfg.maxTrades>0&&b.tradeCount>=b.cfg.maxTrades){stopBot(b);return;}

  const data=b.candles['M1'];
  if(!data||data.length<3) return;

  for(const struct of b.activeStructures){
    if(!struct.projectedLevels||!struct.projectedLevels.length) continue;
    const target=struct.projectedLevels[0];
    if(struct.tradedLevels.has(levelKey(b.cfg.market,target))) continue;
    if(isLevelInDoNotTradeZone(b,target)) continue;
    if(b.ignoredLevels.has(levelKey(b.cfg.market,target))) continue;

    const pct=b.cfg.proximityPct/100;
    const bd=struct.baseDiff||5;
    const maxGap=bd*(1-pct);
    const confirmCount=struct.type==='small'?b.cfg.smallConfirm:b.cfg.bigConfirm;

    let et=b.entryTargets.find(e=>e.structId===struct.id&&Math.abs(e.level-target)<getMarketTolerance(b.cfg.market,'level'));
    if(!et){ et={structId:struct.id,level:target,pricePassed:false,passedCount:0}; b.entryTargets.push(et); }

    if(!et.pricePassed){
      let count=0;
      for(let i=data.length-1;i>=Math.max(0,data.length-40);i--){
        if(Math.max(data[i].open,data[i].close)<target) count++;
        else break;
      }
      if(count>=confirmCount){ et.pricePassed=true; et.passedCount=count;
        setTicker(b,`✅ ${count} candles below ${target.toFixed(getMarketPrecisionDigits(b.cfg.market))} [${struct.type}/${struct.tf}] — waiting pullback...`);
      } else { continue; }
    }

    if(b.currentPrice>=target){ et.pricePassed=false; et.passedCount=0; continue; }
    if(b.currentPrice<target-maxGap) continue;

    const last=data[data.length-1],prev=data[data.length-2];
    if(last.close<=prev.close) continue;

    setTicker(b,`⚡ ENTRY! ${b.currentPrice.toFixed(getMarketPrecisionDigits(b.cfg.market))} at ${target.toFixed(getMarketPrecisionDigits(b.cfg.market))} [${struct.type}/${struct.tf}]`);
    struct.tradedLevels.add(levelKey(b.cfg.market,target));
    struct.projectedLevels=computeProjectedLevels(b,struct,struct.tradedLevels);
    b.entryTargets=b.entryTargets.filter(e=>!(e.structId===struct.id&&Math.abs(e.level-target)<getMarketTolerance(b.cfg.market,'level')));
    placeTrade(b,{level:target,structType:struct.type});
    return;
  }
}

// ── PLACE TRADE ───────────────────────────────────
// New Deriv API: proposal first → buy with proposal_id
function placeTrade(b,meta={}) {
  if(b.backtest){ backtestPlaceTrade(b,meta); return; }
  if(!b.derivWs||b.derivWs.readyState!==WebSocket.OPEN){
    log(b,'❌ placeTrade: WebSocket not open'); return;
  }
  const isMulti=b.cfg.command==='CALL_MULT'||b.cfg.command==='PUT_MULT';
  const isVanilla=b.cfg.command==='VANILLA_CALL'||b.cfg.command==='VANILLA_PUT';
  if(isMulti||isVanilla) b.inTrade=true;
  const duration=b.cfg.durationMins*60;
  if(!b.pendingTrades) b.pendingTrades=[];
  b.pendingTrades.push({
    level:meta.level??null, structType:meta.structType??null,
    command:b.cfg.command, stake:b.cfg.stake, market:b.cfg.market,
    isMulti:isMulti||isVanilla,
    placedAt:Date.now(),
  });
  const type={NOTOUCH:'NOTOUCH',TOUCH:'ONETOUCH',HIGHER:'CALL',LOWER:'PUT',RISE:'CALL',FALL:'PUT',CALL_MULT:'MULTUP',PUT_MULT:'MULTDOWN',VANILLA_CALL:'VANILLALONGCALL',VANILLA_PUT:'VANILLALONGPUT'}[b.cfg.command]||'NOTOUCH';

  // Build proposal parameters (OAuth/OTP connection uses 'underlying_symbol')
  const params = {
    contract_type: type,
    basis:         'stake',
    amount:        b.cfg.stake,
    currency:      'USD',
    underlying_symbol: b.cfg.market,
  };
  if(isMulti){
    params.multiplier = b.cfg.multiplier;
  } else if(isVanilla){
    params.duration      = duration;
    params.duration_unit = 's';
    if(b.cfg.vanillaStrike!==null&&b.cfg.vanillaStrike!==''&&b.cfg.vanillaStrike!==undefined){
      // Send exactly as typed — e.g. "+3.10" or "-3.10"
      params.barrier = String(b.cfg.vanillaStrike);
    }
  } else {
    params.duration      = duration;
    params.duration_unit = 's';
    if(['NOTOUCH','TOUCH','HIGHER','LOWER'].includes(b.cfg.command)){
      params.barrier = b.cfg.barrierOffset;
    }
  }

  const proposalMsg = { proposal:1, subscribe:1, ...params };
  log(b,`📤 Proposal: ${type} ${b.cfg.market} $${b.cfg.stake}${isMulti?` x${b.cfg.multiplier}`:isVanilla?` strike:${b.cfg.vanillaStrike} dur:${duration}s`:` dur:${duration}s`}`);
  b.derivWs.send(JSON.stringify(proposalMsg));
  broadcastBotState(b);
}

// ── HANDLE PROPOSAL RESPONSE — BUY ON RECEIPT ────
function handleProposal(b,d){
  if(d.error){
    log(b,`❌ Proposal error [${d.error.code}]: ${d.error.message}`);
    log(b,`❌ Full error: ${JSON.stringify(d.error)}`);
    b.inTrade=false; broadcastBotState(b); return;
  }
  const proposal=d.proposal;
  if(!proposal||!proposal.id){
    log(b,'❌ Proposal: no id returned — '+JSON.stringify(d));
    b.inTrade=false; broadcastBotState(b); return;
  }
  log(b,`📋 Proposal received: ${proposal.id} | payout: ${proposal.payout}`);
  // Immediately buy using the proposal id
  const buyMsg={ buy: proposal.id, price: b.cfg.stake };
  log(b,`📤 Buying proposal ${proposal.id}...`);
  b.derivWs.send(JSON.stringify(buyMsg));
}

// ── LOSS CONTROL ──────────────────────────────────
// ── TRADE TIMER ──────────────────────────────────
function startTradeTimer(b,contractId,durationSecs){
  stopTradeTimer(b,contractId);
  if(!b.activeTradeTimers) b.activeTradeTimers={};
  b.activeTradeTimers[contractId]={
    remaining:durationSecs, total:durationSecs,
    timer:setInterval(()=>{
      const t=b.activeTradeTimers?.[contractId];
      if(!t) return;
      t.remaining--;
      broadcast({type:'trade_timer',id:b.id,contractId,remaining:t.remaining,total:t.total});
      if(t.remaining<=0) stopTradeTimer(b,contractId);
    },1000)
  };
  broadcast({type:'trade_timer',id:b.id,contractId,remaining:durationSecs,total:durationSecs});
}
function stopTradeTimer(b,contractId){
  if(!b.activeTradeTimers) return;
  if(contractId){
    const t=b.activeTradeTimers[contractId];
    if(t){clearInterval(t.timer);delete b.activeTradeTimers[contractId];}
    broadcast({type:'trade_timer_stop',id:b.id,contractId});
  } else {
    Object.keys(b.activeTradeTimers).forEach(cid=>{
      clearInterval(b.activeTradeTimers[cid].timer);
      broadcast({type:'trade_timer_stop',id:b.id,contractId:cid});
    });
    b.activeTradeTimers={};
  }
}

function startLossCountdown(b,totalSecs) {
  stopLossCountdown(b);
  b.lossCountdownPaused=true; b.lossCountdownRemaining=totalSecs; b.lossCountdownTotal=totalSecs;
  log(b,`⏸ Cooldown: ${totalSecs===1800?'30 MIN':totalSecs===3600?'1 HR':'4 HR'}`);
  setStatus(b,'scanning','PAUSED — COOLDOWN');
  b.lossCountdownTimer=setInterval(()=>{
    b.lossCountdownRemaining--;
    broadcast({type:'loss_countdown',id:b.id,remaining:b.lossCountdownRemaining,total:b.lossCountdownTotal});
    if(b.lossCountdownRemaining<=0) resumeAfterCooldown(b);
  },1000);
}
function stopLossCountdown(b){ if(b.lossCountdownTimer){clearInterval(b.lossCountdownTimer);b.lossCountdownTimer=null;} }
function resumeAfterCooldown(b){
  b.lossCountdownPaused=false; stopLossCountdown(b);
  log(b,'✅ Cooldown done'); setStatus(b,'running','RUNNING');
  setTicker(b,'✅ Cooldown done — scanning...'); broadcastBotState(b);
  if(b.botActive) findLevels(b);
}

function startTimeOff(b,totalSecs) {
  stopTimeOff(b);
  b.timeOffPaused=true; b.timeOffRemaining=totalSecs; b.timeOffTotal=totalSecs;
  log(b,`⏰ Time off: ${totalSecs===1200?'20 MIN':totalSecs===1800?'30 MIN':'1 HR'}`);
  setStatus(b,'scanning','TIME OFF');
  b.timeOffTimer=setInterval(()=>{
    b.timeOffRemaining--;
    broadcast({type:'time_off',id:b.id,remaining:b.timeOffRemaining,total:b.timeOffTotal});
    if(b.timeOffRemaining<=0) resumeAfterTimeOff(b);
  },1000);
}
function stopTimeOff(b){ if(b.timeOffTimer){clearInterval(b.timeOffTimer);b.timeOffTimer=null;} }
function resumeAfterTimeOff(b){
  b.timeOffPaused=false; stopTimeOff(b);
  log(b,'✅ Time off done'); setStatus(b,'running','RUNNING');
  setTicker(b,'✅ Time off done — scanning...'); broadcastBotState(b);
  if(b.botActive) findLevels(b);
}

// ── RESULT ────────────────────────────────────────
function finalizeResult(b,profit,contractInfo,cid) {
  stopTradeTimer(b,cid);
  if(contractInfo?.isMulti) b.inTrade=false;
  b.tradeCount++; b.sessionPnl+=profit;
  const won=profit>0;
  if(won) b.wins++; else b.losses++;
  const wr=Math.round((b.wins/b.tradeCount)*100);
  const level=contractInfo?.level;
  const command=contractInfo?.command||b.cfg.command;
  const market=contractInfo?.market||b.cfg.market;
  const stake=contractInfo?.stake??b.cfg.stake;
  const card={
    id:Date.now(), tradeNum:b.tradeCount,
    time:new Date().toLocaleTimeString(), date:new Date().toLocaleDateString(),
    timestamp:Date.now(), won, profit,
    level:typeof level==='number'?level.toFixed(getMarketPrecisionDigits(market)):null,
    struct:contractInfo?.structType,
    command, market, stake, wr, contractId:cid,
  };
  b.tradeLog.unshift(card);
  if(b.tradeLog.length>500) b.tradeLog.pop();
  saveData();
  log(b,`${won?'✅ WIN':'❌ LOSS'} #${b.tradeCount} | ${profit>=0?'+':''}$${profit.toFixed(2)} | WR:${wr}%`);
  const mkt=MKT_NAMES[market]||market;
  telegram(b,`${won?'✅ WIN':'❌ LOSS'}\nLevel: <b>${card.level??'—'}</b>\nProfit: <b>${profit>=0?'+':''}$${profit.toFixed(2)}</b>\nMarket: ${mkt}\nCommand: ${command}\nWR: ${wr}%`);
  broadcast({type:'trade',id:b.id,card});
  broadcastBotState(b);

  if(won){
    b.consecutiveLosses=0;
    setTicker(b,`✅ WIN +$${profit.toFixed(2)} — scanning...`);
    setTimeout(()=>{if(b.botActive)findLevels(b);},1000);
  } else {
    b.consecutiveLosses++;
    if(b.consecutiveLosses>=b.cfg.maxConsecLosses){
      b.botActive=false; stopLossCountdown(b); stopScanner(b);
      setStatus(b,'stopped',`STOPPED — ${b.cfg.maxConsecLosses} LOSSES`);
      setTicker(b,`🛑 ${b.cfg.maxConsecLosses} losses — restart manually`);
      log(b,`🛑 Stopped after ${b.cfg.maxConsecLosses} consecutive losses`);
      broadcastBotState(b);
    } else {
      setTicker(b,'❌ LOSS — cooldown starting...');
      if(b.cfg.cooldownEnabled!==false){
      if(b.cfg.cooldownEnabled!==false){ startLossCountdown(b,b.cfg.cooldownSecs); } else { log(b,'⏭ Cooldown disabled'); if(b.botActive) findLevels(b); }
    } else {
      log(b,'⏭ Cooldown disabled — resuming immediately');
      if(b.botActive) findLevels(b);
    }
    }
  }
}

// ── OAUTH 2.0 PKCE — LIVE LOGIN ───────────────────

// Step 1: Dashboard requests login URL for a bot
// Returns the Deriv OAuth URL with PKCE params
function buildOAuthUrl() {
  const verifier  = generateCodeVerifier();
  const challenge = generateCodeChallenge(verifier);
  const state     = base64url(crypto.randomBytes(16));

  oauthPending.set(state, { verifier });
  setTimeout(()=>oauthPending.delete(state), 10*60*1000);

  const params = new URLSearchParams({
    response_type:          'code',
    client_id:              APP_ID_LIVE,
    redirect_uri:           REDIRECT_URI,
    scope:                  'trade',
    state,
    code_challenge:         challenge,
    code_challenge_method:  'S256',
  });

  return `https://auth.deriv.com/oauth2/auth?${params.toString()}`;
}

// Fetch a fresh OTP (authenticated WebSocket URL) for the shared account.
// Used both right after login and whenever any bot needs to (re)open its
// own socket to that same account.
async function getSharedOtpUrl() {
  if(!liveAuth.loggedIn || !liveAuth.accessToken || !liveAuth.accountId){
    throw new Error('Not logged in to Deriv');
  }
  const otpRes = await fetch(`https://api.derivws.com/trading/v1/options/accounts/${liveAuth.accountId}/otp`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${liveAuth.accessToken}`,
      'Deriv-App-ID':  APP_ID_LIVE,
    },
  });
  const otpData = await otpRes.json();
  if(!otpData.data || !otpData.data.url){
    throw new Error('OTP endpoint did not return a WebSocket URL');
  }
  return otpData.data.url;
}

// Step 2: Deriv redirects to /callback with ?code=...&state=...
app.get('/callback', async (req, res) => {
  const { code, state, error } = req.query;

  if(error) {
    return res.send(`<script>window.close();</script><p>Login failed: ${error}. You can close this window.</p>`);
  }

  if(!code||!state) {
    return res.send('<script>window.close();</script><p>Missing code or state. Close this window and try again.</p>');
  }

  const pending = oauthPending.get(state);
  if(!pending) {
    return res.send('<script>window.close();</script><p>Session expired or invalid. Close this window and try again.</p>');
  }

  oauthPending.delete(state);
  const { verifier } = pending;

  const page = (inner) => `<!DOCTYPE html><html><head><title>EL ROI — Connect to Deriv</title>
  <style>body{background:#03060f;color:#00d4ff;font-family:monospace;display:flex;align-items:center;justify-content:center;height:100vh;flex-direction:column;gap:16px;padding:20px;text-align:center}
  .spin{width:40px;height:40px;border:3px solid #152840;border-top-color:#00d4ff;border-radius:50%;animation:spin 0.8s linear infinite;}
  @keyframes spin{to{transform:rotate(360deg);}}
  .acctbtn{background:#0a1220;border:1px solid #1e3a52;color:#e5f6ff;border-radius:10px;padding:14px 20px;font-family:monospace;font-size:13px;cursor:pointer;min-width:220px}
  .acctbtn:hover{border-color:#00d4ff}
  .acctbtn:disabled{opacity:0.5;cursor:default}
  .err{color:#ff4d6d}</style></head>
  <body>${inner}</body></html>`;

  const logAll = (msg) => bots.forEach(b=>log(b,msg));

  try {
    logAll('🔐 Exchanging OAuth code for access token...');
    broadcast({ type: 'live_login_status', status: 'exchanging', msg: 'Exchanging auth code...' });

    const tokenRes = await fetch('https://auth.deriv.com/oauth2/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type:    'authorization_code',
        client_id:     APP_ID_LIVE,
        code,
        code_verifier: verifier,
        redirect_uri:  REDIRECT_URI,
      }).toString(),
    });

    const tokenData = await tokenRes.json();
    if(!tokenRes.ok || !tokenData.access_token) {
      throw new Error(tokenData.error_description || tokenData.error || 'Token exchange failed');
    }

    const accessToken = tokenData.access_token;
    logAll('✅ Access token obtained');
    broadcast({ type: 'live_login_status', status: 'got_token', msg: 'Access token obtained...' });

    logAll('🔍 Fetching accounts...');
    broadcast({ type: 'live_login_status', status: 'fetching_account', msg: 'Fetching accounts...' });

    let accountsRes = await fetch('https://api.derivws.com/trading/v1/options/accounts', {
      headers: { 'Authorization': `Bearer ${accessToken}`, 'Deriv-App-ID': APP_ID_LIVE },
    });
    let accountsData = await accountsRes.json();
    let accounts = accountsData.data || [];

    if(!accounts.length) {
      logAll('⚠ No account found, creating one...');
      broadcast({ type: 'live_login_status', status: 'creating_account', msg: 'Creating account...' });
      const createRes = await fetch('https://api.derivws.com/trading/v1/options/accounts', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${accessToken}`, 'Deriv-App-ID': APP_ID_LIVE, 'Content-Type': 'application/json' },
        body: JSON.stringify({ currency: 'USD', group: 'row', account_type: 'real' }),
      });
      const createData = await createRes.json();
      accounts = createData.data || [];
      if(!accounts.length) throw new Error('Could not create or find a live Options account');
    }

    // FIX: this used to grab accounts[0] and log straight in — silently
    // picking whichever account happened to come back first, live or demo.
    // Now it always shows a picker, same as bot 2, so you choose which
    // account (real or demo) actually trades.
    pendingLiveAuth = { accessToken, accounts, expires: Date.now() + 10 * 60 * 1000 };
    logAll(`✅ Found ${accounts.length} account(s) — waiting for you to choose`);

    const buttons = accounts.map(a => `
      <button class="acctbtn" onclick="pick('${a.account_id}')">
        <b>${a.account_id}</b><br>${a.currency || '—'}${a.balance != null ? ` · ${a.balance}` : ''}${(a.account_type || a.type) ? ` · ${(a.account_type || a.type).toUpperCase()}` : ''}
      </button>`).join('');

    res.send(page(`
      <p>Choose which Deriv account to connect:</p>
      <div style="display:flex;flex-direction:column;gap:10px">${buttons}</div>
      <p id="status" style="min-height:16px;font-size:12px"></p>
      <script>
        async function pick(accountId){
          document.querySelectorAll('.acctbtn').forEach(b=>b.disabled=true);
          document.getElementById('status').textContent='Connecting…';
          try {
            const r = await fetch('/api/oauth/select-account', {
              method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ accountId })
            }).then(r=>r.json());
            if (r.error) { document.getElementById('status').textContent = '❌ ' + r.error; document.querySelectorAll('.acctbtn').forEach(b=>b.disabled=false); return; }
            document.getElementById('status').textContent = '✅ Connected — you can close this window.';
            setTimeout(()=>window.close(), 1500);
          } catch (e) {
            document.getElementById('status').textContent = '❌ ' + e.message;
            document.querySelectorAll('.acctbtn').forEach(b=>b.disabled=false);
          }
        }
      </script>
    `));

  } catch(err) {
    logAll(`❌ Live login error: ${err.message}`);
    liveAuth = { accessToken:null, accountId:null, currency:null, balance:null, loggedIn:false };
    broadcast({ type: 'live_login_status', status: 'error', msg: `❌ ${err.message}` });
    bots.forEach(b=>broadcastBotState(b));
    res.send(page(`<div class="err">❌ ${err.message}</div>`));
  }
});

// User picks which account (from the list shown at /callback) to actually
// trade on — completes the login only once a choice is made.
app.post('/api/oauth/select-account', async (req, res) => {
  try {
    const { accountId } = req.body;
    if(!pendingLiveAuth || Date.now() > pendingLiveAuth.expires) {
      pendingLiveAuth = null;
      return res.status(400).json({ error: 'Login session expired — please log in with Deriv again' });
    }
    const chosen = pendingLiveAuth.accounts.find(a => a.account_id === accountId);
    if(!chosen) return res.status(400).json({ error: 'Unknown account_id — not in the list returned at login' });

    const accessToken = pendingLiveAuth.accessToken;
    broadcast({ type: 'live_login_status', status: 'getting_otp', msg: `Account ${accountId} — getting WebSocket token...` });

    liveAuth = { accessToken, accountId, currency: chosen.currency || null, balance: chosen.balance ?? null, loggedIn: true };

    const wssUrl = await getSharedOtpUrl();
    bots.forEach(b=>log(b, `✅ Logged in — account ${accountId}`));

    broadcast({ type: 'live_login_status', status: 'ready', msg: `✅ Logged in — account ${accountId}`, accountId });
    bots.forEach(b=>broadcastBotState(b));

    bots.forEach(b => {
      if(b.userStarted && (!b.derivWs || b.derivWs.readyState !== WebSocket.OPEN)) {
        connectDerivLive(b, wssUrl);
      }
    });

    pendingLiveAuth = null;
    res.json({ success: true, accountId });
  } catch(err) {
    liveAuth = { accessToken:null, accountId:null, currency:null, balance:null, loggedIn:false };
    broadcast({ type: 'live_login_status', status: 'error', msg: `❌ ${err.message}` });
    bots.forEach(b=>broadcastBotState(b));
    res.status(500).json({ error: err.message });
  }
});

// ── DERIV LIVE CONNECTION (OTP WebSocket) ─────────
function connectDerivLive(b, wssUrl) {
  if(b.derivWs){try{b.derivWs.terminate();}catch(e){}}
  log(b, '🔌 Opening authenticated live WebSocket...');
  setStatus(b,'connecting','CONNECTING');

  // The OTP URL is already authenticated — no authorize message needed
  b.derivWs = new WebSocket(wssUrl);

  b.derivWs.on('open', () => {
    log(b, '✅ Live WebSocket open — starting bot...');
    b.botActive = true;
    setStatus(b,'running','RUNNING');
    b.derivWs.send(JSON.stringify({ticks: b.cfg.market, subscribe: 1}));
    ['M1','M5','M15','M30','H1','H4'].forEach(tf=>fetchCandles(b,tf));
    startScanner(b);
    broadcastBotState(b);
  });

  b.derivWs.on('message', (raw) => {
    let d; try{d=JSON.parse(raw);}catch(e){return;}
    handleDerivMessage(b, d);
  });

  b.derivWs.on('close', () => {
    log(b,'Disconnected');
    b.botActive=false; stopScanner(b);
    setStatus(b,'stopped','DISCONNECTED');
    broadcastBotState(b);
    // Reconnect by getting a fresh OTP (access token may still be valid)
    if(b.userStarted && liveAuth.loggedIn) {
      if(b.reconnectTimer) clearTimeout(b.reconnectTimer);
      b.reconnectTimer = setTimeout(()=>refreshLiveOTP(b), 5000);
    }
  });

  b.derivWs.on('error', (e)=>log(b,'WS error: '+e.message));
}

// Reconnect a bot by getting a fresh OTP for the shared account
async function refreshLiveOTP(b) {
  log(b, '🔄 Refreshing live OTP...');
  try {
    const wssUrl = await getSharedOtpUrl();
    connectDerivLive(b, wssUrl);
  } catch(err) {
    log(b, `❌ OTP refresh failed: ${err.message} — clearing live session`);
    liveAuth = { accessToken:null, accountId:null, currency:null, balance:null, loggedIn:false };
    b.userStarted = false;
    setStatus(b,'stopped','SESSION EXPIRED');
    broadcast({ type: 'live_login_status', status: 'expired', msg: '⚠ Session expired — please login again' });
    bots.forEach(x=>broadcastBotState(x));
  }
}

// ── SHARED MESSAGE HANDLER ────────────────────────
function handleDerivMessage(b, d) {
  if(d.msg_type==='tick'){
    b.currentPrice=parseFloat(d.tick.quote);
    broadcast({type:'price',id:b.id,price:b.currentPrice});
    if(b.botActive){
      checkHTFZoneUptrend(b);
      checkEntry(b); // multiple trades allowed — no inTrade restriction
    }
  }

  if(d.msg_type==='candles'){
    const gran=d.echo_req.granularity;
    const tf=gran===60?'M1':gran===300?'M5':gran===900?'M15':gran===1800?'M30':gran===3600?'H1':gran===14400?'H4':'D1';
    b.candles[tf]=d.candles.map(c=>({time:c.epoch,open:parseFloat(c.open),high:parseFloat(c.high),low:parseFloat(c.low),close:parseFloat(c.close)}));
    if(['M1','M5','M15'].includes(tf)) analyzeTrend(b,tf);
    // Run HTF detection when H1/H4/D1 history loads
    if(['M30','H1','H4'].includes(tf)) detectAutoHTFZones(b);
    broadcast({type:'candles',id:b.id,tf,candles:b.candles[tf].slice(-100)});
  }

  if(d.msg_type==='ohlc'){
    const gran=d.ohlc.granularity;
    const tf=gran===60?'M1':gran===300?'M5':gran===900?'M15':gran===1800?'M30':gran===3600?'H1':gran===14400?'H4':'D1';
    const c={time:d.ohlc.open_time,open:parseFloat(d.ohlc.open),high:parseFloat(d.ohlc.high),low:parseFloat(d.ohlc.low),close:parseFloat(d.ohlc.close)};
    if(!b.candles[tf]) b.candles[tf]=[];
    if(b.candles[tf].length&&b.candles[tf][b.candles[tf].length-1].time===c.time) b.candles[tf][b.candles[tf].length-1]=c;
    else{b.candles[tf].push(c);if(b.candles[tf].length>300)b.candles[tf].shift();}
    if(['M1','M5','M15'].includes(tf)) analyzeTrend(b,tf);
    // Re-run HTF detection whenever H1/H4/D1 candles update
    if(['M30','H1','H4'].includes(tf)) detectAutoHTFZones(b);
    broadcast({type:'candle_update',id:b.id,tf,candle:c});
  }

  if(d.msg_type==='proposal'){
    handleProposal(b,d);
    return;
  }

  if(d.msg_type==='buy'){
    if(d.error){
      log(b,`❌ Buy error [${d.error.code||'?'}]: ${d.error.message}`);
      b.inTrade=false; broadcastBotState(b); return;
    }
    const cid=d.buy.contract_id;
    b.currentContractId=cid;
    const meta=(b.pendingTrades&&b.pendingTrades.length)?b.pendingTrades.shift():{
      level:null,structType:null,command:b.cfg.command,stake:b.cfg.stake,market:b.cfg.market,isMulti:false
    };
    b.activeContracts[cid]={
      stake:meta.stake,command:meta.command,market:meta.market,
      level:meta.level,structType:meta.structType,isMulti:meta.isMulti,
    };
    log(b,`📝 Contract: ${cid} | active: ${Object.keys(b.activeContracts).length}`);
    if(!meta.isMulti) startTradeTimer(b,cid,b.cfg.durationMins*60);
    setTimeout(()=>{
      if(b.derivWs?.readyState===WebSocket.OPEN)
        b.derivWs.send(JSON.stringify({proposal_open_contract:1,contract_id:cid,subscribe:1}));
    },2000);
  }

  if(d.msg_type==='proposal_open_contract'){
    const con=d.proposal_open_contract; if(!con) return;
    const cid=con.contract_id;
    const profit=parseFloat(con.profit)||0;
    const contractInfo=b.activeContracts[cid];
    if(!contractInfo) return; // ignore stale/unknown contracts
    // Multiplier TP/SL
    if(contractInfo.command==='CALL_MULT'||contractInfo.command==='PUT_MULT'){
      if(profit>=b.cfg.takeProfit||profit<=-b.cfg.stopLoss)
        b.derivWs.send(JSON.stringify({sell:cid,price:0}));
    }
    // Vanilla — close early when TP hit
    if((contractInfo.command==='VANILLA_CALL'||contractInfo.command==='VANILLA_PUT')&&b.cfg.vanillaTakeProfit>0&&profit>=b.cfg.vanillaTakeProfit){
      log(b,`🎯 Vanilla TP hit $${profit.toFixed(2)} — closing early`);
      b.derivWs.send(JSON.stringify({sell:cid,price:0}));
    }
    // Finalize when done
    if(con.status==='sold'||con.status==='lost'||con.status==='won'||con.is_expired||con.is_settleable){
      delete b.activeContracts[cid];
      finalizeResult(b,profit,contractInfo,cid);
    }
  }

  if(d.msg_type==='sell'){
    if(d.sell){
      const cid=d.sell.contract_id;
      const contractInfo=b.activeContracts[cid];
      if(!contractInfo) return;
      delete b.activeContracts[cid];
      finalizeResult(b,parseFloat(d.sell.sold_for)-contractInfo.stake,contractInfo,cid);
    }
  }
}

function fetchCandles(b,tf) {
  if(!b.derivWs||b.derivWs.readyState!==WebSocket.OPEN) return;
  const gran=tf==='M1'?60:tf==='M5'?300:tf==='M15'?900:tf==='M30'?1800:tf==='H1'?3600:tf==='H4'?14400:86400;
  b.derivWs.send(JSON.stringify({ticks_history:b.cfg.market,adjust_start_time:1,count:200,end:'latest',granularity:gran,start:1,style:'candles',subscribe:1}));
}

function startScanner(b) {
  if(b.scanInterval) clearInterval(b.scanInterval);
  findLevels(b);
  detectAutoHTFZones(b);
  b.scanInterval=setInterval(()=>{
    if(!b.botActive) return;
    // HTF detection runs every second — always fresh, just like main logic
    detectAutoHTFZones(b);
    if(b.inTrade||b.lossCountdownPaused||b.timeOffPaused) return;
    findLevels(b);
  },1000);
}

function stopScanner(b){ if(b.scanInterval){clearInterval(b.scanInterval);b.scanInterval=null;} }

function stopBot(b) {
  b.userStarted=false; b.botActive=false;
  stopScanner(b); stopLossCountdown(b); stopTimeOff(b);
  if(b.derivWs){try{b.derivWs.close();}catch(e){}}
  setStatus(b,'stopped','STOPPED');
  setTicker(b,`— BOT ${b.id} STOPPED —`);
  broadcastBotState(b);
}

// ── DASHBOARD WS ──────────────────────────────────
dashWss.on('connection',(ws)=>{
  console.log('📱 Dashboard connected');
  bots.forEach(b=>{
    ws.send(JSON.stringify({type:'bot_state',id:b.id,...getBotState(b)}));
    Object.keys(b.candles).forEach(tf=>{
      if(b.candles[tf]&&b.candles[tf].length)
        ws.send(JSON.stringify({type:'candles',id:b.id,tf,candles:b.candles[tf].slice(-100)}));
    });
  });

  ws.on('message',(raw)=>{
    let msg; try{msg=JSON.parse(raw);}catch(e){return;}
    const b=bots.find(x=>x.id===msg.id);
    if(!b&&msg.type!=='get_all_states'&&msg.type!=='get_live_login_url') return;

    // ── GET LIVE LOGIN URL (one shared login for all bots) ──
    if(msg.type==='get_live_login_url'){
      const url = buildOAuthUrl();
      ws.send(JSON.stringify({type:'live_login_url',url}));
      return;
    }

    if(msg.type==='start'){
      if(msg.cfg) b.cfg={...b.cfg,...msg.cfg};
      if(!liveAuth.loggedIn){
        ws.send(JSON.stringify({type:'error',id:b.id,msg:'Please login with Deriv first'}));
        return;
      }
      b.tradeCount=0;b.wins=0;b.losses=0;b.sessionPnl=0;
      b.consecutiveLosses=0;b.lossCountdownPaused=false;
      b.activeStructures=[];b.entryTargets=[];
      b.userStarted=true; saveData();
      if(b.derivWs&&b.derivWs.readyState===WebSocket.OPEN){
        b.botActive=true;
        setStatus(b,'running','RUNNING');
        startScanner(b);
        broadcastBotState(b);
      } else {
        // Get this bot its own fresh OTP socket on the shared account
        refreshLiveOTP(b);
      }
    }

    if(msg.type==='stop') stopBot(b);
    if(msg.type==='skip_cooldown'&&b.lossCountdownPaused) resumeAfterCooldown(b);
    if(msg.type==='time_off') startTimeOff(b,msg.secs);
    if(msg.type==='cancel_time_off') resumeAfterTimeOff(b);

    if(msg.type==='ignore_level'){
      const lv=levelKey(b.cfg.market,parseFloat(msg.level));
      if(b.ignoredLevels.has(lv)){b.ignoredLevels.delete(lv);log(b,`✅ Un-ignored ${lv}`);}
      else{b.ignoredLevels.add(lv);log(b,`🚫 Ignored ${lv}`);}
      b.activeStructures.forEach(s=>{s.projectedLevels=computeProjectedLevels(b,s,s.tradedLevels);});
      broadcastBotState(b);
    }

    if(msg.type==='add_dnt_zone'){
      b.doNotTradeZones.push({a:parseFloat(msg.a),b:parseFloat(msg.b)});
      log(b,`🚫 Do Not Trade Zone: ${msg.a}–${msg.b}`);
      b.activeStructures.forEach(s=>{s.projectedLevels=computeProjectedLevels(b,s,s.tradedLevels);});
      broadcastBotState(b);
    }
    if(msg.type==='remove_dnt_zone'){
      b.doNotTradeZones.splice(msg.idx,1);
      log(b,'✅ Do Not Trade Zone removed');
      b.activeStructures.forEach(s=>{s.projectedLevels=computeProjectedLevels(b,s,s.tradedLevels);});
      broadcastBotState(b);
    }

    // ── MANUAL HTF ZONE ───────────────────────────
    if(msg.type==='add_htf_zone'){
      const newZone = {
        a: parseFloat(msg.a), b: parseFloat(msg.b),
        source: 'manual',
        id: `manual_${Date.now()}`,
        label: `Manual ${parseFloat(msg.a).toFixed(getMarketPrecisionDigits(b.cfg.market))}–${parseFloat(msg.b).toFixed(getMarketPrecisionDigits(b.cfg.market))}`,
        cancelled: false,
      };
      b.htfZones.push(newZone);
      log(b,`⚠ Manual HTF Zone: ${msg.a}–${msg.b}`);
      broadcastBotState(b);
    }
    // ── CANCEL HTF ZONE (auto or manual) ──────────
    if(msg.type==='cancel_htf_zone'){
      const zone = b.htfZones.find(z=>z.id===msg.zoneId);
      if(zone){
        zone.cancelled=true;
        log(b,`✅ HTF Zone cancelled: ${zone.label}`);
        // If bot was paused due to this zone, check if any active zones remain
        if(b.htfZonePaused){
          const stillActive=b.htfZones.filter(z=>!z.cancelled);
          const inAny=stillActive.some(z=>b.currentPrice<=Math.max(z.a,z.b)&&b.currentPrice>=Math.min(z.a,z.b));
          if(!inAny){
            b.htfZonePaused=false;
            setStatus(b,'running','RUNNING');
            setTicker(b,'✅ HTF Zone cancelled — resuming...');
          }
        }
        broadcastBotState(b);
      }
    }
    // ── RESTORE CANCELLED HTF ZONE ─────────────────
    if(msg.type==='restore_htf_zone'){
      const zone = b.htfZones.find(z=>z.id===msg.zoneId);
      if(zone){ zone.cancelled=false; log(b,`↩ HTF Zone restored: ${zone.label}`); broadcastBotState(b); }
    }
    // ── REMOVE MANUAL HTF ZONE PERMANENTLY ────────
    if(msg.type==='remove_htf_zone'){
      const idx = b.htfZones.findIndex(z=>z.id===msg.zoneId&&z.source==='manual');
      if(idx>=0){ b.htfZones.splice(idx,1); log(b,'✅ Manual HTF Zone removed'); broadcastBotState(b); }
    }

    // ── TEST TRADE — NO CONDITIONS, FIRES IMMEDIATELY ──
    if(msg.type==='test_trade'){
      if(!b.botActive){
        ws.send(JSON.stringify({type:'error',id:b.id,msg:'Bot must be running to test trade'}));
        return;
      }
      if(b.inTrade){
        ws.send(JSON.stringify({type:'error',id:b.id,msg:'Already in a trade'}));
        return;
      }
      log(b,'🔥 TEST TRADE fired manually — bypassing all conditions');
      broadcast({type:'log',id:b.id,msg:`[Bot${b.id}] 🔥 TEST TRADE — ${b.cfg.command} on ${b.cfg.market} stake $${b.cfg.stake}`});
      placeTrade(b);
      return;
    }

    if(msg.type==='update_cfg'){b.cfg={...b.cfg,...msg.cfg};saveData();}

    if(msg.type==='get_history'){
      ws.send(JSON.stringify({type:'full_history',id:b.id,tradeLog:b.tradeLog}));
    }

    if(msg.type==='get_all_states'){
      bots.forEach(x=>ws.send(JSON.stringify({type:'bot_state',id:x.id,...getBotState(x)})));
    }
  });

  ws.on('close',()=>console.log('📱 Dashboard disconnected'));
});

function getBotState(b){
  return {
    botActive:b.botActive,currentPrice:b.currentPrice,
    trendStatus:b.trendStatus,confirmedTrend:b.confirmedTrend,
    activeStructures:b.activeStructures.map(s=>({
      peaks:s.peaks,baseDiff:s.baseDiff,type:s.type,tf:s.tf,
      projectedLevels:s.projectedLevels,tradedLevels:[...s.tradedLevels],id:s.id
    })),
    ignoredLevels:[...b.ignoredLevels],
    doNotTradeZones:b.doNotTradeZones,
    htfZones:b.htfZones,
    htfZonePaused:b.htfZonePaused,
    htfPauseReason:b.htfPauseReason||'',
    activeHtfZoneId:b.activeHtfZoneId||null,
    autoHtfStructures:b.autoHtfStructures||[],
    activeContractsList:Object.entries(b.activeContracts||{}).map(([cid,i])=>({contractId:cid,level:i.level,structType:i.structType,command:i.command,market:i.market,stake:i.stake})),
    tradeCount:b.tradeCount,wins:b.wins,losses:b.losses,sessionPnl:b.sessionPnl,
    consecutiveLosses:b.consecutiveLosses,
    lossCountdownPaused:b.lossCountdownPaused,lossCountdownRemaining:b.lossCountdownRemaining,lossCountdownTotal:b.lossCountdownTotal,
    timeOffPaused:b.timeOffPaused,timeOffRemaining:b.timeOffRemaining,timeOffTotal:b.timeOffTotal,
    tickerMsg:b.tickerMsg,statusText:b.statusText,cfg:b.cfg,
    liveLoggedIn:liveAuth.loggedIn,
    liveAccountId:liveAuth.accountId,
    tradeLog:b.tradeLog.slice(0,100),
  };
}

app.get('/ping',(req,res)=>res.send('OK'));
app.get('/api/state',(req,res)=>res.json(bots.map(b=>({id:b.id,...getBotState(b)}))));

setInterval(()=>{
  bots.forEach(b=>{
    if(!b.botActive) return;
    const wr=b.tradeCount>0?Math.round((b.wins/b.tradeCount)*100):0;
    console.log(`[Bot${b.id}] ${b.currentPrice} Trades:${b.tradeCount} WR:${wr}% P&L:${b.sessionPnl>=0?'+':''}$${b.sessionPnl.toFixed(2)} Structs:${b.activeStructures.length}`);
  });
},5*60*1000);

// ── DATA ENGINE: INSTANCES + ROUTES ───────────────
const dataDb = new DataEngineDB(DATAENGINE_DB_PATH);
dataDb.rebuildAllCandles(console);
const downloadCandleBuilder = new CandleBuilder(dataDb, DATAENGINE_CANDLE_INTERVAL_SECONDS, console);
const liveCandleBuilder     = new CandleBuilder(dataDb, DATAENGINE_CANDLE_INTERVAL_SECONDS, console);
const historyDownloader     = new HistoryDownloader(dataDb, downloadCandleBuilder, DATAENGINE_WS_URL, console);
const liveTickCollector     = new LiveTickCollector(dataDb, liveCandleBuilder, DATAENGINE_WS_URL, console);
// FIX: this used to auto-start live tick collection for every market the
// moment the server booted — nothing should run on its own. Live collection
// per market now only starts when you explicitly turn it on from the DATA
// tab (POST /api/data/live/:symbol/resume below), same as downloads only
// ever start when you press the Download button.
console.log('DataEngine ready', DATAENGINE_DB_PATH, DATAENGINE_MARKETS);

function toEpochSeconds(v) {
  if (v == null || v === '') return null;
  if (typeof v === 'number') return Math.floor(v);
  const n = Number(v);
  if (!isNaN(n) && String(v).trim() !== '') return Math.floor(n);
  const parsed = Date.parse(v);
  return isNaN(parsed) ? null : Math.floor(parsed / 1000);
}
const UNIT_SECONDS = { days: 86400, weeks: 7 * 86400, months: 30 * 86400 };

app.get('/api/data/markets', (req, res) => res.json(DATAENGINE_MARKETS));

app.get('/api/data/live/status', (req, res) => {
  res.json(DATAENGINE_MARKETS.map(symbol => ({ symbol, running: liveTickCollector.isRunning(symbol) })));
});

app.post('/api/data/live/:symbol/pause', (req, res) => {
  const { symbol } = req.params;
  if (!DATAENGINE_MARKETS.includes(symbol)) return res.status(400).json({ error: 'Unsupported symbol' });
  const stopped = liveTickCollector.stopSymbol(symbol);
  res.json({ symbol, running: false, changed: stopped });
});

app.post('/api/data/live/:symbol/resume', (req, res) => {
  const { symbol } = req.params;
  if (!DATAENGINE_MARKETS.includes(symbol)) return res.status(400).json({ error: 'Unsupported symbol' });
  liveTickCollector.startSymbol(symbol);
  res.json({ symbol, running: true });
});

app.get('/api/data/stats', (req, res) => {
  try { res.json(dataDb.getStats(DATAENGINE_MARKETS)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/data/integrity', (req, res) => {
  try { res.json(dataDb.verifyIntegrity(DATAENGINE_MARKETS)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/data/downloads', (req, res) => res.json(historyDownloader.listJobs()));

app.post('/api/data/download', (req, res) => {
  const { symbol, amount, unit } = req.body;
  if (!DATAENGINE_MARKETS.includes(symbol)) return res.status(400).json({ error: 'Unsupported symbol' });
  const unitSeconds = UNIT_SECONDS[unit];
  const amt = Number(amount);
  if (!unitSeconds || !amt || amt <= 0) return res.status(400).json({ error: 'Invalid amount/unit — unit must be days, weeks, or months' });
  const seconds = Math.round(amt * unitSeconds);
  try {
    const job = historyDownloader.startDownload(symbol, seconds, (progress) => broadcast({ type: 'download_progress', data: progress }));
    res.json(job);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/data/download/:jobId/pause', (req, res) => {
  const job = historyDownloader.pauseJob(req.params.jobId);
  job ? res.json(job) : res.status(404).json({ error: 'Job not found or not running' });
});

app.post('/api/data/download/:jobId/resume', (req, res) => {
  const job = historyDownloader.resumeJob(req.params.jobId, (progress) => broadcast({ type: 'download_progress', data: progress }));
  job ? res.json(job) : res.status(404).json({ error: 'Job not found or not paused' });
});

app.post('/api/data/download/:jobId/cancel', (req, res) => {
  const job = historyDownloader.cancelJob(req.params.jobId);
  job ? res.json(job) : res.status(404).json({ error: 'Job not found' });
});

app.get('/api/data/search', (req, res) => {
  const symbol = req.query.symbol;
  const start = toEpochSeconds(req.query.start);
  const end = toEpochSeconds(req.query.end);
  const limit = Math.min(5000, Number(req.query.limit) || 1000);
  if (!DATAENGINE_MARKETS.includes(symbol) || start == null || end == null) {
    return res.status(400).json({ error: 'symbol, start, and end are required (start/end may be epoch seconds or ISO date strings)' });
  }
  try {
    const ticks = dataDb.getTicksBetween(symbol, start, end).slice(0, limit);
    dataDb.rebuildCandlesForRange(symbol, start, end);
    const candles = dataDb.getCandlesBetween(symbol, start, end).slice(0, limit);
    res.json({ symbol, start, end, ticks, candles, ticksTruncated: ticks.length >= limit, candlesTruncated: candles.length >= limit });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/data/range', (req, res) => {
  const { symbol, start, end } = req.body;
  const s = toEpochSeconds(start), e2 = toEpochSeconds(end);
  if (!DATAENGINE_MARKETS.includes(symbol) || s == null || e2 == null || s > e2) {
    return res.status(400).json({ error: 'symbol, start, and end are required and start must be <= end' });
  }
  try {
    const deletedTicks = dataDb.deleteTicksBetween(symbol, s, e2);
    const deletedCandles = dataDb.deleteCandlesBetween(symbol, s, e2);
    res.json({ success: true, deletedTicks, deletedCandles });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════════════════════
// BACKTEST ENGINE — replays Elroi's OWN strategy (findStructuresInData,
// findLevels, checkHTFZoneUptrend, checkEntry, etc. — completely untouched)
// against stored forex history from the data engine above.
//
// This is NOT a port of bot 2's BacktestRunner — bot 2's replay loop is
// wired directly to bot 2's own "Golden Logic"/"Super Logic" strategy
// functions, which don't exist here. This is a new runner that drives
// Elroi's structure/zone-detection strategy instead, built the same way
// live trading already drives it (tick-by-tick, candle-close-by-candle-
// close), just fed from stored history instead of a live Deriv socket.
//
// Contract outcomes are resolved by walking forward through the exact
// historical tick data (never simulated randomly). Payouts for
// touch/directional/vanilla contracts use a fixed ~85% return estimate,
// since Deriv's real payout depends on live pricing (volatility, rates)
// that can't be reproduced offline — this is the same conservative
// approximation bot 2's engine used. Multiplier contracts (MULTUP/MULTDOWN)
// are resolved properly against real leveraged price movement with TP/SL,
// since Elroi trades those directly, unlike bot 2. Vanilla contracts'
// early-take-profit-close feature is NOT simulated (documented limitation
// below) — only resolution at expiry is modeled.
// ══════════════════════════════════════════════════════════════════════════

const DURATION_UNIT_SECONDS_BT = { s: 1, m: 60, h: 3600, d: 86400 };

function btResolveBarrier(entrySpot, barrierStr) {
  if (barrierStr == null || barrierStr === '') return null;
  const s = String(barrierStr).trim();
  if (s === '') return null;
  if (s[0] === '+' || s[0] === '-') return parseFloat((entrySpot + parseFloat(s)).toFixed(5));
  return parseFloat(s);
}

// Approximate payout for touch/directional/vanilla contracts — see file header.
function btEstimatedProfit(amount) {
  return parseFloat((amount * 1.85 - amount).toFixed(2));
}

// Walks forward through historical ticks from startIndex to resolve one
// contract. Returns null if history runs out before the contract can settle.
function resolveContractFromHistory(ticks, startIndex, params, maxHoldSeconds) {
  if (startIndex >= ticks.length) return null;
  const type = params.contract_type;
  const isTouch       = type === 'ONETOUCH' || type === 'NOTOUCH';
  const isDirectional = type === 'CALL' || type === 'PUT';
  const isVanilla      = type === 'VANILLALONGCALL' || type === 'VANILLALONGPUT';
  const isMultiplier   = type === 'MULTUP' || type === 'MULTDOWN';

  const entryTick  = ticks[startIndex];
  const entrySpot  = entryTick.quote;
  const barrierAbs = btResolveBarrier(entrySpot, params.barrier);
  let maxSpot = entrySpot, minSpot = entrySpot;

  let expireEpoch = null;
  if (params.duration_unit && params.duration_unit !== 't') {
    const mult = DURATION_UNIT_SECONDS_BT[params.duration_unit];
    if (!mult) throw new Error(`resolveContractFromHistory: unsupported duration_unit "${params.duration_unit}"`);
    expireEpoch = entryTick.epoch + params.duration * mult;
  } else if (isMultiplier) {
    // Multiplier contracts have no fixed duration live — capped here so a
    // simulated position can't hold forever if TP/SL never hits.
    expireEpoch = entryTick.epoch + (maxHoldSeconds || 3600);
  }

  const multiplierPnl = (price) => {
    const direction = type === 'MULTUP' ? 1 : -1;
    const pnlPct = direction * (price - entrySpot) / entrySpot * params.multiplier;
    return parseFloat((params.amount * pnlPct).toFixed(2));
  };
  // Favorable price movement from entry, direction-aware — this is the
  // actual "distance from entry" the bot checks against your TP/SL
  // distance settings. Positive = moving in your favor.
  const favorableDistance = (price) => (type === 'MULTUP' ? 1 : -1) * (price - entrySpot);

  for (let i = startIndex + 1; i < ticks.length; i++) {
    const tick = ticks[i]; const price = tick.quote;
    if (price > maxSpot) maxSpot = price;
    if (price < minSpot) minSpot = price;

    if (isTouch && barrierAbs != null) {
      const touched = barrierAbs >= entrySpot ? price >= barrierAbs : price <= barrierAbs;
      if (touched) return finish(type === 'ONETOUCH', price, i);
    }

    if (isMultiplier) {
      const dist = favorableDistance(price);
      if (params.tpDistance > 0 && dist >= params.tpDistance) return finishMulti(multiplierPnl(price), price, i);
      if (params.slDistance > 0 && dist <= -params.slDistance) return finishMulti(multiplierPnl(price), price, i);
    }

    const expired = expireEpoch != null && tick.epoch >= expireEpoch;
    if (!expired) continue;

    if (isTouch)       return finish(type === 'NOTOUCH', price, i);
    if (isDirectional) return finish(type === 'CALL' ? price > entrySpot : price < entrySpot, price, i);
    if (isVanilla)      return finish(type === 'VANILLALONGCALL' ? price > barrierAbs : price < barrierAbs, price, i);
    if (isMultiplier)   return finishMulti(multiplierPnl(price), price, i);
    return finish(false, price, i);
  }
  return null; // ran out of historical data before the contract could resolve

  function finish(won, exitSpot, exitIndex) {
    return {
      won, profit: won ? btEstimatedProfit(params.amount) : -params.amount,
      entrySpot, exitSpot, barrierAbs, maxSpot, minSpot, exitEpoch: ticks[exitIndex].epoch,
    };
  }
  function finishMulti(profit, exitSpot, exitIndex) {
    return { won: profit > 0, profit, entrySpot, exitSpot, barrierAbs: null, maxSpot, minSpot, exitEpoch: ticks[exitIndex].epoch };
  }
}

// Builds Deriv-shaped contract params from a bot's cfg + command, exactly
// mirroring placeTrade()'s own param-building — kept as a separate function
// rather than touching placeTrade() itself.
function btBuildParams(b) {
  const isMulti   = b.cfg.command === 'CALL_MULT' || b.cfg.command === 'PUT_MULT';
  const isVanilla = b.cfg.command === 'VANILLA_CALL' || b.cfg.command === 'VANILLA_PUT';
  const duration  = b.cfg.durationMins * 60;
  const type = {
    NOTOUCH:'NOTOUCH', TOUCH:'ONETOUCH', HIGHER:'CALL', LOWER:'PUT', RISE:'CALL', FALL:'PUT',
    CALL_MULT:'MULTUP', PUT_MULT:'MULTDOWN', VANILLA_CALL:'VANILLALONGCALL', VANILLA_PUT:'VANILLALONGPUT',
  }[b.cfg.command] || 'NOTOUCH';

  const params = { contract_type: type, amount: b.cfg.stake };
  if (isMulti) {
    params.multiplier = b.cfg.multiplier;
    // Price-distance TP/SL (e.g. 0.0050 = 50 pips on EUR/USD), not dollars —
    // this is what backtestPlaceTrade actually checks price against.
    params.tpDistance = b.cfg.multTPDistance || 0;
    params.slDistance = b.cfg.multSLDistance || 0;
  } else if (isVanilla) {
    params.duration = duration; params.duration_unit = 's';
    if (b.cfg.vanillaStrike != null && b.cfg.vanillaStrike !== '') params.barrier = String(b.cfg.vanillaStrike);
  } else {
    params.duration = duration; params.duration_unit = 's';
    if (['NOTOUCH','TOUCH','HIGHER','LOWER'].includes(b.cfg.command)) params.barrier = b.cfg.barrierOffset;
  }
  return { params, isMulti, isVanilla, duration };
}

// Multiplier contracts have no fixed duration live — they stay open until
// your dollar TP/SL naturally hits, which can take hours. Backtest needs
// *some* safety cap so a position can't hold forever if TP/SL never
// triggers, but capping it at durationMins (5 min default) meant almost no
// realistic forex move could ever reach a $-based TP/SL in time — nearly
// every multiplier trade was timing out at a near-zero, meaningless P/L.
// 24 hours gives real price movement room to actually reach the target.
const BACKTEST_MULTIPLIER_MAX_HOLD_SECONDS = 24 * 3600;

// Called instead of the live proposal/buy flow when b.backtest is true.
// Resolves the contract immediately by scanning forward through the
// runner's in-memory tick array — no waiting, since it's all historical.
function backtestPlaceTrade(b, meta) {
  const { params, isMulti, isVanilla, duration } = btBuildParams(b);
  if (isMulti || isVanilla) b.inTrade = true;

  const runner = b.backtestRunner;
  const contractInfo = {
    stake: b.cfg.stake, command: b.cfg.command, market: b.cfg.market,
    level: meta.level ?? null, structType: meta.structType ?? null, isMulti: isMulti || isVanilla,
  };

  const result = resolveContractFromHistory(runner.allTicks, runner.currentTickIndex, params, isMulti ? BACKTEST_MULTIPLIER_MAX_HOLD_SECONDS : duration);
  if (!result) {
    backtestFinalizeResult(runner, b, -b.cfg.stake, contractInfo, true);
    return;
  }
  backtestFinalizeResult(runner, b, result.profit, contractInfo, false);
}

// Backtest's own equivalent of finalizeResult() — same bookkeeping
// (tradeCount/wins/losses/tradeLog/consecutive-loss stop/cooldown), but
// using simulated epoch-based cooldown instead of real setTimeout, and
// without saveData()/telegram()/dashboard broadcast side effects.
function backtestFinalizeResult(runner, b, profit, contractInfo, insufficientData) {
  if (contractInfo.isMulti) b.inTrade = false;
  b.tradeCount++; b.sessionPnl = parseFloat((b.sessionPnl + profit).toFixed(2));
  const won = profit > 0;
  if (won) b.wins++; else b.losses++;
  const wr = b.tradeCount ? Math.round((b.wins / b.tradeCount) * 100) : 0;

  b.tradeLog.unshift({
    id: b.tradeCount, tradeNum: b.tradeCount,
    time: new Date(runner.currentEpoch * 1000).toISOString(),
    timestamp: runner.currentEpoch * 1000, won, profit,
    level: typeof contractInfo.level === 'number' ? contractInfo.level.toFixed(getMarketPrecisionDigits(contractInfo.market)) : null,
    struct: contractInfo.structType, command: contractInfo.command,
    market: contractInfo.market, stake: contractInfo.stake, wr,
    insufficientData: !!insufficientData,
  });
  if (b.tradeLog.length > 500) b.tradeLog.pop();

  if (won) {
    b.consecutiveLosses = 0;
  } else {
    b.consecutiveLosses++;
    if (b.consecutiveLosses >= b.cfg.maxConsecLosses) {
      b.botActive = false;
      runner.note = `Stopped — ${b.cfg.maxConsecLosses} consecutive losses`;
    } else if (b.cfg.cooldownEnabled !== false) {
      runner.cooldownUntilEpoch = runner.currentEpoch + (b.cfg.cooldownSecs || 0);
    }
  }
  if (b.cfg.maxTrades > 0 && b.tradeCount >= b.cfg.maxTrades) {
    b.botActive = false;
    runner.note = `Stopped — reached max trades (${b.cfg.maxTrades})`;
  }
}

// Generic OHLC aggregation from 1-minute candles into any larger bucket
// size, bucketed by wall-clock epoch (same approach the data engine's own
// candle builder uses — see DATA ENGINE header comment above).
function aggregateCandlesEpoch(m1Candles, bucketSeconds) {
  const buckets = [];
  let cur = null;
  for (const c of m1Candles) {
    const bucketStart = Math.floor(c.epoch / bucketSeconds) * bucketSeconds;
    if (!cur || cur.epoch !== bucketStart) {
      if (cur) buckets.push(cur);
      cur = { epoch: bucketStart, open: c.open, high: c.high, low: c.low, close: c.close };
    } else {
      cur.high = Math.max(cur.high, c.high);
      cur.low  = Math.min(cur.low, c.low);
      cur.close = c.close;
    }
  }
  if (cur) buckets.push(cur);
  return buckets;
}

function sleepMs(ms) { return new Promise(r => setTimeout(r, ms)); }

const backtestRunners = new Map();
let backtestCounter = 0;

class ElroiBacktestRunner {
  constructor(cfg, symbol, fromEpoch, toEpoch, speed) {
    this.id = `bt${++backtestCounter}`;
    this.symbol = symbol;
    this.fromEpoch = fromEpoch;
    this.toEpoch = toEpoch;
    this.speed = speed; // 1..500, or 'max'
    this.status = 'starting'; // starting | running | paused | completed | cancelled | error
    this._paused = false;
    this._cancelled = false;
    this.error = null;
    this.note = null;
    this.startedAt = Date.now();

    this.bot = createBot(this.id);
    this.bot.id = this.id;
    this.bot.backtest = true;
    this.bot.backtestRunner = this;
    this.bot.cfg = { ...this.bot.cfg, ...cfg, market: symbol };
    this.bot.botActive = true;
    this.bot.candles = { M1:[], M5:[], M15:[], M30:[], H1:[], H4:[], D1:[] };

    this.allTicks = [];
    this.currentTickIndex = 0;
    this.processedTicks = 0;
    this.totalTicks = 0;
    this.currentEpoch = fromEpoch;
    this.cooldownUntilEpoch = 0;
    this.lastScanEpoch = 0; // drives the simulated 1-second scanner cadence, matching live's real-time scanInterval
  }

  publicState() {
    const b = this.bot;
    return {
      id: this.id, symbol: this.symbol, status: this.status,
      fromEpoch: this.fromEpoch, toEpoch: this.toEpoch, speed: this.speed,
      processedTicks: this.processedTicks, totalTicks: this.totalTicks,
      percent: this.totalTicks ? Math.min(100, Math.round((this.processedTicks / this.totalTicks) * 100)) : 0,
      currentEpoch: this.currentEpoch, error: this.error, note: this.note,
      tradeCount: b.tradeCount, wins: b.wins, losses: b.losses,
      winRate: b.tradeCount ? Math.round((b.wins / b.tradeCount) * 100) : 0,
      sessionPnl: b.sessionPnl, tradeLog: b.tradeLog.slice(0, 100),
      cfg: b.cfg,
    };
  }

  pause()  { if (this.status === 'running') { this._paused = true; this.status = 'paused'; } return this.publicState(); }
  resume() { if (this.status === 'paused')  { this._paused = false; this.status = 'running'; } return this.publicState(); }
  cancel() { this._cancelled = true; this.status = 'cancelled'; return this.publicState(); }

  async run() {
    try {
      this.status = 'running';
      this.allTicks = dataDb.getTicksBetween(this.symbol, this.fromEpoch, this.toEpoch);
      this.totalTicks = this.allTicks.length;
      if (!this.totalTicks) {
        this.status = 'error';
        this.error = 'No stored tick data in that range — download history for this pair first';
        return;
      }

      dataDb.rebuildCandlesForRange(this.symbol, this.fromEpoch, this.toEpoch);
      const m1Raw = dataDb.getCandlesBetween(this.symbol, this.fromEpoch, this.toEpoch);
      if (m1Raw.length < 20) {
        this.status = 'error';
        this.error = 'Not enough 1-minute candles in range to run the strategy (need at least 20)';
        return;
      }

      const queues = {
        M1: m1Raw, M5: aggregateCandlesEpoch(m1Raw, 300), M15: aggregateCandlesEpoch(m1Raw, 900),
        M30: aggregateCandlesEpoch(m1Raw, 1800), H1: aggregateCandlesEpoch(m1Raw, 3600), H4: aggregateCandlesEpoch(m1Raw, 14400),
      };
      const idx = { M1:0, M5:0, M15:0, M30:0, H1:0, H4:0 };
      const intervalSecs = { M1:60, M5:300, M15:900, M30:1800, H1:3600, H4:14400 };
      const trendTFs = ['M1','M5','M15'];
      const htfTFs   = ['M30','H1','H4'];

      const b = this.bot;

      for (let i = 0; i < this.allTicks.length; i++) {
        if (this._cancelled) { this.status = 'cancelled'; return; }
        while (this._paused && !this._cancelled) await sleepMs(200);
        if (this._cancelled) { this.status = 'cancelled'; return; }

        const tick = this.allTicks[i];
        this.currentTickIndex = i;
        this.currentEpoch = tick.epoch;
        this.processedTicks = i + 1;

        for (const tf of ['M1','M5','M15','M30','H1','H4']) {
          const q = queues[tf], secs = intervalSecs[tf];
          while (idx[tf] < q.length && q[idx[tf]].epoch + secs <= tick.epoch) {
            const c = q[idx[tf]];
            const candle = { time: c.epoch, open: c.open, high: c.high, low: c.low, close: c.close };
            if (!b.candles[tf]) b.candles[tf] = [];
            b.candles[tf].push(candle);
            if (b.candles[tf].length > 300) b.candles[tf].shift();
            if (trendTFs.includes(tf)) analyzeTrend(b, tf);
            if (htfTFs.includes(tf))   detectAutoHTFZones(b);
            idx[tf]++;
          }
        }

        b.lossCountdownPaused = this.currentEpoch < this.cooldownUntilEpoch;

        // Live re-evaluates structures/HTF zones every real second via a
        // free-running timer (startScanner's setInterval), completely
        // independent of candle closes. This mirrors that using simulated
        // time instead of wall-clock time — previously this only ran on M1
        // candle close (~once/minute), a much coarser cadence than live
        // actually uses, which meant the backtest wasn't re-evaluating
        // entries/zones anywhere near as often as the real strategy does.
        if (this.currentEpoch >= this.lastScanEpoch + 1) {
          this.lastScanEpoch = this.currentEpoch;
          if (b.botActive) {
            detectAutoHTFZones(b);
            if (!b.inTrade && !b.lossCountdownPaused && !b.timeOffPaused) findLevels(b);
          }
        }

        b.currentPrice = tick.quote;
        if (b.botActive) {
          checkHTFZoneUptrend(b);
          checkEntry(b);
        } else {
          // Stopped (max losses / max trades) — nothing left to simulate
          break;
        }

        if (this.speed !== 'max') {
          if (i % Math.max(1, this.speed) === 0) await sleepMs(1);
        } else if (i % 2000 === 0) {
          await sleepMs(0); // yield periodically even at max speed so the event loop stays responsive
        }
      }

      if (this.status === 'running') this.status = 'completed';
    } catch (e) {
      this.status = 'error';
      this.error = e.message;
      console.error('[backtest] error:', e);
    }
  }
}

// ── BACKTEST ROUTES ───────────────────────────────
app.post('/api/backtest/start', (req, res) => {
  const { symbol, from, to, speed, cfg } = req.body;
  if (!DATAENGINE_MARKETS.includes(symbol)) return res.status(400).json({ error: 'Unsupported symbol' });
  const fromEpoch = toEpochSeconds(from), toEpoch = toEpochSeconds(to);
  if (fromEpoch == null || toEpoch == null || fromEpoch >= toEpoch) {
    return res.status(400).json({ error: 'from/to are required and from must be before to (epoch seconds or ISO date strings)' });
  }
  if (!cfg || typeof cfg !== 'object') return res.status(400).json({ error: 'cfg (bot strategy config) is required' });

  const spd = speed === 'max' ? 'max' : Math.max(1, Math.min(500, Number(speed) || 100));
  const runner = new ElroiBacktestRunner(cfg, symbol, fromEpoch, toEpoch, spd);
  backtestRunners.set(runner.id, runner);
  runner.run(); // fire and forget — poll /api/backtest/:id for progress
  res.json(runner.publicState());
});

app.get('/api/backtest/list', (req, res) => {
  res.json(Array.from(backtestRunners.values()).map(r => r.publicState()));
});

app.get('/api/backtest/:id', (req, res) => {
  const r = backtestRunners.get(req.params.id);
  r ? res.json(r.publicState()) : res.status(404).json({ error: 'Backtest run not found' });
});

app.post('/api/backtest/:id/pause', (req, res) => {
  const r = backtestRunners.get(req.params.id);
  r ? res.json(r.pause()) : res.status(404).json({ error: 'Backtest run not found' });
});

app.post('/api/backtest/:id/resume', (req, res) => {
  const r = backtestRunners.get(req.params.id);
  r ? res.json(r.resume()) : res.status(404).json({ error: 'Backtest run not found' });
});

app.post('/api/backtest/:id/cancel', (req, res) => {
  const r = backtestRunners.get(req.params.id);
  r ? res.json(r.cancel()) : res.status(404).json({ error: 'Backtest run not found' });
});

app.delete('/api/backtest/:id', (req, res) => {
  const existed = backtestRunners.delete(req.params.id);
  res.json({ success: existed });
});


server.listen(PORT,()=>console.log(`⚡ EL ROI 4-in-1 running on port ${PORT}`));
process.on('SIGINT',()=>{bots.forEach(b=>stopBot(b));saveData();setTimeout(()=>process.exit(0),1000);});
process.on('SIGTERM',()=>{bots.forEach(b=>stopBot(b));saveData();setTimeout(()=>process.exit(0),1000);});
