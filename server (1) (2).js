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
const APP_ID_DEMO   = 1089;
const APP_ID_LIVE   = '33kbRhT3vWWKhrOsdu0vN';
const REDIRECT_URI  = process.env.REDIRECT_URI || 'https://elroi4in1-f9fu.onrender.com/callback';

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

// ── MKT NAMES ─────────────────────────────────────
const MKT_NAMES = {
  '1HZ100V':'Volatility 100 (1s)','R_100':'Volatility 100',
  '1HZ75V':'Volatility 75 (1s)','R_75':'Volatility 75',
  '1HZ50V':'Volatility 50 (1s)','R_50':'Volatility 50',
  '1HZ25V':'Volatility 25 (1s)','1HZ10V':'Volatility 10 (1s)',
  'frxEURUSD':'EUR/USD','frxGBPUSD':'GBP/USD','frxXAUUSD':'Gold/USD',
  'cryBTCUSD':'BTC/USD','cryETHUSD':'ETH/USD','stpRNG':'Step Index',
  'BOOM1000':'Boom 1000','BOOM500':'Boom 500','CRASH1000':'Crash 1000','CRASH500':'Crash 500',
};

// ── BOT FACTORY ──────────────────────────────────
function createBot(id) {
  const saved = savedData.bots?.[id] || {};
  return {
    id,
    cfg: {
      accountType:'demo',   // 'demo' | 'live'
      apiToken:'',          // demo legacy token
      market:'1HZ100V', command:'NOTOUCH',
      stake:1.00, durationMins:5, barrierOffset:'+2.1',
      multiplier:10, takeProfit:4.00, stopLoss:2.00,
      scanTFs:['M1','M5'], minTFConfirm:2, smallTol:10, bigTol:15,
      smallConfirm:1, bigConfirm:2, proximityPct:90,
      maxTrades:0, maxConsecLosses:2, cooldownSecs:1800, cooldownEnabled:true,
      liveAppId:'', redirectUri:'',
      vanillaStrike:null, vanillaTakeProfit:5.00, cooldownEnabled:true,
      liveAppId:'',
      teleToken:'', teleChatId:'',
      redirectUri:'',
      htfClosePct:20, htfPassPct:30,
      ...(saved.cfg||{}),
    },
    // live OAuth state
    liveAccessToken: null,
    liveAccountId:   null,
    liveLoggedIn:    false,

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
    liveLoggedIn:b.liveLoggedIn,
    liveAccountId:b.liveAccountId,
    tradeLog:b.tradeLog.slice(0,100),
  });
}

function log(b,msg) {
  const t=new Date().toISOString().replace('T',' ').slice(0,19);
  const full=`[${t}][Bot${b.id}] ${msg}`;
  console.log(full);
  broadcast({type:'log',id:b.id,msg:full});
}

function setTicker(b,msg){ b.tickerMsg=msg; broadcast({type:'ticker',id:b.id,msg}); }
function setStatus(b,s,t){ b.statusText=t; broadcast({type:'status',id:b.id,status:s,text:t}); }

// ── TELEGRAM ──────────────────────────────────────
async function telegram(b,msg) {
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
  broadcast({type:'trend',id:b.id,trendStatus:b.trendStatus,confirmedTrend:b.confirmedTrend});
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
          if(Math.max(data[k].open,data[k].close)>lp+0.05){broken=true;break;}
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
    const zA_12     = parseFloat((sl1.low + closePct * range12).toFixed(2));
    const zB_12     = parseFloat((sl1.low - passPct  * range12).toFixed(2));
    const id12      = `auto_12_${tf}_${sl1.low.toFixed(4)}`;
    const existCancel12 = b.htfZones.find(z=>z.id===id12&&z.cancelled);
    const zone12Broken  = recent.some(c => c.close < zB_12);
    if(!existCancel12 && !zone12Broken){
      autoZones.push({ a:zA_12, b:zB_12, source:'auto', id:id12,
        label:`${tf} SL1+2 (${sl1.low.toFixed(2)})`, cancelled:false });
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
        zoneA      = parseFloat((nextLevel + closePct * r).toFixed(2));
        zoneB      = parseFloat((nextLevel - passPct  * r).toFixed(2));
        structType = 'equal';
      } else if(diff > 0){
        // Ascending — next bounce expected higher
        const baseDiff = diff;
        nextLevel  = parseFloat((slA.low + baseDiff).toFixed(2));
        zoneA      = parseFloat((nextLevel + closePct * baseDiff).toFixed(2));
        zoneB      = parseFloat((nextLevel - passPct  * baseDiff).toFixed(2));
        structType = 'ascending';
      } else {
        // Descending — next bounce expected lower
        const baseDiff = absDiff;
        nextLevel  = parseFloat((slA.low - baseDiff).toFixed(2));
        zoneA      = parseFloat((nextLevel + closePct * baseDiff).toFixed(2));
        zoneB      = parseFloat((nextLevel - passPct  * baseDiff).toFixed(2));
        structType = 'descending';
      }

      const idFull      = `auto_${structType}_${tf}_${slA.low.toFixed(4)}_${slB.low.toFixed(4)}`;
      const existCancel = b.htfZones.find(z=>z.id===idFull&&z.cancelled);
      const zoneBroken  = recent.some(c => c.close < zoneB);
      if(!existCancel && !zoneBroken){
        autoZones.push({ a:zoneA, b:zoneB, source:'auto', id:idFull,
          label:`${tf} ${structType} NEXT:${nextLevel.toFixed(2)}`, cancelled:false });
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
      const existing=b.activeStructures.find(s=>s.type==='small'&&s.tf===tf&&Math.abs(s.peaks[0].price-result.smallStruct.peaks[0].price)<0.05);
      const tradedLevels=existing?existing.tradedLevels:new Set();
      newStructures.push({
        ...result.smallStruct, type:'small',tf,tradedLevels,
        projectedLevels:computeProjectedLevels(b,result.smallStruct,tradedLevels),
        id:`small_${tf}_${result.smallStruct.peaks[0].price.toFixed(2)}`
      });
    }
    if(result.bigStruct){
      const existing=b.activeStructures.find(s=>s.type==='big'&&s.tf===tf&&Math.abs(s.peaks[0].price-result.bigStruct.peaks[0].price)<0.05);
      const tradedLevels=existing?existing.tradedLevels:new Set();
      newStructures.push({
        ...result.bigStruct, type:'big',tf,tradedLevels,
        projectedLevels:computeProjectedLevels(b,result.bigStruct,tradedLevels),
        id:`big_${tf}_${result.bigStruct.peaks[0].price.toFixed(2)}`
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
        if(!s._lastTeleLevel||Math.abs(s._lastTeleLevel-np)>0.01){
          s._lastTeleLevel=np;
          const r1=s.peaks.length>=2?s.peaks[s.peaks.length-2].price:null;
          const r2=s.peaks[s.peaks.length-1].price;
          const diff=r1?Math.abs(r1-r2).toFixed(2):s.baseDiff.toFixed(2);
          const mkt=MKT_NAMES[b.cfg.market]||b.cfg.market;
          telegram(b,`🎯 <b>NEXT LEVEL ACTIVE</b>\nLevel: <b>${np.toFixed(2)}</b>\n${r1?`R1: ${r1.toFixed(2)} | R2: ${r2.toFixed(2)}\n`:''}Diff: ${diff}\nMarket: ${mkt}\nCommand: ${b.cfg.command}\nStruct: ${s.type.toUpperCase()} (${s.tf})`);
        }
      }
    });
  }

  broadcastBotState(b);
}

function computeProjectedLevels(b,struct,tradedLevels) {
  if(!struct||!struct.peaks||struct.peaks.length<1) return [];
  const lastLevel=struct.peaks[struct.peaks.length-1].price;
  let np=parseFloat((lastLevel-struct.baseDiff).toFixed(2));
  let safety=0;
  while(safety<20){
    if(
      !tradedLevels.has(np.toFixed(2)) &&
      !isLevelInDoNotTradeZone(b,np) &&
      !b.ignoredLevels.has(np.toFixed(2))
    ){
      return [np];
    }
    np=parseFloat((np-struct.baseDiff).toFixed(2));
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
    setTicker(b,`⚠ HTF zone ${lo.toFixed(2)}–${hi.toFixed(2)} — ${pauseReason}`);
    telegram(b,`⚠ <b>Bot paused — HTF Zone</b>\n${pauseReason}\nZone: ${lo.toFixed(2)}–${hi.toFixed(2)}\nResumes: price below zone B OR 2+ TFs uptrend`);
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
    if(struct.tradedLevels.has(target.toFixed(2))) continue;
    if(isLevelInDoNotTradeZone(b,target)) continue;
    if(b.ignoredLevels.has(target.toFixed(2))) continue;

    const pct=b.cfg.proximityPct/100;
    const bd=struct.baseDiff||5;
    const maxGap=bd*(1-pct);
    const confirmCount=struct.type==='small'?b.cfg.smallConfirm:b.cfg.bigConfirm;

    let et=b.entryTargets.find(e=>e.structId===struct.id&&Math.abs(e.level-target)<0.01);
    if(!et){ et={structId:struct.id,level:target,pricePassed:false,passedCount:0}; b.entryTargets.push(et); }

    if(!et.pricePassed){
      let count=0;
      for(let i=data.length-1;i>=Math.max(0,data.length-40);i--){
        if(Math.max(data[i].open,data[i].close)<target) count++;
        else break;
      }
      if(count>=confirmCount){ et.pricePassed=true; et.passedCount=count;
        setTicker(b,`✅ ${count} candles below ${target.toFixed(2)} [${struct.type}/${struct.tf}] — waiting pullback...`);
      } else { continue; }
    }

    if(b.currentPrice>=target){ et.pricePassed=false; et.passedCount=0; continue; }
    if(b.currentPrice<target-maxGap) continue;

    const last=data[data.length-1],prev=data[data.length-2];
    if(last.close<=prev.close) continue;

    setTicker(b,`⚡ ENTRY! ${b.currentPrice.toFixed(2)} at ${target.toFixed(2)} [${struct.type}/${struct.tf}]`);
    struct.tradedLevels.add(target.toFixed(2));
    struct.projectedLevels=computeProjectedLevels(b,struct,struct.tradedLevels);
    b.entryTargets=b.entryTargets.filter(e=>!(e.structId===struct.id&&Math.abs(e.level-target)<0.01));
    placeTrade(b,{level:target,structType:struct.type});
    return;
  }
}

// ── PLACE TRADE ───────────────────────────────────
// New Deriv API: proposal first → buy with proposal_id
function placeTrade(b,meta={}) {
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

  // Build proposal parameters
  // Demo (legacy WS) uses 'symbol', Live (OTP WS) uses 'underlying_symbol'
  const isLive = b.cfg.accountType === 'live';
  const params = {
    contract_type: type,
    basis:         'stake',
    amount:        b.cfg.stake,
    currency:      'USD',
  };
  if(isLive){
    params.underlying_symbol = b.cfg.market;
  } else {
    params.symbol = b.cfg.market;
  }
  if(isMulti){
    params.multiplier = b.cfg.multiplier;
  } else if(isVanilla){
    params.duration      = duration;
    params.duration_unit = 's';
    if(b.cfg.vanillaStrike!==null&&b.cfg.vanillaStrike!==undefined)
      params.barrier = b.cfg.vanillaStrike;
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
    level:typeof level==='number'?level.toFixed(2):null,
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
function buildOAuthUrl(botId) {
  const verifier  = generateCodeVerifier();
  const challenge = generateCodeChallenge(verifier);
  const state     = base64url(crypto.randomBytes(16));

  // Store pending state so /callback knows which bot this is
  oauthPending.set(state, { botId, verifier });

  // Clean up stale entries after 10 minutes
  setTimeout(()=>oauthPending.delete(state), 10*60*1000);

  const params = new URLSearchParams({
    response_type:          'code',
    client_id:              (b.cfg.liveAppId||APP_ID_LIVE),
    redirect_uri:           b.cfg.redirectUri||REDIRECT_URI,
    scope:                  'trade',
    state,
    code_challenge:         challenge,
    code_challenge_method:  'S256',
  });

  return `https://auth.deriv.com/oauth2/auth?${params.toString()}`;
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
  const { botId, verifier } = pending;
  const b = bots.find(x=>x.id===botId);
  if(!b) {
    return res.send('<script>window.close();</script><p>Bot not found. Close this window.</p>');
  }

  // Show a loading page — the real work happens server-side
  res.send(`<!DOCTYPE html><html><head><title>EL ROI — Connecting Bot ${botId}</title>
  <style>body{background:#03060f;color:#00d4ff;font-family:monospace;display:flex;align-items:center;justify-content:center;height:100vh;flex-direction:column;gap:16px;}
  .spin{width:40px;height:40px;border:3px solid #152840;border-top-color:#00d4ff;border-radius:50%;animation:spin 0.8s linear infinite;}
  @keyframes spin{to{transform:rotate(360deg);}}</style></head>
  <body><div class="spin"></div><p>Connecting Bot ${botId} to Deriv...</p>
  <script>setTimeout(()=>window.close(),8000);</script></body></html>`);

  // Step 3: Exchange code for access token
  try {
    log(b, '🔐 Exchanging OAuth code for access token...');
    broadcast({ type: 'live_login_status', id: botId, status: 'exchanging', msg: 'Exchanging auth code...' });

    const tokenRes = await fetch('https://auth.deriv.com/oauth2/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type:    'authorization_code',
        client_id:     (b.cfg.liveAppId||APP_ID_LIVE),
        code,
        code_verifier: verifier,
        redirect_uri:  b.cfg.redirectUri||REDIRECT_URI,
      }).toString(),
    });

    const tokenData = await tokenRes.json();
    if(!tokenRes.ok || !tokenData.access_token) {
      throw new Error(tokenData.error_description || tokenData.error || 'Token exchange failed');
    }

    const accessToken = tokenData.access_token;
    log(b, '✅ Access token obtained');
    broadcast({ type: 'live_login_status', id: botId, status: 'got_token', msg: 'Access token obtained...' });

    // Step 4: Get Options account list
    log(b, '🔍 Fetching options account...');
    broadcast({ type: 'live_login_status', id: botId, status: 'fetching_account', msg: 'Fetching account...' });

    let accountsRes = await fetch('https://api.derivws.com/trading/v1/options/accounts', {
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Deriv-App-ID':  (b.cfg.liveAppId||APP_ID_LIVE),
      },
    });

    let accountsData = await accountsRes.json();
    let accountId = null;

    if(accountsData.data && accountsData.data.length > 0) {
      accountId = accountsData.data[0].account_id;
      log(b, `✅ Account found: ${accountId}`);
    } else {
      // No account — create a real account
      log(b, '⚠ No account found, creating one...');
      broadcast({ type: 'live_login_status', id: botId, status: 'creating_account', msg: 'Creating account...' });

      const createRes = await fetch('https://api.derivws.com/trading/v1/options/accounts', {
        method: 'POST',
        headers: {
          'Authorization':  `Bearer ${accessToken}`,
          'Deriv-App-ID':  (b.cfg.liveAppId||APP_ID_LIVE),
          'Content-Type':   'application/json',
        },
        body: JSON.stringify({ currency: 'USD', group: 'row', account_type: 'real' }),
      });

      const createData = await createRes.json();
      if(!createData.data || !createData.data[0]) {
        throw new Error('Could not create or find a live Options account');
      }
      accountId = createData.data[0].account_id;
      log(b, `✅ Account created: ${accountId}`);
    }

    // Step 5: Get OTP → authenticated WebSocket URL
    log(b, '🔑 Getting OTP WebSocket URL...');
    broadcast({ type: 'live_login_status', id: botId, status: 'getting_otp', msg: 'Getting WebSocket token...' });

    const otpRes = await fetch(`https://api.derivws.com/trading/v1/options/accounts/${accountId}/otp`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Deriv-App-ID':  (b.cfg.liveAppId||APP_ID_LIVE),
      },
    });

    const otpData = await otpRes.json();
    if(!otpData.data || !otpData.data.url) {
      throw new Error('OTP endpoint did not return a WebSocket URL');
    }

    const wssUrl = otpData.data.url;
    log(b, `✅ OTP WebSocket URL obtained`);

    // Store on bot
    b.liveAccessToken = accessToken;
    b.liveAccountId   = accountId;
    b.liveLoggedIn    = true;

    broadcast({ type: 'live_login_status', id: botId, status: 'ready', msg: `✅ Bot ${botId} logged in — account ${accountId}` });
    broadcastBotState(b);

    // Step 6: Open the authenticated WebSocket and start trading
    connectDerivLive(b, wssUrl);

  } catch(err) {
    log(b, `❌ Live login error: ${err.message}`);
    b.liveLoggedIn = false;
    broadcast({ type: 'live_login_status', id: botId, status: 'error', msg: `❌ ${err.message}` });
    broadcastBotState(b);
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
    // For live, reconnect by getting a fresh OTP (access token may still be valid)
    if(b.userStarted && b.liveLoggedIn && b.liveAccessToken && b.liveAccountId) {
      if(b.reconnectTimer) clearTimeout(b.reconnectTimer);
      b.reconnectTimer = setTimeout(()=>refreshLiveOTP(b), 5000);
    }
  });

  b.derivWs.on('error', (e)=>log(b,'WS error: '+e.message));
}

// Reconnect live bot by getting a fresh OTP with the stored access token
async function refreshLiveOTP(b) {
  log(b, '🔄 Refreshing live OTP...');
  try {
    const otpRes = await fetch(`https://api.derivws.com/trading/v1/options/accounts/${b.liveAccountId}/otp`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${b.liveAccessToken}`,
        'Deriv-App-ID':  (b.cfg.liveAppId||APP_ID_LIVE),
      },
    });
    const otpData = await otpRes.json();
    if(!otpData.data || !otpData.data.url) {
      throw new Error('OTP refresh failed — no URL returned');
    }
    connectDerivLive(b, otpData.data.url);
  } catch(err) {
    log(b, `❌ OTP refresh failed: ${err.message} — clearing live session`);
    b.liveLoggedIn    = false;
    b.liveAccessToken = null;
    b.liveAccountId   = null;
    b.userStarted     = false;
    setStatus(b,'stopped','SESSION EXPIRED');
    broadcast({ type: 'live_login_status', id: b.id, status: 'expired', msg: '⚠ Session expired — please login again' });
    broadcastBotState(b);
  }
}

// ── DERIV DEMO CONNECTION (legacy token) ──────────
function connectDerivDemo(b) {
  if(b.derivWs){try{b.derivWs.terminate();}catch(e){}}
  log(b,'🔌 Connecting to Deriv [DEMO]...');
  setStatus(b,'connecting','CONNECTING');

  b.derivWs = new WebSocket(`wss://ws.binaryws.com/websockets/v3?app_id=${APP_ID_DEMO}`);

  b.derivWs.on('open',()=>{
    log(b,'🔗 Authorizing demo...');
    b.derivWs.send(JSON.stringify({authorize: b.cfg.apiToken}));
  });

  b.derivWs.on('message',(raw)=>{
    let d; try{d=JSON.parse(raw);}catch(e){return;}

    if(d.msg_type==='authorize'){
      if(d.error){
        log(b,'❌ Auth failed: '+d.error.message);
        setStatus(b,'stopped','AUTH FAILED');
        b.userStarted=false; b.derivWs.close(); broadcastBotState(b); return;
      }
      log(b,`✅ Auth: ${d.authorize.loginid} | $${d.authorize.balance}`);
      b.botActive=true;
      setStatus(b,'running','RUNNING');
      b.derivWs.send(JSON.stringify({ticks:b.cfg.market,subscribe:1}));
      ['M1','M5','M15','M30','H1','H4'].forEach(tf=>fetchCandles(b,tf));
      startScanner(b);
      broadcastBotState(b);
    }

    handleDerivMessage(b, d);
  });

  b.derivWs.on('close',()=>{
    log(b,'Disconnected');
    b.botActive=false; stopScanner(b);
    setStatus(b,'stopped','DISCONNECTED');
    broadcastBotState(b);
    if(b.userStarted){
      if(b.reconnectTimer) clearTimeout(b.reconnectTimer);
      b.reconnectTimer=setTimeout(()=>connectDerivDemo(b),5000);
    }
  });

  b.derivWs.on('error',(e)=>log(b,'WS error: '+e.message));
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
    if(!b&&msg.type!=='get_all_states') return;

    // ── GET LIVE LOGIN URL ─────────────────────────
    if(msg.type==='get_live_login_url'){
      const url = buildOAuthUrl(b.id);
      ws.send(JSON.stringify({type:'live_login_url',id:b.id,url}));
      return;
    }

    if(msg.type==='start'){
      if(msg.cfg) b.cfg={...b.cfg,...msg.cfg};
      const isLive = b.cfg.accountType === 'live';
      if(isLive){
        if(!b.liveLoggedIn){
          ws.send(JSON.stringify({type:'error',id:b.id,msg:'Please login with Deriv first'}));
          return;
        }
        // Live bot already has an open WS from the OAuth flow — just start scanning
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
          // WebSocket dropped — refresh OTP and reconnect
          refreshLiveOTP(b);
        }
      } else {
        if(!b.cfg.apiToken){ ws.send(JSON.stringify({type:'error',id:b.id,msg:'No API token for demo account'})); return; }
        b.tradeCount=0;b.wins=0;b.losses=0;b.sessionPnl=0;
        b.consecutiveLosses=0;b.lossCountdownPaused=false;
        b.activeStructures=[];b.entryTargets=[];
        b.userStarted=true; saveData(); connectDerivDemo(b);
      }
    }

    if(msg.type==='stop') stopBot(b);
    if(msg.type==='skip_cooldown'&&b.lossCountdownPaused) resumeAfterCooldown(b);
    if(msg.type==='time_off') startTimeOff(b,msg.secs);
    if(msg.type==='cancel_time_off') resumeAfterTimeOff(b);

    if(msg.type==='ignore_level'){
      const lv=parseFloat(msg.level).toFixed(2);
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
        label: `Manual ${parseFloat(msg.a).toFixed(2)}–${parseFloat(msg.b).toFixed(2)}`,
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
    liveLoggedIn:b.liveLoggedIn,
    liveAccountId:b.liveAccountId,
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

server.listen(PORT,()=>console.log(`⚡ EL ROI 4-in-1 running on port ${PORT}`));
process.on('SIGINT',()=>{bots.forEach(b=>stopBot(b));saveData();setTimeout(()=>process.exit(0),1000);});
process.on('SIGTERM',()=>{bots.forEach(b=>stopBot(b));saveData();setTimeout(()=>process.exit(0),1000);});
