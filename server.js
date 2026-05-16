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

const app     = express();
const server  = http.createServer(app);
const dashWss = new WebSocket.Server({ server, path: '/dashboard' });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const PORT   = process.env.PORT || 3000;
const APP_ID = 1089;

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
      apiToken:'', market:'1HZ100V', command:'NOTOUCH',
      stake:1.00, durationMins:5, barrierOffset:'+2.1',
      multiplier:10, takeProfit:4.00, stopLoss:2.00,
      scanTF:'M1+M5', minTFConfirm:2, smallTol:10, bigTol:15,
      smallConfirm:1, bigConfirm:2, proximityPct:90,
      maxTrades:0, maxConsecLosses:2, cooldownSecs:1800,
      teleToken:'', teleChatId:'',
      ...(saved.cfg||{}),
    },
    derivWs:null, botActive:false, userStarted:false,
    reconnectTimer:null, scanInterval:null,
    currentPrice:0,
    candles:{ M1:[],M5:[],M15:[],H1:[],H4:[],D1:[] },
    trendStatus:{ M1:null,M5:null,M15:null },
    confirmedTrend:false,
    activeStructures:[],
    ignoredLevels:new Set(),
    ignoreZones:[],
    htfZonePaused:false,
    inTrade:false, currentContractId:null,
    entryTargets:[],
    currentActiveLevel:null, currentStructType:null,
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
    ignoreZones:b.ignoreZones,
    htfZonePaused:b.htfZonePaused,
    currentActiveLevel:b.currentActiveLevel,
    currentStructType:b.currentStructType,
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

// ── FIND LEVELS — EVERY SECOND ────────────────────
function findLevels(b) {
  const tfs=b.cfg.scanTF==='M1'?['M1']:b.cfg.scanTF==='M5'?['M5']:['M1','M5'];
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

  // Telegram alert — new level when 2+ TFs confirm downtrend
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
  // ONE next level per structure
  let np=parseFloat((lastLevel-struct.baseDiff).toFixed(2));
  let safety=0;
  while(safety<20){
    if(!tradedLevels.has(np.toFixed(2))&&!isLevelInIgnoredZone(b,np)&&!b.ignoredLevels.has(np.toFixed(2))){
      return [np];
    }
    np=parseFloat((np-struct.baseDiff).toFixed(2));
    safety++;
  }
  return [];
}

function isLevelInIgnoredZone(b,level) {
  return b.ignoreZones.some(z=>{
    const lo=Math.min(z.a,z.b),hi=Math.max(z.a,z.b);
    return level>=lo&&level<=hi;
  });
}

// ── HTF ZONE UPTREND DETECTION ────────────────────
function checkHTFZoneUptrend(b) {
  if(!b.ignoreZones.length||!b.currentPrice) return;
  const data=b.candles['M1'];
  if(!data||data.length<20) return;

  const nearZone=b.ignoreZones.find(z=>{
    const lo=Math.min(z.a,z.b),hi=Math.max(z.a,z.b);
    return b.currentPrice>=lo*0.995&&b.currentPrice<=hi*1.01;
  });

  if(!nearZone){
    if(b.htfZonePaused){
      const stillInAny=b.ignoreZones.some(z=>{
        const lo=Math.min(z.a,z.b);
        return b.currentPrice>=lo*0.998;
      });
      if(!stillInAny){
        log(b,'✅ Price broke out of zone downward — resuming');
        b.htfZonePaused=false;
        setStatus(b,'running','RUNNING');
        setTicker(b,'✅ Zone cleared — resuming...');
        broadcastBotState(b);
      }
    }
    return;
  }

  if(b.trendStatus['M1']!=='down') return;

  const recent=data.slice(-30);
  let swingLows=[],swingHighs=[];
  for(let i=2;i<recent.length-2;i++){
    if(recent[i].low<recent[i-1].low&&recent[i].low<recent[i-2].low&&
       recent[i].low<recent[i+1].low&&recent[i].low<recent[i+2].low)
      swingLows.push({price:recent[i].low,idx:i});
    if(recent[i].high>recent[i-1].high&&recent[i].high>recent[i-2].high&&
       recent[i].high>recent[i+1].high&&recent[i].high>recent[i+2].high)
      swingHighs.push({price:recent[i].high,idx:i});
  }

  if(swingLows.length>=2&&swingHighs.length>=1){
    const lastLow=swingLows[swingLows.length-1];
    const prevLow=swingLows[swingLows.length-2];
    const lastHigh=swingHighs[swingHighs.length-1];
    if(lastLow.price>prevLow.price&&lastLow.idx>prevLow.idx){
      if(b.currentPrice>lastHigh.price&&lastHigh.idx>prevLow.idx){
        if(!b.htfZonePaused){
          log(b,'⚠ Uptrend structure in zone — pausing');
          b.htfZonePaused=true;
          setStatus(b,'scanning','PAUSED — ZONE');
          const lo=Math.min(nearZone.a,nearZone.b),hi=Math.max(nearZone.a,nearZone.b);
          setTicker(b,`⚠ Uptrend in zone ${lo.toFixed(2)}–${hi.toFixed(2)} — paused`);
          telegram(b,`⚠ <b>Bot paused</b>\nUptrend structure forming in zone\nResumes on breakout or full uptrend`);
          broadcastBotState(b);
        }
      }
    }
  }

  const upCount=Object.values(b.trendStatus).filter(t=>t==='up').length;
  if(b.htfZonePaused&&upCount>=3){
    log(b,'✅ All TFs uptrend — zone resolved, waiting for next downtrend');
    b.htfZonePaused=false;
    setStatus(b,'running','RUNNING');
    setTicker(b,'✅ All TFs uptrend — waiting for next downtrend...');
    broadcastBotState(b);
  }
}

// ── ENTRY CHECK ───────────────────────────────────
function checkEntry(b) {
  if(!b.botActive||b.inTrade||!b.confirmedTrend) return;
  if(b.lossCountdownPaused||b.timeOffPaused||b.htfZonePaused) return;
  if(!b.activeStructures.length) return;
  if(b.cfg.maxTrades>0&&b.tradeCount>=b.cfg.maxTrades){stopBot(b);return;}

  const data=b.candles['M1'];
  if(!data||data.length<3) return;

  for(const struct of b.activeStructures){
    if(!struct.projectedLevels||!struct.projectedLevels.length) continue;
    const target=struct.projectedLevels[0];
    if(struct.tradedLevels.has(target.toFixed(2))) continue;
    if(isLevelInIgnoredZone(b,target)) continue;
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
    b.currentActiveLevel=target; b.currentStructType=struct.type;
    b.entryTargets=b.entryTargets.filter(e=>!(e.structId===struct.id&&Math.abs(e.level-target)<0.01));
    placeTrade(b);
    return;
  }
}

// ── PLACE TRADE ───────────────────────────────────
function placeTrade(b) {
  if(!b.derivWs||b.derivWs.readyState!==WebSocket.OPEN){b.inTrade=false;return;}
  b.inTrade=true;
  const duration=b.cfg.durationMins*60;
  const type={NOTOUCH:'NOTOUCH',TOUCH:'ONETOUCH',HIGHER:'CALL',LOWER:'PUT',RISE:'CALL',FALL:'PUT',CALL_MULT:'MULTUP',PUT_MULT:'MULTDOWN'}[b.cfg.command]||'NOTOUCH';

  if(b.cfg.command==='CALL_MULT'||b.cfg.command==='PUT_MULT'){
    b.derivWs.send(JSON.stringify({buy:1,price:b.cfg.stake,parameters:{contract_type:type,symbol:b.cfg.market,basis:'stake',amount:b.cfg.stake,currency:'USD',multiplier:b.cfg.multiplier}}));
    setTimeout(()=>{
      if(b.currentContractId&&b.derivWs?.readyState===WebSocket.OPEN)
        b.derivWs.send(JSON.stringify({proposal_open_contract:1,contract_id:b.currentContractId,subscribe:1}));
    },2000);
  } else {
    const params={contract_type:type,symbol:b.cfg.market,duration,duration_unit:'s',basis:'stake',amount:b.cfg.stake,currency:'USD'};
    if(['NOTOUCH','TOUCH','HIGHER','LOWER'].includes(b.cfg.command)) params.barrier=b.cfg.barrierOffset;
    b.derivWs.send(JSON.stringify({buy:1,price:b.cfg.stake,parameters:params}));
    setTimeout(()=>{
      if(b.currentContractId&&b.derivWs?.readyState===WebSocket.OPEN)
        b.derivWs.send(JSON.stringify({proposal_open_contract:1,contract_id:b.currentContractId}));
    },(duration+5)*1000);
  }
  broadcastBotState(b);
}

// ── LOSS CONTROL ──────────────────────────────────
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
function finalizeResult(b,profit) {
  if(!b.inTrade) return;
  b.inTrade=false; b.tradeCount++; b.sessionPnl+=profit;
  const won=profit>0;
  if(won) b.wins++; else b.losses++;
  const wr=Math.round((b.wins/b.tradeCount)*100);

  const card={
    id:Date.now(), tradeNum:b.tradeCount,
    time:new Date().toLocaleTimeString(), date:new Date().toLocaleDateString(),
    timestamp:Date.now(), won, profit,
    level:b.currentActiveLevel?.toFixed(2), struct:b.currentStructType,
    command:b.cfg.command, market:b.cfg.market, stake:b.cfg.stake, wr,
  };
  b.tradeLog.unshift(card);
  if(b.tradeLog.length>500) b.tradeLog.pop();
  saveData();

  log(b,`${won?'✅ WIN':'❌ LOSS'} #${b.tradeCount} | ${profit>=0?'+':''}$${profit.toFixed(2)} | WR:${wr}%`);
  const mkt=MKT_NAMES[b.cfg.market]||b.cfg.market;
  telegram(b,`${won?'✅ WIN':'❌ LOSS'}\nLevel: <b>${b.currentActiveLevel?.toFixed(2)}</b>\nProfit: <b>${profit>=0?'+':''}$${profit.toFixed(2)}</b>\nMarket: ${mkt}\nCommand: ${b.cfg.command}\nWR: ${wr}%`);

  b.currentContractId=null;
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
      startLossCountdown(b,b.cfg.cooldownSecs);
    }
  }
}

// ── DERIV CONNECTION ──────────────────────────────
function connectDeriv(b) {
  if(b.derivWs){try{b.derivWs.terminate();}catch(e){}}
  log(b,'🔌 Connecting to Deriv...');
  setStatus(b,'connecting','CONNECTING');
  b.derivWs=new WebSocket(`wss://ws.binaryws.com/websockets/v3?app_id=${APP_ID}`);

  b.derivWs.on('open',()=>{
    log(b,'🔗 Authorizing...');
    b.derivWs.send(JSON.stringify({authorize:b.cfg.apiToken}));
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
      ['M1','M5','M15','H1','H4','D1'].forEach(tf=>fetchCandles(b,tf));
      startScanner(b);
      broadcastBotState(b);
    }

    if(d.msg_type==='tick'){
      b.currentPrice=parseFloat(d.tick.quote);
      broadcast({type:'price',id:b.id,price:b.currentPrice});
      if(b.botActive&&!b.inTrade){ checkEntry(b); checkHTFZoneUptrend(b); }
    }

    if(d.msg_type==='candles'){
      const gran=d.echo_req.granularity;
      const tf=gran===60?'M1':gran===300?'M5':gran===900?'M15':gran===3600?'H1':gran===14400?'H4':'D1';
      b.candles[tf]=d.candles.map(c=>({time:c.epoch,open:parseFloat(c.open),high:parseFloat(c.high),low:parseFloat(c.low),close:parseFloat(c.close)}));
      if(['M1','M5','M15'].includes(tf)) analyzeTrend(b,tf);
      broadcast({type:'candles',id:b.id,tf,candles:b.candles[tf].slice(-100)});
    }

    if(d.msg_type==='ohlc'){
      const gran=d.ohlc.granularity;
      const tf=gran===60?'M1':gran===300?'M5':gran===900?'M15':gran===3600?'H1':gran===14400?'H4':'D1';
      const c={time:d.ohlc.open_time,open:parseFloat(d.ohlc.open),high:parseFloat(d.ohlc.high),low:parseFloat(d.ohlc.low),close:parseFloat(d.ohlc.close)};
      if(!b.candles[tf]) b.candles[tf]=[];
      if(b.candles[tf].length&&b.candles[tf][b.candles[tf].length-1].time===c.time) b.candles[tf][b.candles[tf].length-1]=c;
      else{b.candles[tf].push(c);if(b.candles[tf].length>300)b.candles[tf].shift();}
      if(['M1','M5','M15'].includes(tf)) analyzeTrend(b,tf);
      broadcast({type:'candle_update',id:b.id,tf,candle:c});
    }

    if(d.msg_type==='buy'){
      if(d.error){log(b,'❌ '+d.error.message);b.inTrade=false;broadcastBotState(b);return;}
      b.currentContractId=d.buy.contract_id;
      log(b,`📝 Contract: ${b.currentContractId}`);
    }

    if(d.msg_type==='proposal_open_contract'){
      const con=d.proposal_open_contract; if(!con) return;
      const profit=parseFloat(con.profit)||0;
      if(b.cfg.command==='CALL_MULT'||b.cfg.command==='PUT_MULT'){
        if(profit>=b.cfg.takeProfit||profit<=-b.cfg.stopLoss)
          b.derivWs.send(JSON.stringify({sell:b.currentContractId,price:0}));
      }
      if(con.status==='sold'||con.is_expired||con.is_settleable) finalizeResult(b,profit);
    }

    if(d.msg_type==='sell'){
      if(d.sell) finalizeResult(b,parseFloat(d.sell.sold_for)-b.cfg.stake);
    }
  });

  b.derivWs.on('close',()=>{
    log(b,'Disconnected');
    b.botActive=false; stopScanner(b);
    setStatus(b,'stopped','DISCONNECTED');
    broadcastBotState(b);
    if(b.userStarted){
      if(b.reconnectTimer) clearTimeout(b.reconnectTimer);
      b.reconnectTimer=setTimeout(()=>connectDeriv(b),5000);
    }
  });

  b.derivWs.on('error',(e)=>log(b,'WS error: '+e.message));
}

function fetchCandles(b,tf) {
  if(!b.derivWs||b.derivWs.readyState!==WebSocket.OPEN) return;
  const gran=tf==='M1'?60:tf==='M5'?300:tf==='M15'?900:tf==='H1'?3600:tf==='H4'?14400:86400;
  b.derivWs.send(JSON.stringify({ticks_history:b.cfg.market,adjust_start_time:1,count:200,end:'latest',granularity:gran,start:1,style:'candles',subscribe:1}));
}

function startScanner(b) {
  if(b.scanInterval) clearInterval(b.scanInterval);
  findLevels(b);
  b.scanInterval=setInterval(()=>{
    if(!b.botActive||b.inTrade||b.lossCountdownPaused||b.timeOffPaused) return;
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

    if(msg.type==='start'){
      if(msg.cfg) b.cfg={...b.cfg,...msg.cfg};
      if(!b.cfg.apiToken){ws.send(JSON.stringify({type:'error',id:b.id,msg:'No API token'}));return;}
      b.tradeCount=0;b.wins=0;b.losses=0;b.sessionPnl=0;
      b.consecutiveLosses=0;b.lossCountdownPaused=false;
      b.activeStructures=[];b.entryTargets=[];
      b.userStarted=true; saveData(); connectDeriv(b);
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

    if(msg.type==='add_zone'){
      b.ignoreZones.push({a:parseFloat(msg.a),b:parseFloat(msg.b)});
      log(b,`🚫 Zone: ${msg.a}–${msg.b}`);
      b.activeStructures.forEach(s=>{s.projectedLevels=computeProjectedLevels(b,s,s.tradedLevels);});
      broadcastBotState(b);
    }

    if(msg.type==='remove_zone'){
      b.ignoreZones.splice(msg.idx,1);
      log(b,'✅ Zone removed');
      b.activeStructures.forEach(s=>{s.projectedLevels=computeProjectedLevels(b,s,s.tradedLevels);});
      broadcastBotState(b);
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
    ignoredLevels:[...b.ignoredLevels],ignoreZones:b.ignoreZones,
    htfZonePaused:b.htfZonePaused,
    currentActiveLevel:b.currentActiveLevel,currentStructType:b.currentStructType,
    tradeCount:b.tradeCount,wins:b.wins,losses:b.losses,sessionPnl:b.sessionPnl,
    consecutiveLosses:b.consecutiveLosses,
    lossCountdownPaused:b.lossCountdownPaused,lossCountdownRemaining:b.lossCountdownRemaining,lossCountdownTotal:b.lossCountdownTotal,
    timeOffPaused:b.timeOffPaused,timeOffRemaining:b.timeOffRemaining,timeOffTotal:b.timeOffTotal,
    tickerMsg:b.tickerMsg,statusText:b.statusText,cfg:b.cfg,
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
