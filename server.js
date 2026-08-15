/**
 * Royale X relay server
 * ----------------------------------------------------------------------
 * A single-file WebSocket relay implementing every message type the
 * royale-x.html client sends/expects. Deploy this on Render (or any
 * Node host) as a "Web Service", then point the client's SERVER_URL at
 * its wss:// address.
 *
 * Run locally:   npm install ws && node server.js
 * Render:        Build command: npm install   Start command: node server.js
 *
 * Data (friends / leaderboard / admin state) is persisted to a local
 * JSON file (data.json) so it survives restarts. On Render's free tier
 * the filesystem is wiped on redeploy, but survives normal restarts —
 * good enough for a game like this without a real database.
 * ----------------------------------------------------------------------
 */
const WebSocket = require("ws");
const fs = require("fs");
const crypto = require("crypto");

const PORT = process.env.PORT || 8080;
const ADMIN_KEY = process.env.ADMIN_KEY || "letmein123"; // change this in Render's env vars!
const DATA_FILE = __dirname + "/data.json";

// ---------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------
function loadData(){
  try{ return JSON.parse(fs.readFileSync(DATA_FILE, "utf8")); }
  catch(e){
    return {
      friends: {},        // playerId -> [playerId, ...]
      leaderboard: [],     // [{id,name,score,kills,wins}, ...]
      banned: [],           // [playerId, ...]
      season: 1,
      seasonFrames: [{ season:1, id:"frame_season1", mid:200, name:"1-ci Sezon Çempionu", color:"#ffcf4d", glow:"#fff2b0" }]
    };
  }
}
let DATA = loadData();
let saveTimer = null;
function persistData(){
  clearTimeout(saveTimer);
  saveTimer = setTimeout(()=>{
    try{ fs.writeFileSync(DATA_FILE, JSON.stringify(DATA)); }catch(e){ console.error("save failed", e); }
  }, 500);
}

// ---------------------------------------------------------------------
// In-memory runtime state
// ---------------------------------------------------------------------
const clientsById = new Map();   // playerId -> { ws, id, name, room, isAdmin }
const pendingFriendReqs = new Map(); // requestId -> { fromId, targetId }
const pendingSquadInvites = new Map(); // requestId -> { fromId, targetId }
const queue = [];                // [{ id, squad:[ids] }]
const rooms = new Map();         // roomId -> { players: Set<playerId> }
const chatHistory = [];          // last 50 { name, text, ts }
let maintenance = { enabled:false, minutes:0, message:"" };
let marketOverrides = {};        // mid -> { price, discountPrice, saleEndsAt, enabled }
let eventWarningText = "";

function uid(){ return crypto.randomBytes(8).toString("hex"); }
function send(ws, obj){ if(ws && ws.readyState===1) try{ ws.send(JSON.stringify(obj)); }catch(e){} }
function sendToId(id, obj){ const c = clientsById.get(id); if(c) send(c.ws, obj); }
function broadcastAll(obj, exceptId){
  clientsById.forEach((c, id)=>{ if(id!==exceptId) send(c.ws, obj); });
}
function broadcastRoom(roomId, obj, exceptId){
  const room = rooms.get(roomId);
  if(!room) return;
  room.players.forEach(id=>{ if(id!==exceptId) sendToId(id, obj); });
}
function friendsOf(id){ return DATA.friends[id] || []; }
function areFriends(a,b){ return friendsOf(a).includes(b); }
function friendsUpdatePayload(id){
  return {
    type: "friendsUpdate",
    friends: friendsOf(id).map(fid=>{
      const c = clientsById.get(fid);
      return { id: fid, name: c ? c.name : fid, status: c ? "online" : "offline" };
    })
  };
}

const wss = new WebSocket.Server({ port: PORT });
console.log("Royale X relay listening on port " + PORT);

wss.on("connection", (ws)=>{
  let me = null; // set once "hello" arrives

  ws.on("message", (raw)=>{
    let msg;
    try{ msg = JSON.parse(raw); }catch(e){ return; }
    if(!msg || typeof msg.type!=="string") return;

    // ---------------- hello ----------------
    if(msg.type==="hello"){
      if(!msg.id || !msg.name) return;
      if(DATA.banned.includes(msg.id)){ send(ws, { type:"banned", reason:"Banned" }); ws.close(); return; }
      me = { ws, id: msg.id, name: String(msg.name).slice(0,16), room:null, isAdmin:false };
      clientsById.set(me.id, me);
      send(ws, friendsUpdatePayload(me.id));
      send(ws, { type:"chatHistory", messages: chatHistory });
      send(ws, { type:"seasonUpdate", season: DATA.season, seasonFrames: DATA.seasonFrames });
      if(maintenance.enabled) send(ws, { type:"maintenanceUpdate", ...maintenance });
      if(eventWarningText) send(ws, { type:"eventWarning", text: eventWarningText });
      send(ws, { type:"priceOverrides", overrides: marketOverrides });
      // let this player's online friends know they've come online
      friendsOf(me.id).forEach(fid=>{ if(clientsById.has(fid)) sendToId(fid, friendsUpdatePayload(fid)); });
      return;
    }
    if(!me) return; // everything below requires hello first

    // ---------------- friends ----------------
    if(msg.type==="addFriend"){
      const targetId = msg.targetId;
      const target = clientsById.get(targetId);
      if(!target){ send(ws, { type:"addFriendResult", ok:false, msg:"Player not online." }); return; }
      if(areFriends(me.id, targetId)){ send(ws, { type:"addFriendResult", ok:false, msg:"Already friends." }); return; }
      const requestId = uid();
      pendingFriendReqs.set(requestId, { fromId: me.id, targetId });
      send(target.ws, { type:"friendRequestIncoming", requestId, fromId: me.id, fromName: me.name });
      send(ws, { type:"addFriendResult", ok:true, msg:"Request sent." });
      return;
    }
    if(msg.type==="friendRequestResponse"){
      const req = pendingFriendReqs.get(msg.requestId);
      pendingFriendReqs.delete(msg.requestId);
      if(!req) return;
      if(msg.accept){
        DATA.friends[req.fromId] = DATA.friends[req.fromId] || [];
        DATA.friends[req.targetId] = DATA.friends[req.targetId] || [];
        if(!DATA.friends[req.fromId].includes(req.targetId)) DATA.friends[req.fromId].push(req.targetId);
        if(!DATA.friends[req.targetId].includes(req.fromId)) DATA.friends[req.targetId].push(req.fromId);
        persistData();
        sendToId(req.fromId, friendsUpdatePayload(req.fromId));
        sendToId(req.targetId, friendsUpdatePayload(req.targetId));
      }
      return;
    }

    // ---------------- public profile lookup (frame/rank, no privacy concern) ----------------
    if(msg.type==="profileRequest"){
      const target = clientsById.get(msg.targetId);
      if(!target){ send(ws, { type:"profileResult", found:false }); return; }
      send(target.ws, { type:"profileRequestIncoming", requesterId: me.id });
      return;
    }
    if(msg.type==="profileResponse"){
      sendToId(msg.requesterId, { type:"profileResult", found:true, id:msg.id, name:msg.name, rankTier:msg.rankTier, frame:msg.frame, icon:msg.icon });
      return;
    }

    // ---------------- squad invites ----------------
    if(msg.type==="squadInvite"){
      const target = clientsById.get(msg.targetId);
      if(!target) return;
      const requestId = uid();
      pendingSquadInvites.set(requestId, { fromId: me.id, targetId: msg.targetId });
      send(target.ws, { type:"squadInviteIncoming", requestId, fromId: me.id, fromName: me.name });
      return;
    }
    if(msg.type==="squadInviteResponse"){
      const req = pendingSquadInvites.get(msg.requestId);
      pendingSquadInvites.delete(msg.requestId);
      if(!req) return;
      sendToId(req.fromId, { type:"squadInviteResult", byId: req.targetId, accept: !!msg.accept });
      return;
    }

    // ---------------- matchmaking ----------------
    if(msg.type==="queue"){
      if(queue.some(q=>q.id===me.id)) return;
      queue.push({ id: me.id, squad: Array.isArray(msg.squad) ? msg.squad : [] });
      tryFormMatch();
      return;
    }
    if(msg.type==="leaveQueue"){
      const idx = queue.findIndex(q=>q.id===me.id);
      if(idx>=0) queue.splice(idx,1);
      return;
    }
    if(msg.type==="leaveMatch"){
      if(me.room){
        const room = rooms.get(me.room);
        if(room){ room.players.delete(me.id); broadcastRoom(me.room, { type:"peerLeft", id: me.id }, me.id); if(room.players.size===0) rooms.delete(me.room); }
        me.room = null;
      }
      return;
    }

    // ---------------- in-match relay ----------------
    if(msg.type==="state"){
      if(!me.room) return;
      broadcastRoom(me.room, { type:"peerState", id:me.id, x:msg.x, y:msg.y, angle:msg.angle, hp:msg.hp, weapon:msg.weapon, skin:msg.skin }, me.id);
      return;
    }
    if(msg.type==="shoot"){
      if(!me.room) return;
      broadcastRoom(me.room, { type:"peerShoot", id:me.id, x:msg.x, y:msg.y, angle:msg.angle }, me.id);
      return;
    }
    if(msg.type==="hit"){
      if(!me.room) return;
      sendToId(msg.targetId, { type:"peerHit", id:me.id, dmg:msg.dmg });
      return;
    }
    if(msg.type==="downed"){
      if(!me.room) return;
      broadcastRoom(me.room, { type:"peerDowned", id:me.id, killerId: msg.killerId||null }, null);
      return;
    }
    if(msg.type==="skinKill"){
      if(!me.room) return;
      broadcastRoom(me.room, { type:"skinKill", skinId:msg.skinId, killer:msg.killer, x:msg.x, y:msg.y }, me.id);
      return;
    }

    // ---------------- chat ----------------
    if(msg.type==="chatMessage"){
      const text = String(msg.text||"").slice(0,200).trim();
      if(!text) return;
      const entry = { name: me.name, text, ts: Date.now() };
      chatHistory.push(entry);
      if(chatHistory.length>50) chatHistory.shift();
      broadcastAll({ type:"chatMessage", ...entry });
      return;
    }

    // ---------------- leaderboard ----------------
    if(msg.type==="leaderboardSubmit"){
      const i = DATA.leaderboard.findIndex(e=>e.id===msg.id);
      const entry = { id: msg.id, name: msg.name, score: msg.score||0, kills: msg.kills||0, wins: msg.wins||0 };
      if(i>=0){ if(entry.score>DATA.leaderboard[i].score) DATA.leaderboard[i]=entry; }
      else DATA.leaderboard.push(entry);
      DATA.leaderboard.sort((a,b)=>b.score-a.score);
      DATA.leaderboard = DATA.leaderboard.slice(0,100);
      persistData();
      broadcastAll({ type:"leaderboardUpdate", entries: DATA.leaderboard.slice(0,50) });
      return;
    }
    if(msg.type==="leaderboardRequest"){
      send(ws, { type:"leaderboardUpdate", entries: DATA.leaderboard.slice(0,50) });
      return;
    }

    // ---------------- admin ----------------
    if(msg.type==="adminAuth"){
      me.isAdmin = (msg.key === ADMIN_KEY);
      send(ws, { type:"adminAuthResult", ok: me.isAdmin });
      return;
    }
    if(msg.type.indexOf("admin")===0){
      if(!me.isAdmin) return; // silently ignore admin actions from non-admins
      handleAdminAction(msg, me);
      return;
    }
  });

  ws.on("close", ()=>{
    if(!me) return;
    clientsById.delete(me.id);
    const qi = queue.findIndex(q=>q.id===me.id);
    if(qi>=0) queue.splice(qi,1);
    if(me.room){
      const room = rooms.get(me.room);
      if(room){ room.players.delete(me.id); broadcastRoom(me.room, { type:"peerLeft", id: me.id }, me.id); if(room.players.size===0) rooms.delete(me.room); }
    }
    friendsOf(me.id).forEach(fid=>{ if(clientsById.has(fid)) sendToId(fid, friendsUpdatePayload(fid)); });
  });
});

// ---------------------------------------------------------------------
// Matchmaking: groups queued players into rooms, keeping squads together.
// Real players relay peer-state to each other; each client fills the
// rest of the 60-player lobby with local NPCs (existing client design).
// ---------------------------------------------------------------------
const MATCH_MAX_REAL = 8;
const MATCH_MAX_WAIT_MS = 12000;
let queueOpenedAt = null;

function tryFormMatch(){
  if(queue.length===0) return;
  if(queueOpenedAt===null) queueOpenedAt = Date.now();
  const waited = Date.now()-queueOpenedAt;
  if(queue.length < 2 && waited < MATCH_MAX_WAIT_MS) return; // give a couple seconds for others to join

  // squad-aware grouping: pull whole squads out together
  const batch = [];
  const used = new Set();
  for(const q of queue){
    if(used.has(q.id)) continue;
    if(batch.length>=MATCH_MAX_REAL) break;
    batch.push(q.id); used.add(q.id);
    q.squad.forEach(sid=>{
      if(!used.has(sid) && queue.some(x=>x.id===sid) && batch.length<MATCH_MAX_REAL){
        batch.push(sid); used.add(sid);
      }
    });
  }
  if(batch.length===0) return;

  // remove batched players from queue
  for(let i=queue.length-1;i>=0;i--) if(used.has(queue[i].id)) queue.splice(i,1);
  queueOpenedAt = queue.length ? Date.now() : null;

  const roomId = uid();
  const players = batch.map(id=>{
    const c = clientsById.get(id);
    return c ? { id, name: c.name } : null;
  }).filter(Boolean);
  if(players.length===0) return;

  rooms.set(roomId, { players: new Set(players.map(p=>p.id)) });
  players.forEach(p=>{
    const c = clientsById.get(p.id);
    if(c) c.room = roomId;
    sendToId(p.id, { type:"matchStart", roomId, players });
  });
}
setInterval(tryFormMatch, 2000);

// ---------------------------------------------------------------------
// Admin actions
// ---------------------------------------------------------------------
function handleAdminAction(msg, me){
  switch(msg.type){
    case "adminGiveCoins":
      sendToId(msg.targetId, { type:"coinGrant", amount: msg.amount });
      send(me.ws, { type:"adminActionResult", ok:true, msg:"Coins granted." });
      break;
    case "adminBan":
      if(!DATA.banned.includes(msg.targetId)) DATA.banned.push(msg.targetId);
      persistData();
      sendToId(msg.targetId, { type:"banned", reason:"Banned by admin" });
      const t = clientsById.get(msg.targetId); if(t) t.ws.close();
      break;
    case "adminUnban":
      DATA.banned = DATA.banned.filter(id=>id!==msg.targetId);
      persistData();
      break;
    case "adminMaintenance":
      maintenance = { enabled: !!msg.enabled, minutes: msg.minutes||0, message: msg.message||"" };
      broadcastAll({ type:"maintenanceUpdate", ...maintenance });
      break;
    case "adminSetPrice":
      marketOverrides[msg.itemId] = Object.assign({}, marketOverrides[msg.itemId], { price: msg.price });
      broadcastAll({ type:"priceOverrides", overrides: marketOverrides });
      break;
    case "adminClearPrice":
      delete marketOverrides[msg.itemId];
      broadcastAll({ type:"priceOverrides", overrides: marketOverrides });
      break;
    case "adminMarketOverride":
      marketOverrides[msg.mid] = Object.assign({}, marketOverrides[msg.mid], { price: msg.price, discountPrice: msg.discountPrice });
      broadcastAll({ type:"priceOverrides", overrides: marketOverrides });
      break;
    case "adminMarketSale":
      marketOverrides[msg.mid] = Object.assign({}, marketOverrides[msg.mid], { saleEndsAt: Date.now()+msg.minutes*60000 });
      broadcastAll({ type:"priceOverrides", overrides: marketOverrides });
      break;
    case "adminMarketToggle":
      marketOverrides[msg.mid] = Object.assign({}, marketOverrides[msg.mid], { enabled: msg.enabled });
      broadcastAll({ type:"priceOverrides", overrides: marketOverrides });
      break;
    case "adminBroadcast":
      broadcastAll({ type:"announcement", text: msg.text });
      break;
    case "adminEventWarning":
      eventWarningText = msg.text||"";
      broadcastAll({ type:"eventWarning", text: eventWarningText });
      break;
    case "adminZombieForce":
      broadcastAll({ type:"announcement", text: msg.action==="start" ? "🧟 Zombie event incoming!" : "" });
      break;
    case "adminWeatherTrigger":
      broadcastAll({ type:"announcement", text: msg.weatherType ? ("🌩️ Weather event: "+msg.weatherType) : "" });
      break;
    case "adminChestSkinAdd":
    case "adminChestSkinRemove":
      // purely cosmetic pool, kept client-side in ADMIN_STATE (localStorage);
      // nothing to relay server-side for these two
      break;
    case "adminEndSeason": {
      // whoever's currently #1 on the leaderboard gets credit; the client
      // grants the frame locally when it reaches top rank, this just
      // rotates the shared season counter for everyone
      DATA.season = msg.season;
      persistData();
      broadcastAll({ type:"seasonUpdate", season: DATA.season, seasonFrames: DATA.seasonFrames });
      break;
    }
    case "adminNewSeason": {
      DATA.season = msg.season;
      const newFrame = { season: DATA.season, id:"frame_season"+DATA.season, mid: 200+(DATA.season-1), name: DATA.season+"-ci Sezon Çempionu", color:"#ffcf4d", glow:"#fff2b0" };
      if(!DATA.seasonFrames.some(f=>f.season===DATA.season)) DATA.seasonFrames.push(newFrame);
      persistData();
      broadcastAll({ type:"seasonUpdate", season: DATA.season, seasonFrames: DATA.seasonFrames });
      break;
    }
  }
}
