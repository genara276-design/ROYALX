/**
 * ROYALE X — Multiplayer Relay Server
 * ------------------------------------------------------------------
 * A small, dependency-light Node.js WebSocket server that gives the
 * game real online features:
 *   - persistent player profiles (name + 7-digit ID)
 *   - real friend requests / friend lists with live online status
 *   - real matchmaking (queues actual connected players together)
 *   - real-time position/action sync between real players in a match
 *   - a global lobby chat between real connected players
 *   - a server-authenticated admin system (coins, bans, maintenance
 *     mode, market price overrides)
 *
 * NPCs are NOT synced over the network — each player's client still
 * simulates its own NPCs locally (same as the offline build). Only
 * real human players are relayed through this server.
 *
 * Requirements: Node.js 18+, and the "ws" package:
 *     npm install ws
 *
 * Run:
 *     node server.js
 * By default it listens on process.env.PORT or 8080.
 * ------------------------------------------------------------------
 * ADMIN PASSWORD — READ THIS
 * The admin password is set via the ADMIN_KEY environment variable on
 * whatever host you deploy this to (Render/Railway/Fly all have a
 * place to set env vars in their dashboard). If you don't set one, it
 * defaults to "1403" as requested — but that default is public
 * (it's written in this very file), so anyone who reads this source
 * knows it. Set your own ADMIN_KEY before going live with real
 * players, or anyone could grant themselves coins, ban people, or
 * shut the game down. This is the ONLY place the real password is
 * compared — it is never sent to or stored in the game client, so
 * changing it here (and only here) is all you need to do.
 * ------------------------------------------------------------------
 * HOSTING
 * This process must be deployed somewhere with a public URL before
 * the game client can use it (Claude cannot host it for you). Any
 * Node-friendly host works, e.g. Render.com, Railway.app, Fly.io, or
 * your own VPS. Once deployed you'll get a URL like
 * wss://your-app.onrender.com — paste that into SERVER_URL near the
 * top of index.html's <script>.
 * ------------------------------------------------------------------
 */

const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const ADMIN_KEY = process.env.ADMIN_KEY || "1403"; // change this via env var for real deployments
const DATA_FILE = path.join(__dirname, "royalex-server-data.json");
const MATCH_SIZE_TARGET = 50;        // total match "slots" (real players + client-simulated NPCs)
const MATCHMAKING_WINDOW_MS = 15000; // matches the client's 15s "Finding Match" screen
const CHAT_HISTORY_MAX = 50;

/* ---------------- persistence (flat JSON file — simple by design) ---------------- */
function loadData(){
  try{ return JSON.parse(fs.readFileSync(DATA_FILE, "utf8")); }
  catch(e){
    return {
      profiles: {},       // id -> {name}
      friends: {},        // id -> [ids]
      banned: {},         // id -> {reason, at}
      pendingCoins: {},   // id -> total coins to deliver next time they connect
      priceOverrides: {}, // itemId -> new price
      maintenance: { active:false, until:0, message:"" },
      chatHistory: []      // [{id,name,text,at}]
    };
  }
}
function saveData(data){
  try{ fs.writeFileSync(DATA_FILE, JSON.stringify(data)); }catch(e){ console.error("save failed", e); }
}
let DB = loadData();
DB.banned = DB.banned || {};
DB.pendingCoins = DB.pendingCoins || {};
DB.priceOverrides = DB.priceOverrides || {};
DB.maintenance = DB.maintenance || { active:false, until:0, message:"" };
DB.chatHistory = DB.chatHistory || [];

/* ---------------- live state ---------------- */
const clients = new Map();     // id -> { ws, name, isAdmin }
const queue = [];              // ids waiting for a match
let queueTimer = null;
const rooms = new Map();       // roomId -> Set of ids
const playerRoom = new Map();  // id -> roomId
let maintenanceTimer = null;

function send(ws, obj){ try{ ws.send(JSON.stringify(obj)); }catch(e){} }
function broadcastAll(obj){ clients.forEach(entry=>send(entry.ws, obj)); }
function broadcastFriendsUpdate(id){
  const entry = clients.get(id);
  if(!entry) return;
  const friendIds = DB.friends[id] || [];
  const list = friendIds.map(fid=>({
    id: fid,
    name: (DB.profiles[fid]||{}).name || "Player",
    status: clients.has(fid) ? "online" : "offline"
  }));
  send(entry.ws, { type:"friendsUpdate", friends:list });
}
function notifyFriendsOfStatusChange(id){
  const friendIds = DB.friends[id] || [];
  friendIds.forEach(fid=>{ if(clients.has(fid)) broadcastFriendsUpdate(fid); });
}
function currentMaintenancePayload(){
  const active = DB.maintenance.active && DB.maintenance.until > Date.now();
  return { type:"maintenanceUpdate", active, until: DB.maintenance.until, message: DB.maintenance.message };
}
function scheduleMaintenanceEnd(){
  if(maintenanceTimer) clearTimeout(maintenanceTimer);
  if(!DB.maintenance.active) return;
  const msLeft = DB.maintenance.until - Date.now();
  if(msLeft<=0){ endMaintenance(); return; }
  maintenanceTimer = setTimeout(endMaintenance, msLeft);
}
function endMaintenance(){
  DB.maintenance = { active:false, until:0, message:"" };
  saveData(DB);
  broadcastAll(currentMaintenancePayload());
}

/* ---------------- matchmaking ---------------- */
function broadcastQueueUpdate(){
  queue.forEach(id=>{
    const entry = clients.get(id);
    if(entry) send(entry.ws, { type:"queueUpdate", realPlayers: queue.length, target: MATCH_SIZE_TARGET });
  });
}
function startMatchmakingWindowIfNeeded(){
  if(queueTimer) return;
  queueTimer = setTimeout(()=>{
    queueTimer = null;
    formMatchFromQueue();
  }, MATCHMAKING_WINDOW_MS);
}
function formMatchFromQueue(){
  if(queue.length===0) return;
  const roomId = "room_"+Date.now()+"_"+Math.floor(Math.random()*1e6);
  const members = queue.splice(0, queue.length); // everyone currently queued joins the same room
  const npcCount = Math.max(0, MATCH_SIZE_TARGET - members.length);
  const roster = members.map(id=>({ id, name:(DB.profiles[id]||{}).name || "Player" }));
  rooms.set(roomId, new Set(members));
  members.forEach(id=>{
    playerRoom.set(id, roomId);
    const entry = clients.get(id);
    if(entry) send(entry.ws, { type:"matchStart", roomId, players: roster, npcCount });
  });
}

/* ---------------- server setup ---------------- */
const httpServer = http.createServer((req,res)=>{
  res.writeHead(200, {"Content-Type":"text/plain"});
  res.end("ROYALE X relay server is running.\n");
});
const wss = new WebSocketServer({ server: httpServer });

wss.on("connection", (ws)=>{
  let myId = null;

  ws.on("message", (raw)=>{
    let msg;
    try{ msg = JSON.parse(raw); }catch(e){ return; }

    if(msg.type === "hello"){
      const candidateId = String(msg.id||"").slice(0,7);
      const name = String(msg.name||"Player").slice(0,16);
      if(!/^\d{7}$/.test(candidateId)) return;
      if(DB.banned[candidateId]){
        send(ws, { type:"banned", reason: DB.banned[candidateId].reason||"" });
        ws.close();
        return;
      }
      myId = candidateId;
      clients.set(myId, { ws, name, isAdmin:false });
      DB.profiles[myId] = { name };
      if(!DB.friends[myId]) DB.friends[myId] = [];
      saveData(DB);
      send(ws, { type:"helloAck", id: myId });
      send(ws, currentMaintenancePayload());
      send(ws, { type:"priceOverrides", overrides: DB.priceOverrides });
      send(ws, { type:"chatHistory", messages: DB.chatHistory.slice(-CHAT_HISTORY_MAX) });
      // deliver any coins an admin granted while this player was offline
      if(DB.pendingCoins[myId]){
        send(ws, { type:"coinGrant", amount: DB.pendingCoins[myId] });
        delete DB.pendingCoins[myId];
        saveData(DB);
      }
      broadcastFriendsUpdate(myId);
      notifyFriendsOfStatusChange(myId);
      return;
    }
    if(!myId) return; // must say hello first

    if(msg.type === "addFriend"){
      const targetId = String(msg.targetId||"").slice(0,7);
      if(!/^\d{7}$/.test(targetId) || targetId===myId) return;
      if(!DB.profiles[targetId]){
        send(ws, { type:"addFriendResult", ok:false, msg:"That ID hasn't been seen online yet." });
        return;
      }
      DB.friends[myId] = DB.friends[myId] || [];
      if(!DB.friends[myId].includes(targetId)) DB.friends[myId].push(targetId);
      saveData(DB);
      send(ws, { type:"addFriendResult", ok:true });
      broadcastFriendsUpdate(myId);
      return;
    }

    if(msg.type === "queue"){
      if(!queue.includes(myId)) queue.push(myId);
      broadcastQueueUpdate();
      startMatchmakingWindowIfNeeded();
      if(queue.length >= MATCH_SIZE_TARGET){
        clearTimeout(queueTimer); queueTimer = null;
        formMatchFromQueue();
      }
      return;
    }
    if(msg.type === "leaveQueue"){
      const idx = queue.indexOf(myId);
      if(idx>=0) queue.splice(idx,1);
      broadcastQueueUpdate();
      return;
    }

    if(msg.type === "hit"){
      const roomId = playerRoom.get(myId);
      if(!roomId) return;
      const members = rooms.get(roomId);
      if(!members || !members.has(msg.targetId)) return;
      const target = clients.get(msg.targetId);
      if(target) send(target.ws, { type:"peerHit", id: myId, dmg: msg.dmg });
      return;
    }

    if(msg.type === "state" || msg.type === "shoot" || msg.type === "downed"){
      const roomId = playerRoom.get(myId);
      if(!roomId) return;
      const members = rooms.get(roomId);
      if(!members) return;
      members.forEach(pid=>{
        if(pid===myId) return;
        const entry = clients.get(pid);
        if(entry) send(entry.ws, Object.assign({}, msg, { type:"peer"+msg.type.charAt(0).toUpperCase()+msg.type.slice(1), id: myId }));
      });
      return;
    }

    if(msg.type === "leaveMatch"){
      const roomId = playerRoom.get(myId);
      if(roomId){
        const members = rooms.get(roomId);
        if(members){
          members.delete(myId);
          members.forEach(pid=>{ const e=clients.get(pid); if(e) send(e.ws,{type:"peerLeft", id:myId}); });
          if(members.size===0) rooms.delete(roomId);
        }
        playerRoom.delete(myId);
      }
      return;
    }

    /* ---------------- global lobby chat ---------------- */
    if(msg.type === "chatMessage"){
      const text = String(msg.text||"").slice(0,200).trim();
      if(!text) return;
      const entry = clients.get(myId);
      const chatMsg = { id:myId, name:(entry&&entry.name)||"Player", text, at:Date.now() };
      DB.chatHistory.push(chatMsg);
      if(DB.chatHistory.length > CHAT_HISTORY_MAX) DB.chatHistory = DB.chatHistory.slice(-CHAT_HISTORY_MAX);
      saveData(DB);
      broadcastAll({ type:"chatMessage", message: chatMsg });
      return;
    }

    /* ---------------- admin: authentication ---------------- */
    if(msg.type === "adminAuth"){
      const ok = String(msg.key||"") === ADMIN_KEY;
      if(ok){ const entry = clients.get(myId); if(entry) entry.isAdmin = true; }
      send(ws, { type:"adminAuthResult", ok });
      return;
    }

    // everything below requires this connection to have authenticated as admin
    const me = clients.get(myId);
    if(!me || !me.isAdmin){
      if(["adminGiveCoins","adminBan","adminUnban","adminMaintenance","adminSetPrice","adminClearPrice","adminBroadcast"].includes(msg.type)){
        send(ws, { type:"adminActionResult", ok:false, msg:"Not authenticated as admin." });
      }
      return;
    }

    if(msg.type === "adminGiveCoins"){
      const targetId = String(msg.targetId||"").slice(0,7);
      const amount = Math.max(0, Math.min(1000000, parseInt(msg.amount,10)||0));
      if(!/^\d{7}$/.test(targetId) || amount<=0){ send(ws,{type:"adminActionResult",ok:false,msg:"Invalid target/amount."}); return; }
      const target = clients.get(targetId);
      if(target){ send(target.ws, { type:"coinGrant", amount }); }
      else { DB.pendingCoins[targetId] = (DB.pendingCoins[targetId]||0) + amount; saveData(DB); }
      send(ws, { type:"adminActionResult", ok:true, msg:"Granted "+amount+" coins to "+targetId+(target?" (delivered now)":" (will deliver on next login)") });
      return;
    }

    if(msg.type === "adminBan"){
      const targetId = String(msg.targetId||"").slice(0,7);
      if(!/^\d{7}$/.test(targetId)){ send(ws,{type:"adminActionResult",ok:false,msg:"Invalid ID."}); return; }
      DB.banned[targetId] = { reason: String(msg.reason||"").slice(0,140), at: Date.now() };
      saveData(DB);
      const target = clients.get(targetId);
      if(target){ send(target.ws, { type:"banned", reason: DB.banned[targetId].reason }); target.ws.close(); }
      send(ws, { type:"adminActionResult", ok:true, msg:targetId+" banned." });
      return;
    }
    if(msg.type === "adminUnban"){
      const targetId = String(msg.targetId||"").slice(0,7);
      delete DB.banned[targetId];
      saveData(DB);
      send(ws, { type:"adminActionResult", ok:true, msg:targetId+" unbanned." });
      return;
    }

    if(msg.type === "adminMaintenance"){
      if(msg.enabled){
        const minutes = Math.max(1, Math.min(1440, parseInt(msg.minutes,10)||10));
        DB.maintenance = { active:true, until: Date.now()+minutes*60000, message: String(msg.message||"").slice(0,200) };
        saveData(DB);
        scheduleMaintenanceEnd();
        broadcastAll(currentMaintenancePayload());
        send(ws, { type:"adminActionResult", ok:true, msg:"Maintenance started for "+minutes+" minutes." });
      } else {
        endMaintenance();
        send(ws, { type:"adminActionResult", ok:true, msg:"Maintenance ended." });
      }
      return;
    }

    if(msg.type === "adminSetPrice"){
      const itemId = String(msg.itemId||"").slice(0,64);
      const price = Math.max(0, Math.min(100000, parseInt(msg.price,10)||0));
      if(!itemId){ send(ws,{type:"adminActionResult",ok:false,msg:"Missing item id."}); return; }
      DB.priceOverrides[itemId] = price;
      saveData(DB);
      broadcastAll({ type:"priceOverrides", overrides: DB.priceOverrides });
      send(ws, { type:"adminActionResult", ok:true, msg:itemId+" price set to "+price });
      return;
    }
    if(msg.type === "adminClearPrice"){
      const itemId = String(msg.itemId||"").slice(0,64);
      delete DB.priceOverrides[itemId];
      saveData(DB);
      broadcastAll({ type:"priceOverrides", overrides: DB.priceOverrides });
      send(ws, { type:"adminActionResult", ok:true, msg:itemId+" price override cleared." });
      return;
    }

    if(msg.type === "adminBroadcast"){
      const text = String(msg.text||"").slice(0,240);
      if(!text) return;
      broadcastAll({ type:"announcement", text });
      send(ws, { type:"adminActionResult", ok:true, msg:"Announcement sent to "+clients.size+" online player(s)." });
      return;
    }
  });

  ws.on("close", ()=>{
    if(!myId) return;
    clients.delete(myId);
    const idx = queue.indexOf(myId);
    if(idx>=0) queue.splice(idx,1);
    const roomId = playerRoom.get(myId);
    if(roomId){
      const members = rooms.get(roomId);
      if(members){
        members.delete(myId);
        members.forEach(pid=>{ const e=clients.get(pid); if(e) send(e.ws,{type:"peerLeft", id:myId}); });
      }
      playerRoom.delete(myId);
    }
    notifyFriendsOfStatusChange(myId);
  });
});

scheduleMaintenanceEnd();
httpServer.listen(PORT, ()=>{
  console.log("ROYALE X relay server listening on port "+PORT);
});
