/**
 * ROYALE X — Multiplayer Relay Server
 * ------------------------------------------------------------------
 * A small, dependency-light Node.js WebSocket server that gives the
 * game real online features:
 *   - persistent player profiles (name + 7-digit ID)
 *   - real friend requests / friend lists with live online status
 *   - real matchmaking (queues actual connected players together)
 *   - real-time position/action sync between real players in a match
 *
 * NPCs are NOT synced over the network — each player's client still
 * simulates its own NPCs locally (same as the offline build). Only
 * real human players are relayed through this server. That keeps the
 * server simple (a relay, not a full authoritative game simulation)
 * while still making friends, matchmaking, and "seeing other real
 * players" genuinely real.
 *
 * Requirements: Node.js 18+, and the "ws" package:
 *     npm install ws
 *
 * Run:
 *     node server.js
 * By default it listens on process.env.PORT or 8080.
 * ------------------------------------------------------------------
 * HOSTING
 * This process must be deployed somewhere with a public URL before
 * the game client can use it (Claude cannot host it for you). Any
 * Node-friendly host works, e.g.:
 *   - Render.com (free web service tier)
 *   - Railway.app
 *   - Fly.io
 *   - Your own VPS
 * Once deployed you'll get a URL like wss://your-app.onrender.com —
 * paste that into SERVER_URL near the top of index.html's <script>.
 * ------------------------------------------------------------------
 */

const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const DATA_FILE = path.join(__dirname, "royalex-server-data.json");
const MATCH_SIZE_TARGET = 50;     // total match "slots" (real players + client-simulated NPCs)
const MATCHMAKING_WINDOW_MS = 15000; // matches the client's 15s "Finding Match" screen

/* ---------------- persistence (flat JSON file — simple by design) ---------------- */
function loadData(){
  try{ return JSON.parse(fs.readFileSync(DATA_FILE, "utf8")); }
  catch(e){ return { profiles:{}, friends:{} }; } // profiles: id -> {name}; friends: id -> [ids]
}
function saveData(data){
  try{ fs.writeFileSync(DATA_FILE, JSON.stringify(data)); }catch(e){ console.error("save failed", e); }
}
let DB = loadData();

/* ---------------- live state ---------------- */
const clients = new Map();     // id -> { ws, name }
const queue = [];              // ids waiting for a match
let queueTimer = null;
const rooms = new Map();       // roomId -> Set of ids
const playerRoom = new Map();  // id -> roomId

function send(ws, obj){ try{ ws.send(JSON.stringify(obj)); }catch(e){} }
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
      myId = String(msg.id||"").slice(0,7);
      const name = String(msg.name||"Player").slice(0,16);
      if(!/^\d{7}$/.test(myId)) return;
      clients.set(myId, { ws, name });
      DB.profiles[myId] = { name };
      if(!DB.friends[myId]) DB.friends[myId] = [];
      saveData(DB);
      send(ws, { type:"helloAck", id: myId });
      broadcastFriendsUpdate(myId);
      notifyFriendsOfStatusChange(myId);
      return;
    }
    if(!myId) return; // must say hello first

    if(msg.type === "addFriend"){
      const targetId = String(msg.targetId||"").slice(0,7);
      if(!/^\d{7}$/.test(targetId) || targetId===myId) return;
      if(!DB.profiles[targetId]){
        // unknown ID: nothing to add server-side (client can still show
        // "not found" — real online friends must have connected at least once)
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
      // if a lot of real players are already waiting, don't make them wait
      // out the full window unnecessarily
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

httpServer.listen(PORT, ()=>{
  console.log("ROYALE X relay server listening on port "+PORT);
});
