/**
 * ROYALE X — WebSocket & Multiplayer Backend (MongoDB-siz)
 *
 * Bu fayl köhnə "hər mesajı hamıya təkrar yolla" relyeyinin yerinə keçir.
 * Köhnə versiya heç bir oyunçu ID -> socket uyğunluğu saxlamırdı, ona görə
 * server client-in gözlədiyi mesajları (friendRequestIncoming, adminAuthResult,
 * matchStart, peerState və s.) HEÇ VAXT göndərmirdi — düymələr basılırdı,
 * amma cavab heç kimə çatmırdı.
 *
 * Bu versiya:
 *   - Hər WebSocket bağlantısını 7 rəqəmli Player ID-yə bağlayır ("hello")
 *   - Arkadaşlıq / komanda dəvəti / VS çağırışını DOĞRU adama yönləndirir
 *   - Admin şifrəsini serverdə yoxlayır (adminAuthResult ilə cavab verir)
 *   - Matçmeykinq: sıraya düşənləri "otaq"a yığır, matchStart göndərir
 *   - Otaq daxilində mövqe/atəş/zərbə/ölüm mesajlarını YALNIZ eyni otaqdakı
 *     oyunçulara ötürür (əvvəlki kimi hamıya yox)
 *   - Qlobal chat, leaderboard, admin əməliyyatları (coin, ban, maintenance,
 *     qiymət override, elan) üçün əsas dəstək
 *
 * Yaddaş yalnız RAM-dadır (MongoDB-siz dizayna uyğun) — server yenidən
 * başladılanda (Render-də redeploy) arkadaşlıq/leaderboard sıfırlanır.
 * Client bunu qismən "syncFriends" ilə bərpa edir.
 */

require("dotenv").config();
const express = require("express");
const cors = require("cors");
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || "1414"; // client-in yerli fallback şifrəsi ilə eyni

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));
app.get("/api/health", (req, res) => res.json({ ok: true, status: "online", players: players.size }));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

/* =========================================================================
   YADDAŞ (RAM) — MongoDB-siz dizayn üçün sadə in-memory saxlanc
   ========================================================================= */
const players = new Map();          // id -> { ws, name, roomId, isAdmin }
const friends = new Map();          // id -> Set<friendId>
const pendingFriendReq = new Map(); // requestId -> { fromId, toId }
const pendingSquadInv = new Map();  // requestId -> { fromId, toId }
const pendingVsReq = new Map();     // requestId -> { fromId, toId }
const rooms = new Map();            // roomId -> Set<id>
const queue = [];                   // [{ id, squad: [ids] }]
const banned = new Set();
const leaderboard = new Map();      // id -> { id, name, rankPoints, wins, kills }
const chatHistory = [];             // last 50 { id, name, text, ts }
let maintenance = { active: false, until: 0, message: "" };
let priceOverrides = {};
let eventWarningText = "";
let reqCounter = 1;
const newReqId = () => "r" + (reqCounter++) + "_" + Date.now();
const newRoomId = () => "room_" + Date.now() + "_" + Math.floor(Math.random() * 1e6);

function send(id, obj) {
  const p = players.get(id);
  if (p && p.ws.readyState === 1) { try { p.ws.send(JSON.stringify(obj)); } catch (e) {} }
}
function broadcastAll(obj, excludeId) {
  players.forEach((p, id) => { if (id !== excludeId) send(id, obj); });
}
function broadcastRoom(roomId, obj, excludeId) {
  const set = rooms.get(roomId);
  if (!set) return;
  set.forEach(id => { if (id !== excludeId) send(id, obj); });
}
function friendsListFor(id) {
  const set = friends.get(id) || new Set();
  return Array.from(set).map(fid => ({
    id: fid,
    name: (players.get(fid) || {}).name || "Player",
    status: players.has(fid) ? "online" : "offline"
  }));
}
function pushFriendsUpdate(id) {
  if (players.has(id)) send(id, { type: "friendsUpdate", friends: friendsListFor(id) });
}
function addMutualFriend(a, b) {
  if (!friends.has(a)) friends.set(a, new Set());
  if (!friends.has(b)) friends.set(b, new Set());
  friends.get(a).add(b);
  friends.get(b).add(a);
}
function topLeaderboard() {
  return Array.from(leaderboard.values()).sort((a, b) => b.rankPoints - a.rankPoints).slice(0, 50);
}

/* =========================================================================
   MATÇMEYKİNQ — hər 14 saniyədə sıranı bir otaqda birləşdirir
   ========================================================================= */
setInterval(() => {
  if (queue.length === 0) return;
  const roomId = newRoomId();
  const realIds = new Set();
  queue.forEach(entry => {
    if (players.has(entry.id)) realIds.add(entry.id);
    (entry.squad || []).forEach(sid => { if (players.has(sid)) realIds.add(sid); });
  });
  if (realIds.size === 0) { queue.length = 0; return; }
  rooms.set(roomId, realIds);
  const playerList = Array.from(realIds).map(id => ({ id, name: (players.get(id) || {}).name || "Player" }));
  const npcCount = Math.max(0, 100 - playerList.length);
  realIds.forEach(id => {
    const p = players.get(id);
    if (p) p.roomId = roomId;
    send(id, { type: "matchStart", roomId, players: playerList, npcCount });
  });
  queue.length = 0;
}, 14000);

/* =========================================================================
   BAĞLANTI
   ========================================================================= */
wss.on("connection", (ws) => {
  let myId = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!msg || !msg.type) return;

    /* ---------------- İDENTİFİKASİYA ---------------- */
    if (msg.type === "hello") {
      myId = String(msg.id || "");
      if (!myId) return;
      if (banned.has(myId)) { send(myId, { type: "banned", reason: "" }); try { ws.close(); } catch (e) {} return; }
      players.set(myId, { ws, name: msg.name || "Player", roomId: null, isAdmin: false });
      pushFriendsUpdate(myId);
      send(myId, { type: "chatHistory", messages: chatHistory });
      send(myId, { type: "leaderboardUpdate", board: topLeaderboard() });
      send(myId, { type: "maintenanceUpdate", active: maintenance.active, until: maintenance.until, message: maintenance.message });
      send(myId, { type: "priceOverrides", overrides: priceOverrides });
      if (eventWarningText) send(myId, { type: "eventWarning", text: eventWarningText });
      return;
    }
    if (!myId || !players.has(myId)) return; // hello gəlməyib, mesajı yoxsay
    const me = players.get(myId);

    if (msg.type === "syncFriends") {
      (msg.friends || []).forEach(fid => { if (players.has(fid) || fid) addMutualFriend(myId, fid); });
      pushFriendsUpdate(myId);
      return;
    }

    /* ---------------- ARKADAŞLIQ ---------------- */
    if (msg.type === "addFriend") {
      const targetId = String(msg.targetId || "");
      if (!players.has(targetId)) { send(myId, { type: "addFriendResult", ok: false, msg: "Oyunçu tapılmadı və ya oflayndır." }); return; }
      const reqId = newReqId();
      pendingFriendReq.set(reqId, { fromId: myId, toId: targetId });
      send(targetId, { type: "friendRequestIncoming", requestId: reqId, fromId: myId, fromName: me.name });
      return;
    }
    if (msg.type === "friendRequestResponse") {
      const pend = pendingFriendReq.get(msg.requestId);
      pendingFriendReq.delete(msg.requestId);
      if (!pend) return;
      if (msg.accept) { addMutualFriend(pend.fromId, pend.toId); pushFriendsUpdate(pend.fromId); pushFriendsUpdate(pend.toId); }
      return;
    }

    /* ---------------- KOMANDA (SQUAD) DƏVƏTİ ---------------- */
    if (msg.type === "squadInvite") {
      const targetId = String(msg.targetId || "");
      if (!players.has(targetId)) { send(myId, { type: "squadInviteResult", accept: false, byId: targetId, byName: "?" }); return; }
      const reqId = newReqId();
      pendingSquadInv.set(reqId, { fromId: myId, toId: targetId });
      send(targetId, { type: "squadInviteIncoming", requestId: reqId, fromId: myId, fromName: me.name });
      return;
    }
    if (msg.type === "squadInviteResponse") {
      const pend = pendingSquadInv.get(msg.requestId);
      pendingSquadInv.delete(msg.requestId);
      if (!pend) return;
      const accepter = players.get(pend.toId);
      send(pend.fromId, { type: "squadInviteResult", accept: !!msg.accept, byId: pend.toId, byName: accepter ? accepter.name : "Player" });
      return;
    }

    /* ---------------- VS ARENA ÇAĞIRIŞI ---------------- */
    if (msg.type === "vsRequest") {
      const targetId = String(msg.targetId || "");
      if (!players.has(targetId)) { send(myId, { type: "vsRequestResult", accept: false, byId: targetId, byName: "?" }); return; }
      const reqId = newReqId();
      pendingVsReq.set(reqId, { fromId: myId, toId: targetId });
      send(targetId, { type: "vsRequestIncoming", requestId: reqId, fromId: myId, fromName: me.name });
      return;
    }
    if (msg.type === "vsRequestResponse") {
      const pend = pendingVsReq.get(msg.requestId);
      pendingVsReq.delete(msg.requestId);
      if (!pend) return;
      const accepter = players.get(pend.toId);
      send(pend.fromId, { type: "vsRequestResult", accept: !!msg.accept, byId: pend.toId, byName: accepter ? accepter.name : "Player" });
      if (msg.accept && players.has(pend.fromId) && players.has(pend.toId)) {
        const roomId = newRoomId();
        rooms.set(roomId, new Set([pend.fromId, pend.toId]));
        players.get(pend.fromId).roomId = roomId;
        players.get(pend.toId).roomId = roomId;
        send(pend.fromId, { type: "vsMatchStart", roomId, opponent: { id: pend.toId, name: accepter.name }, youAreA: true });
        send(pend.toId, { type: "vsMatchStart", roomId, opponent: { id: pend.fromId, name: me.name }, youAreA: false });
      }
      return;
    }

    /* ---------------- PROFİL BAXIŞI ---------------- */
    if (msg.type === "profileRequest") {
      const targetId = String(msg.targetId || "");
      if (!players.has(targetId)) { send(myId, { type: "profileResult", found: false }); return; }
      send(targetId, { type: "profileRequestIncoming", requesterId: myId });
      return;
    }
    if (msg.type === "profileResponse") {
      send(msg.requesterId, { type: "profileResult", found: true, id: msg.id, name: msg.name, rankTier: msg.rankTier, frame: msg.frame, icon: msg.icon });
      return;
    }

    /* ---------------- MATÇMEYKİNQ ---------------- */
    if (msg.type === "queue") {
      if (!queue.some(e => e.id === myId)) queue.push({ id: myId, squad: msg.squad || [] });
      return;
    }
    if (msg.type === "leaveQueue") {
      const idx = queue.findIndex(e => e.id === myId);
      if (idx >= 0) queue.splice(idx, 1);
      return;
    }

    /* ---------------- OTAQ İÇİ (mövqe/atəş/zərbə/ölüm) ---------------- */
    if (msg.type === "state") {
      if (me.roomId) broadcastRoom(me.roomId, { type: "peerState", id: myId, x: msg.x, y: msg.y, angle: msg.angle, hp: msg.hp, weapon: msg.weapon, skin: msg.skin }, myId);
      return;
    }
    if (msg.type === "shoot") {
      if (me.roomId) broadcastRoom(me.roomId, { type: "peerShoot", id: myId, x: msg.x, y: msg.y, angle: msg.angle }, myId);
      return;
    }
    if (msg.type === "hit") {
      send(msg.targetId, { type: "peerHit", id: myId, dmg: msg.dmg });
      return;
    }
    if (msg.type === "downed") {
      if (me.roomId) broadcastRoom(me.roomId, { type: "peerDowned", id: myId, killerId: msg.killerId || null }, myId);
      return;
    }
    if (msg.type === "skinKill") {
      if (me.roomId) broadcastRoom(me.roomId, { type: "skinKill", skinId: msg.skinId, killer: msg.killer, x: msg.x, y: msg.y }, myId);
      return;
    }
    if (msg.type === "leaveMatch") {
      if (me.roomId) {
        broadcastRoom(me.roomId, { type: "peerLeft", id: myId }, myId);
        const set = rooms.get(me.roomId);
        if (set) { set.delete(myId); if (set.size === 0) rooms.delete(me.roomId); }
      }
      me.roomId = null;
      return;
    }

    /* ---------------- QLOBAL CHAT ---------------- */
    if (msg.type === "chatMessage") {
      const entry = { id: myId, name: me.name, text: String(msg.text || "").slice(0, 200), ts: Date.now() };
      chatHistory.push(entry);
      if (chatHistory.length > 50) chatHistory.shift();
      broadcastAll({ type: "chatMessage", message: entry });
      return;
    }

    /* ---------------- LEADERBOARD ---------------- */
    if (msg.type === "leaderboardSubmit") {
      leaderboard.set(myId, { id: myId, name: me.name, rankPoints: msg.rankPoints || 0, wins: msg.wins || 0, kills: msg.kills || 0 });
      broadcastAll({ type: "leaderboardUpdate", board: topLeaderboard() });
      return;
    }
    if (msg.type === "leaderboardRequest") {
      send(myId, { type: "leaderboardUpdate", board: topLeaderboard() });
      return;
    }

    /* ---------------- ADMİN ---------------- */
    if (msg.type === "adminAuth") {
      me.isAdmin = (msg.key === ADMIN_PASSWORD);
      send(myId, { type: "adminAuthResult", ok: me.isAdmin });
      return;
    }
    if (String(msg.type).indexOf("admin") === 0 && msg.type !== "adminAuth") {
      if (!me.isAdmin) { send(myId, { type: "adminActionResult", ok: false, msg: "Admin deyilsiniz." }); return; }
      if (msg.type === "adminGiveCoins") {
        if (players.has(msg.targetId)) { send(msg.targetId, { type: "coinGrant", amount: msg.amount || 0 }); send(myId, { type: "adminActionResult", ok: true, msg: "Coin göndərildi." }); }
        else send(myId, { type: "adminActionResult", ok: false, msg: "Hədəf oyunçu oflayndır." });
        return;
      }
      if (msg.type === "adminBan") {
        banned.add(msg.targetId);
        if (players.has(msg.targetId)) { send(msg.targetId, { type: "banned", reason: "" }); try { players.get(msg.targetId).ws.close(); } catch (e) {} }
        send(myId, { type: "adminActionResult", ok: true, msg: "Ban olundu." });
        return;
      }
      if (msg.type === "adminUnban") { banned.delete(msg.targetId); send(myId, { type: "adminActionResult", ok: true, msg: "Ban götürüldü." }); return; }
      if (msg.type === "adminMaintenance") {
        maintenance = { active: !!msg.enabled, until: msg.enabled ? Date.now() + (msg.minutes || 15) * 60000 : 0, message: msg.message || "" };
        broadcastAll({ type: "maintenanceUpdate", active: maintenance.active, until: maintenance.until, message: maintenance.message });
        send(myId, { type: "adminActionResult", ok: true, msg: "Maintenance yeniləndi." });
        return;
      }
      if (msg.type === "adminSetPrice") {
        priceOverrides[msg.itemId] = { price: msg.price };
        broadcastAll({ type: "priceOverrides", overrides: priceOverrides });
        send(myId, { type: "adminActionResult", ok: true, msg: "Qiymət yeniləndi." });
        return;
      }
      if (msg.type === "adminClearPrice") {
        delete priceOverrides[msg.itemId];
        broadcastAll({ type: "priceOverrides", overrides: priceOverrides });
        send(myId, { type: "adminActionResult", ok: true, msg: "Qiymət sıfırlandı." });
        return;
      }
      if (msg.type === "adminBroadcast") { broadcastAll({ type: "announcement", text: msg.text || "" }); send(myId, { type: "adminActionResult", ok: true, msg: "Elan göndərildi." }); return; }
      if (msg.type === "adminEventWarning") { eventWarningText = msg.text || ""; broadcastAll({ type: "eventWarning", text: eventWarningText }); send(myId, { type: "adminActionResult", ok: true, msg: "Yeniləndi." }); return; }
      if (msg.type === "adminZombieForce") {
        broadcastAll({ type: "zombieEventUpdate", forceNext: msg.action === "start" }, myId);
        send(myId, { type: "adminActionResult", ok: true, msg: "Zombi hadisəsi bütün oyunçulara sinxronlaşdırıldı." });
        return;
      }
      if (msg.type === "adminWeatherTrigger") {
        broadcastAll({ type: "weatherUpdate", weatherType: msg.weatherType, seconds: msg.seconds }, myId);
        send(myId, { type: "adminActionResult", ok: true, msg: "Hava effekti bütün oyunçulara göndərildi." });
        return;
      }
      if (msg.type === "adminMarketOverride") {
        const o = Object.assign({}, priceOverrides[msg.mid], { price: msg.price, discountPrice: msg.discountPrice });
        priceOverrides[msg.mid] = o;
        broadcastAll({ type: "marketOverrideUpdate", mid: msg.mid, override: o }, myId);
        send(myId, { type: "adminActionResult", ok: true, msg: "Qiymət bütün oyunçulara sinxronlaşdırıldı." });
        return;
      }
      if (msg.type === "adminMarketSale") {
        const o = Object.assign({}, priceOverrides[msg.mid], { saleEndsAt: Date.now() + (msg.minutes || 0) * 60000 });
        priceOverrides[msg.mid] = o;
        broadcastAll({ type: "marketOverrideUpdate", mid: msg.mid, override: o }, myId);
        send(myId, { type: "adminActionResult", ok: true, msg: "Kampaniya bütün oyunçulara sinxronlaşdırıldı." });
        return;
      }
      if (msg.type === "adminMarketToggle") {
        const o = Object.assign({}, priceOverrides[msg.mid], { enabled: msg.enabled });
        priceOverrides[msg.mid] = o;
        broadcastAll({ type: "marketOverrideUpdate", mid: msg.mid, override: o }, myId);
        send(myId, { type: "adminActionResult", ok: true, msg: "Market vəziyyəti sinxronlaşdırıldı." });
        return;
      }
      if (msg.type === "adminChestSkinAdd") {
        broadcastAll({ type: "chestSkinUpdate", action: "add", skinId: msg.skinId }, myId);
        send(myId, { type: "adminActionResult", ok: true, msg: "Skin bütün oyunçuların sandığına əlavə olundu." });
        return;
      }
      if (msg.type === "adminChestSkinRemove") {
        broadcastAll({ type: "chestSkinUpdate", action: "remove", skinId: msg.skinId }, myId);
        send(myId, { type: "adminActionResult", ok: true, msg: "Skin sandıqdan bütün oyunçular üçün silindi." });
        return;
      }
      if (msg.type === "adminNewSeason") {
        broadcastAll({ type: "seasonUpdate", season: msg.season, seasonFrames: msg.seasonFrames }, myId);
        send(myId, { type: "adminActionResult", ok: true, msg: "Yeni sezon bütün oyunçulara elan olundu." });
        return;
      }
      if (msg.type === "adminEndSeason") {
        broadcastAll({ type: "announcement", text: "🏆 Sezon " + msg.season + " bitdi! Yeni sezon tezliklə." }, myId);
        send(myId, { type: "adminActionResult", ok: true, msg: "Sezon sonu bütün oyunçulara elan olundu." });
        return;
      }
      send(myId, { type: "adminActionResult", ok: true, msg: "Qeydə alındı." });
      return;
    }
  });

  ws.on("close", () => {
    if (!myId) return;
    if (players.has(myId)) {
      const me = players.get(myId);
      if (me.roomId) {
        broadcastRoom(me.roomId, { type: "peerLeft", id: myId }, myId);
        const set = rooms.get(me.roomId);
        if (set) { set.delete(myId); if (set.size === 0) rooms.delete(me.roomId); }
      }
    }
    players.delete(myId);
    const qIdx = queue.findIndex(e => e.id === myId);
    if (qIdx >= 0) queue.splice(qIdx, 1);
    console.log("❌ Oyunçu ayrıldı: " + myId);
  });
});

server.listen(PORT, () => console.log(`🚀 Server ${PORT} portunda MongoDB-siz aktivdir`));
