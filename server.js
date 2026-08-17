/**
 * ROYALE X — Hesab / Qeydiyyat / Bulud Save & Multiplayer Backend-i
 */

require("dotenv").config();
const express = require("express");
const mongoose = require("mongoose");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const cors = require("cors");
const rateLimit = require("express-rate-limit");
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const MONGODB_URI = process.env.MONGODB_URI;
const JWT_SECRET = process.env.JWT_SECRET;

if (!MONGODB_URI) {
  console.error("XƏTA: .env faylında MONGODB_URI tapılmadı.");
  process.exit(1);
}
if (!JWT_SECRET) {
  console.error("XƏTA: .env faylında JWT_SECRET tapılmadı.");
  process.exit(1);
}

/* Bulud bazasına qoşulma */
mongoose.connect(MONGODB_URI)
  .then(() => console.log("✅ MongoDB Atlas-a qoşuldu"))
  .catch(err => {
    console.error("❌ MongoDB bağlantı xətası:", err.message);
    process.exit(1);
  });

const userSchema = new mongoose.Schema({
  username: { type: String, required: true, unique: true, trim: true, minlength: 3, maxlength: 24 },
  passwordHash: { type: String, required: true },
  save: { type: mongoose.Schema.Types.Mixed, default: {} },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});
const User = mongoose.model("User", userSchema);

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

const authLimiter = rateLimit({ windowMs: 15 * 60 * 1000, max: 30, standardHeaders: true, legacyHeaders: false });
const saveLimiter = rateLimit({ windowMs: 60 * 1000, max: 60, standardHeaders: true, legacyHeaders: false });

function signToken(user) {
  return jwt.sign({ uid: user._id.toString(), username: user.username }, JWT_SECRET, { expiresIn: "365d" });
}

function requireAuth(req, res, next) {
  const header = req.headers.authorization || "";
  const token = header.startsWith("Bearer ") ? header.slice(7) : null;
  if (!token) return res.status(401).json({ error: "Token yoxdur." });
  try {
    req.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch (e) {
    return res.status(401).json({ error: "Token etibarsızdır və ya vaxtı bitib." });
  }
}

function validUsername(u) {
  return typeof u === "string" && /^[a-zA-Z0-9_ƏəÖöÜüĞğŞşÇçİı]{3,24}$/.test(u.trim());
}

/* API Marshrutları */
app.post("/api/register", authLimiter, async (req, res) => {
  try {
    const username = (req.body.username || "").trim();
    const password = req.body.password || "";
    if (!validUsername(username)) return res.status(400).json({ error: "Hesab adı 3-24 simvol olmalıdır." });
    if (password.length < 4) return res.status(400).json({ error: "Şifrə ən az 4 simvol olmalıdır." });
    
    const existing = await User.findOne({ username: new RegExp(`^${username}$`, "i") });
    if (existing) return res.status(409).json({ error: "Bu hesab adı artıq mövcuddur." });
    
    const passwordHash = await bcrypt.hash(password, 10);
    const user = await User.create({ username, passwordHash, save: req.body.initialSave || {} });
    const token = signToken(user);
    return res.json({ token, username: user.username, save: user.save });
  } catch (e) {
    return res.status(500).json({ error: "Server xətası." });
  }
});

app.post("/api/login", authLimiter, async (req, res) => {
  try {
    const username = (req.body.username || "").trim();
    const password = req.body.password || "";
    const user = await User.findOne({ username: new RegExp(`^${username}$`, "i") });
    if (!user) return res.status(404).json({ error: "Bu adda hesab tapılmadı." });
    const ok = await bcrypt.compare(password, user.passwordHash);
    if (!ok) return res.status(401).json({ error: "Şifrə yanlışdır." });
    const token = signToken(user);
    return res.json({ token, username: user.username, save: user.save });
  } catch (e) {
    return res.status(500).json({ error: "Server xətası." });
  }
});

app.get("/api/save", requireAuth, async (req, res) => {
  const user = await User.findById(req.user.uid);
  if (!user) return res.status(404).json({ error: "Hesab tapılmadı." });
  return res.json({ save: user.save });
});

app.put("/api/save", saveLimiter, requireAuth, async (req, res) => {
  if (typeof req.body.save !== "object" || req.body.save === null) {
    return res.status(400).json({ error: "save obyekti tələb olunur." });
  }
  await User.findByIdAndUpdate(req.user.uid, { save: req.body.save, updatedAt: new Date() });
  return res.json({ ok: true });
});

app.get("/api/health", (req, res) => res.json({ ok: true }));

/* HTTP və WebSocket Serverinin Birləşdirilməsi */
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  console.log("🎮 Yeni oyunçu qoşuldu");

  ws.on("message", (message) => {
    // Gələn paketləri bütün digər qoşulmuş oyunçulara yayımlayır (Broadcast)
    wss.clients.forEach((client) => {
      if (client !== ws && client.readyState === 1) {
        client.send(message.toString());
      }
    });
  });

  ws.on("close", () => console.log("❌ Oyunçu ayrıldı"));
});

server.listen(PORT, () => console.log(`🚀 Server və WebSocket ${PORT} portunda aktivdir`));
