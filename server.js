/**
 * ROYALE X — Hesab / Qeydiyyat / Bulud Save Backend-i
 * ----------------------------------------------------
 * Bu server 4 iş görür:
 *   1) POST /api/register  — yeni hesab yaradır (şifrə bcrypt ilə hash-lənir)
 *   2) POST /api/login     — mövcud hesabla giriş, JWT token qaytarır
 *   3) GET  /api/save      — token sahibinin oyun məlumatını (SAVE) bulud
 *                             bazasından qaytarır
 *   4) PUT  /api/save      — token sahibinin oyun məlumatını bulud bazasında
 *                             yeniləyir/saxlayır
 *
 * Hesab MongoDB Atlas-da (bulud) saxlanılır — oyun telefondan silinib
 * yenidən yüklənsə belə, istifadəçi eyni ad+şifrə ilə giriş edəndə
 * bütün coin/skin/rank məlumatı geri gəlir, çünki heç vaxt telefonda
 * deyil, bulud bazasında saxlanılıb.
 */

require("dotenv").config();
const express = require("express");
const mongoose = require("mongoose");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const cors = require("cors");
const rateLimit = require("express-rate-limit");

const PORT = process.env.PORT || 3000;
const MONGODB_URI = process.env.MONGODB_URI;
const JWT_SECRET = process.env.JWT_SECRET;

if (!MONGODB_URI) {
  console.error("XƏTA: .env faylında MONGODB_URI tapılmadı. .env.example-a bax.");
  process.exit(1);
}
if (!JWT_SECRET) {
  console.error("XƏTA: .env faylında JWT_SECRET tapılmadı. .env.example-a bax.");
  process.exit(1);
}

/* -------------------------------------------------------------------------
   Bulud bazasına qoşulma (MongoDB Atlas)
   ------------------------------------------------------------------------- */
mongoose.connect(MONGODB_URI)
  .then(() => console.log("✅ MongoDB Atlas-a qoşuldu"))
  .catch(err => {
    console.error("❌ MongoDB bağlantı xətası:", err.message);
    process.exit(1);
  });

/* Hər istifadəçi üçün bir sənəd: username unikal, şifrə yalnız hash olaraq,
   save isə tam oyun məlumatını (coin, skinlər, rank və s.) saxlayan sərbəst
   formatlı JSON-dur — client tərəf hansı sahələri göndərirsə elə saxlanılır,
   backend-in oyun strukturunu bilməsinə ehtiyac yoxdur. */
const userSchema = new mongoose.Schema({
  username: { type: String, required: true, unique: true, trim: true, minlength: 3, maxlength: 24 },
  passwordHash: { type: String, required: true },
  save: { type: mongoose.Schema.Types.Mixed, default: {} },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});
const User = mongoose.model("User", userSchema);

const app = express();
app.use(cors());              // statik HTML fayl file:// və ya istənilən domendən çağıra bilsin deyə hamısına açıq
app.use(express.json({ limit: "1mb" }));

/* Qaba-güc (brute-force) hücumlarının qarşısını almaq üçün sadə limitlər */
const authLimiter = rateLimit({ windowMs: 15 * 60 * 1000, max: 30, standardHeaders: true, legacyHeaders: false });
const saveLimiter = rateLimit({ windowMs: 60 * 1000, max: 60, standardHeaders: true, legacyHeaders: false });

function signToken(user) {
  return jwt.sign({ uid: user._id.toString(), username: user.username }, JWT_SECRET, { expiresIn: "365d" });
}

/* Authorization: Bearer <token> başlığını yoxlayan middleware */
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

/* ---------------------------------------------------------------------
   POST /api/register  { username, password }
   --------------------------------------------------------------------- */
app.post("/api/register", authLimiter, async (req, res) => {
  try {
    const username = (req.body.username || "").trim();
    const password = req.body.password || "";
    if (!validUsername(username)) {
      return res.status(400).json({ error: "Hesab adı 3-24 simvol olmalıdır." });
    }
    if (password.length < 4) {
      return res.status(400).json({ error: "Şifrə ən az 4 simvol olmalıdır." });
    }
    const existing = await User.findOne({ username: new RegExp(`^${username}$`, "i") });
    if (existing) {
      return res.status(409).json({ error: "Bu hesab adı artıq mövcuddur." });
    }
    const passwordHash = await bcrypt.hash(password, 10);
    const user = await User.create({ username, passwordHash, save: req.body.initialSave || {} });
    const token = signToken(user);
    return res.json({ token, username: user.username, save: user.save });
  } catch (e) {
    console.error("register error:", e);
    return res.status(500).json({ error: "Server xətası. Bir az sonra yenidən cəhd et." });
  }
});

/* ---------------------------------------------------------------------
   POST /api/login  { username, password }
   --------------------------------------------------------------------- */
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
    console.error("login error:", e);
    return res.status(500).json({ error: "Server xətası. Bir az sonra yenidən cəhd et." });
  }
});

/* ---------------------------------------------------------------------
   GET /api/save  — cari istifadəçinin bulud save-ini qaytarır
   --------------------------------------------------------------------- */
app.get("/api/save", requireAuth, async (req, res) => {
  const user = await User.findById(req.user.uid);
  if (!user) return res.status(404).json({ error: "Hesab tapılmadı." });
  return res.json({ save: user.save });
});

/* ---------------------------------------------------------------------
   PUT /api/save  { save }  — cari istifadəçinin save-ini yeniləyir
   --------------------------------------------------------------------- */
app.put("/api/save", saveLimiter, requireAuth, async (req, res) => {
  if (typeof req.body.save !== "object" || req.body.save === null) {
    return res.status(400).json({ error: "save obyekti tələb olunur." });
  }
  await User.findByIdAndUpdate(req.user.uid, { save: req.body.save, updatedAt: new Date() });
  return res.json({ ok: true });
});

app.get("/api/health", (req, res) => res.json({ ok: true }));

app.listen(PORT, () => console.log(`🚀 ROYALE X hesab backend-i ${PORT} portunda işləyir`));

