/**
 * ROYALE X — WebSocket & Multiplayer Backend (MongoDB-siz)
 */

require("dotenv").config();
const express = require("express");
const cors = require("cors");
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

app.get("/api/health", (req, res) => res.json({ ok: true, status: "online" }));

/* HTTP və WebSocket Serveri */
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  console.log("🎮 Yeni oyunçu qoşuldu");

  ws.on("message", (message) => {
    wss.clients.forEach((client) => {
      if (client !== ws && client.readyState === 1) {
        client.send(message.toString());
      }
    });
  });

  ws.on("close", () => console.log("❌ Oyunçu ayrıldı"));
});

server.listen(PORT, () => console.log(`🚀 Server ${PORT} portunda MongoDB-siz aktivdir`));

