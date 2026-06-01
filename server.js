/**
 * M6 Watch Party — Serveur de synchronisation
 * --------------------------------------------
 * Relaie en temps réel, entre les participants d'une même salle :
 *   - les événements de lecture (play / pause / seek)
 *   - les messages de chat
 *   - la présence (qui rejoint / quitte)
 *
 * Aucun flux vidéo ne transite par ce serveur : chaque participant lit M6+
 * de son côté, le serveur ne fait que synchroniser les commandes.
 *
 * Lancement :  npm install && npm start
 * Variable d'env PORT (défaut 8080).
 */

import { WebSocketServer } from "ws";
import { createServer } from "http";
import { randomUUID } from "crypto";

const PORT = process.env.PORT || 8080;

// Sert une petite page d'état + healthcheck (utile pour l'hébergement cloud)
const httpServer = createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", rooms: rooms.size }));
    return;
  }
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(
    `<!doctype html><meta charset="utf-8"><title>M6 Watch Party</title>` +
      `<body style="font-family:sans-serif;max-width:560px;margin:60px auto;padding:0 20px;color:#222">` +
      `<h1>🎬 M6 Watch Party — serveur actif</h1>` +
      `<p>Ce serveur relaie la synchronisation et le chat de l'extension.</p>` +
      `<p>Salles ouvertes : <b>${rooms.size}</b></p>` +
      `<p>Renseigne l'URL <code>${publicWsHint(req)}</code> dans le popup de l'extension.</p>` +
      `</body>`
  );
});

function publicWsHint(req) {
  const host = req.headers.host || `localhost:${PORT}`;
  const proto = req.headers["x-forwarded-proto"] === "https" ? "wss" : "ws";
  return `${proto}://${host}`;
}

const wss = new WebSocketServer({ server: httpServer });

/**
 * rooms: Map<roomId, Map<clientId, { ws, pseudo, color }>>
 */
const rooms = new Map();

function send(ws, payload) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function broadcast(roomId, payload, exceptId = null) {
  const room = rooms.get(roomId);
  if (!room) return;
  for (const [id, member] of room) {
    if (id === exceptId) continue;
    send(member.ws, payload);
  }
}

function participantList(roomId) {
  const room = rooms.get(roomId);
  if (!room) return [];
  return [...room.entries()].map(([id, m]) => ({
    id,
    pseudo: m.pseudo,
    color: m.color,
  }));
}

function leaveRoom(ws) {
  const { roomId, clientId, pseudo } = ws.meta || {};
  if (!roomId || !rooms.has(roomId)) return;
  const room = rooms.get(roomId);
  room.delete(clientId);
  if (room.size === 0) {
    rooms.delete(roomId);
  } else {
    broadcast(roomId, { type: "presence", action: "leave", clientId, pseudo });
    broadcast(roomId, { type: "participants", participants: participantList(roomId) });
  }
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    switch (msg.type) {
      case "join": {
        const roomId = String(msg.room || "").trim().toUpperCase();
        const pseudo = String(msg.pseudo || "Invité").slice(0, 32);
        const color = msg.color || randomColor();
        if (!roomId) {
          send(ws, { type: "error", message: "Code de salle manquant." });
          return;
        }
        // Quitte une éventuelle salle précédente
        leaveRoom(ws);

        const clientId = randomUUID();
        ws.meta = { roomId, clientId, pseudo };

        if (!rooms.has(roomId)) rooms.set(roomId, new Map());
        const room = rooms.get(roomId);
        room.set(clientId, { ws, pseudo, color });

        // Confirme au nouvel arrivant
        send(ws, {
          type: "joined",
          clientId,
          room: roomId,
          participants: participantList(roomId),
        });

        // Prévient les autres
        broadcast(roomId, { type: "presence", action: "join", clientId, pseudo, color }, clientId);
        broadcast(roomId, { type: "participants", participants: participantList(roomId) });

        // Demande aux autres l'état de lecture courant pour aligner le nouvel arrivant
        broadcast(roomId, { type: "request-state", forClientId: clientId }, clientId);
        break;
      }

      case "sync": {
        // Relaie play / pause / seek vers les autres membres de la salle
        const { roomId, clientId, pseudo } = ws.meta || {};
        if (!roomId) return;
        broadcast(
          roomId,
          {
            type: "sync",
            action: msg.action, // 'play' | 'pause' | 'seek'
            currentTime: msg.currentTime,
            paused: msg.paused,
            by: pseudo,
            fromClientId: clientId,
            ts: Date.now(),
          },
          clientId
        );
        break;
      }

      case "state": {
        // Réponse à request-state : envoyée uniquement au client cible
        const { roomId } = ws.meta || {};
        if (!roomId) return;
        const room = rooms.get(roomId);
        const target = room && room.get(msg.forClientId);
        if (target) {
          send(target.ws, {
            type: "sync",
            action: "seek",
            currentTime: msg.currentTime,
            paused: msg.paused,
            by: "(synchronisation)",
            ts: Date.now(),
          });
        }
        break;
      }

      case "chat": {
        const { roomId, clientId, pseudo } = ws.meta || {};
        if (!roomId) return;
        const text = String(msg.text || "").slice(0, 1000);
        if (!text.trim()) return;
        broadcast(roomId, {
          type: "chat",
          text,
          pseudo,
          fromClientId: clientId,
          ts: Date.now(),
        });
        break;
      }

      case "signal": {
        // Signalisation WebRTC (micro en direct, type Zoom).
        // Relai ciblé : on transmet uniquement au destinataire `to`.
        const { roomId, clientId } = ws.meta || {};
        if (!roomId) return;
        const room = rooms.get(roomId);
        const target = room && room.get(msg.to);
        if (target) {
          send(target.ws, { type: "signal", from: clientId, kind: msg.kind, payload: msg.payload });
        }
        break;
      }

      default:
        break;
    }
  });

  ws.on("close", () => leaveRoom(ws));
  ws.on("error", () => leaveRoom(ws));
});

// Ping périodique pour nettoyer les connexions mortes
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on("close", () => clearInterval(interval));

function randomColor() {
  const colors = ["#e84545", "#2d9cdb", "#27ae60", "#f2994a", "#9b51e0", "#eb5757", "#00b8a9", "#f78fb3"];
  return colors[Math.floor(Math.random() * colors.length)];
}

httpServer.listen(PORT, () => {
  console.log(`M6 Watch Party server en écoute sur le port ${PORT}`);
  console.log(`WebSocket : ws://localhost:${PORT}`);
});
