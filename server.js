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

// Politique de confidentialité servie sur /privacy (URL exigée par le Chrome Web Store).
const PRIVACY_HTML = `<!doctype html><html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CinéSync — Politique de confidentialité</title>
<style>body{font-family:-apple-system,"Segoe UI",Roboto,sans-serif;max-width:720px;margin:48px auto;padding:0 20px;color:#1d1d1f;line-height:1.6}h1{font-size:26px}h2{font-size:18px;margin-top:28px}.meta{color:#6b6b70;font-size:13px}code{background:#f2f2f4;padding:1px 5px;border-radius:4px}</style></head><body>
<h1>Politique de confidentialité — CinéSync (Watch Party)</h1>
<p class="meta">Dernière mise à jour : juin 2026</p>
<p>CinéSync est une extension de navigateur qui permet de regarder des programmes en streaming de façon synchronisée avec d'autres personnes, avec un chat texte et un micro en direct.</p>
<h2>Données traitées</h2>
<ul>
<li><strong>Pseudo</strong> que vous saisissez, pour vous identifier dans la salle.</li>
<li><strong>Code de salle</strong> et <strong>réglages</strong>, stockés localement via l'API <code>storage</code> de Chrome.</li>
<li><strong>Messages de chat</strong> transmis aux autres participants.</li>
<li><strong>Événements de lecture</strong> (play, pause, avance) pour la synchronisation.</li>
<li><strong>Audio du microphone</strong>, uniquement lorsque vous activez le micro.</li>
</ul>
<h2>Circulation des données</h2>
<p>La voix est échangée <strong>directement entre participants</strong> en pair-à-pair (WebRTC). Le chat et les événements de lecture transitent par un serveur de relai uniquement pour être transmis en temps réel aux autres participants de la même salle.</p>
<p><strong>Aucune donnée n'est enregistrée ni conservée</strong> sur le serveur : le relai est instantané et en mémoire vive. Aucun historique, aucun enregistrement audio, aucune vidéo.</p>
<h2>Ce que nous ne faisons pas</h2>
<ul><li>Pas de revente ni de partage à des tiers.</li><li>Pas de publicité ni de pistage.</li><li>Pas de profil utilisateur.</li></ul>
<h2>Microphone</h2>
<p>Le micro est capté uniquement quand vous l'activez, transmis en direct aux autres participants, et coupé dès que vous le désactivez.</p>
<h2>Services tiers</h2>
<p>Des serveurs STUN publics (Google) sont utilisés pour établir les connexions audio pair-à-pair ; ils ne reçoivent que des informations techniques de connexion réseau.</p>
<h2>Contact</h2>
<p><a href="mailto:contact@fb.ventures">contact@fb.ventures</a></p>
<h2>Affiliation</h2>
<p>CinéSync est une extension indépendante, ni affiliée ni approuvée par M6 ou tout autre service de streaming. Les marques citées appartiennent à leurs propriétaires respectifs.</p>
</body></html>`;

// Sert une petite page d'état + healthcheck (utile pour l'hébergement cloud)
const httpServer = createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", rooms: rooms.size }));
    return;
  }
  if (req.url === "/privacy") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(PRIVACY_HTML);
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
