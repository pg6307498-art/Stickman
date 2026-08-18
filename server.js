"use strict";

/**
 * Stickman Duel — real-time WebSocket server
 * -------------------------------------------------------------------------
 * Handles ONLY the high-frequency real-time game layer:
 *   - player movement / direction / animation state relay
 *   - server-authoritative attack validation, HP, death, victory/defeat
 *   - disconnect / reconnect handling during an active match
 *
 * Firebase Realtime Database (used by the client, not this server) still
 * owns room creation, joining, and lobby presence. This server treats the
 * Firebase room code and Firebase playerId as opaque strings and simply
 * reuses them — no second identity system is created.
 *
 * Deploy target: Render Web Service. No database, no Docker, no build step.
 */

const http = require("http");
const WebSocket = require("ws");

/* ============================================================
   CONFIG / CONSTANTS
   These mirror the values baked into the client HTML. Keep in
   sync with WORLD_WIDTH / WORLD_MARGIN / ATTACK_RANGE / damage
   in the game file — do not invent different numbers here.
   ============================================================ */
const PORT = process.env.PORT || 10000;
const HOST = "0.0.0.0";

const WORLD_WIDTH = 5000;
const WORLD_MARGIN = 70;
const MAX_PLAYERS_PER_ROOM = 2;

const ATTACK_RANGE = 130;
const ATTACK_DAMAGE = 20;
const START_HP = 100;

// Matches ATK_IMPACT_PROGRESS (0.30) * ATTACK_DURATION (0.40s) in the client
// animation — the server resolves the hit at roughly the same instant the
// sword visually reaches the target.
const IMPACT_DELAY_MS = 120;

// Minimum time between two attacks landing from the same attacker. Slightly
// longer than the client's own attack animation (400ms) so a modified
// client can't spam attack requests faster than the animation allows.
const ATTACK_COOLDOWN_MS = 380;

// How long a disconnected player has to reconnect during an active match
// before the remaining player is declared the winner.
const DISCONNECT_GRACE_MS = 15000;

const HEARTBEAT_INTERVAL_MS = 30000;

// Basic anti-abuse limits
const MAX_USERNAME_LEN = 24;
const MAX_ROOM_CODE_LEN = 12;
const MAX_PLAYER_ID_LEN = 64;
const MAX_MESSAGE_BYTES = 2048;
const MAX_ATTACK_IDS_REMEMBERED = 50;
const MAX_INBOUND_MSGS_PER_SEC = 30; // simple per-connection throttle

/* ============================================================
   ROOM STATE (in-memory only — no database for movement/combat)
   rooms[roomCode] = {
     roomCode, players: { playerId: Player }, matchState,
     winnerId, loserId, createdAt
   }
   Player = {
     playerId, username, ws, connected,
     x, direction, velocityX, state,
     hp, alive, lastAttackAt, seenAttackIds (array, capped),
     disconnectTimer, joinedAt
   }
   ============================================================ */
const rooms = Object.create(null);

function clamp(v, lo, hi) { return v < lo ? lo : (v > hi ? hi : v); }

function getOrCreateRoom(roomCode) {
  let room = rooms[roomCode];
  if (!room) {
    room = {
      roomCode: roomCode,
      players: Object.create(null),
      matchState: "waiting", // waiting | active | ended
      winnerId: null,
      loserId: null,
      createdAt: Date.now()
    };
    rooms[roomCode] = room;
  }
  return room;
}

function roomPlayerList(room) {
  return Object.keys(room.players).map(function (id) { return room.players[id]; });
}

function opponentOf(room, playerId) {
  const ids = Object.keys(room.players);
  for (let i = 0; i < ids.length; i++) {
    if (ids[i] !== playerId) return room.players[ids[i]];
  }
  return null;
}

function cleanupRoomIfEmpty(room) {
  const anyLeft = Object.keys(room.players).some(function (id) {
    return room.players[id].connected;
  });
  if (!anyLeft) {
    // give a short window in case both are mid-reconnect, then drop it
    setTimeout(function () {
      const stillEmpty = Object.keys(room.players).every(function (id) {
        return !room.players[id].connected;
      });
      if (stillEmpty) delete rooms[room.roomCode];
    }, DISCONNECT_GRACE_MS + 2000);
  }
}

/* ============================================================
   SAFE SEND / BROADCAST
   ============================================================ */
function safeSend(ws, obj) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  try { ws.send(JSON.stringify(obj)); } catch (e) { /* ignore */ }
}

function sendToPlayer(room, playerId, obj) {
  const p = room.players[playerId];
  if (p) safeSend(p.ws, obj);
}

function broadcastRoom(room, obj, excludePlayerId) {
  Object.keys(room.players).forEach(function (id) {
    if (id === excludePlayerId) return;
    safeSend(room.players[id].ws, obj);
  });
}

/* ============================================================
   VALIDATION HELPERS
   ============================================================ */
function isValidRoomCode(code) {
  return typeof code === "string" && code.length > 0 && code.length <= MAX_ROOM_CODE_LEN;
}
function isValidPlayerId(id) {
  return typeof id === "string" && id.length > 0 && id.length <= MAX_PLAYER_ID_LEN;
}
function sanitizeUsername(name) {
  if (typeof name !== "string") return "Player";
  const trimmed = name.trim().slice(0, MAX_USERNAME_LEN);
  return trimmed.length ? trimmed : "Player";
}
function isValidX(x) {
  return typeof x === "number" && isFinite(x) && x >= WORLD_MARGIN - 50 && x <= WORLD_WIDTH - WORLD_MARGIN + 50;
}

/* ============================================================
   MATCH LIFECYCLE
   ============================================================ */
function maybeActivateMatch(room) {
  if (room.matchState === "waiting" && Object.keys(room.players).length >= MAX_PLAYERS_PER_ROOM) {
    room.matchState = "active";
  }
}

function endMatch(room, winnerId, loserId) {
  if (room.matchState === "ended") return;
  room.matchState = "ended";
  room.winnerId = winnerId;
  room.loserId = loserId;
  broadcastRoom(room, { type: "game_over", winnerId: winnerId, loserId: loserId });
}

function declareWinnerByForfeit(room, remainingPlayerId, goneePlayerId) {
  if (room.matchState !== "active") return;
  const remaining = room.players[remainingPlayerId];
  if (!remaining || !remaining.connected) return; // both gone — nothing to award
  endMatch(room, remainingPlayerId, goneePlayerId);
}

/* ============================================================
   MESSAGE HANDLERS
   ============================================================ */
function handleJoin(ws, room, msg) {
  const playerId = msg.playerId;
  const username = sanitizeUsername(msg.username);
  let player = room.players[playerId];

  if (player) {
    // ---- reconnect path: same playerId rejoining ----
    if (player.disconnectTimer) { clearTimeout(player.disconnectTimer); player.disconnectTimer = null; }
    player.ws = ws;
    player.connected = true;
    ws.playerId = playerId;
    ws.roomCode = room.roomCode;

    safeSend(ws, { type: "joined", playerId: playerId, room: room.roomCode });
    // resync this reconnecting client with full authoritative state
    safeSend(ws, {
      type: "resync",
      players: roomPlayerList(room).map(function (p) {
        return { playerId: p.playerId, x: p.x, direction: p.direction, hp: p.hp, dead: !p.alive };
      })
    });
    broadcastRoom(room, { type: "player_reconnected", playerId: playerId }, playerId);
    return;
  }

  if (Object.keys(room.players).length >= MAX_PLAYERS_PER_ROOM) {
    safeSend(ws, { type: "error", message: "room full" });
    return;
  }

  // ---- first-time join ----
  player = {
    playerId: playerId,
    username: username,
    ws: ws,
    connected: true,
    x: WORLD_WIDTH / 2,
    direction: 1,
    velocityX: 0,
    state: "idle",
    hp: START_HP,
    alive: true,
    lastAttackAt: 0,
    seenAttackIds: [],
    disconnectTimer: null,
    joinedAt: Date.now()
  };
  room.players[playerId] = player;
  ws.playerId = playerId;
  ws.roomCode = room.roomCode;

  safeSend(ws, { type: "joined", playerId: playerId, room: room.roomCode });
  safeSend(ws, {
    type: "resync",
    players: roomPlayerList(room).map(function (p) {
      return { playerId: p.playerId, x: p.x, direction: p.direction, hp: p.hp, dead: !p.alive };
    })
  });

  maybeActivateMatch(room);
}

function handleState(room, playerId, msg) {
  const player = room.players[playerId];
  if (!player || !player.connected) return;
  if (!player.alive) return; // dead players don't keep moving the world

  if (isValidX(msg.x)) player.x = clamp(msg.x, WORLD_MARGIN, WORLD_WIDTH - WORLD_MARGIN);
  if (msg.direction === 1 || msg.direction === -1) player.direction = msg.direction;
  if (typeof msg.velocityX === "number" && isFinite(msg.velocityX)) {
    player.velocityX = clamp(msg.velocityX, -2000, 2000);
  }
  if (typeof msg.state === "string" && msg.state.length <= 16) player.state = msg.state;

  broadcastRoom(room, {
    type: "state",
    playerId: playerId,
    x: player.x,
    direction: player.direction,
    velocityX: player.velocityX,
    state: player.state
  }, playerId);
}

function handleAttack(room, playerId, msg) {
  const attacker = room.players[playerId];
  if (!attacker || !attacker.connected) return;
  if (room.matchState !== "active") return; // no damage before start / after end
  if (!attacker.alive) return; // dead players can't attack

  const attackId = msg.attackId;
  if (typeof attackId !== "string" || !attackId.length || attackId.length > 128) return;
  if (attacker.seenAttackIds.indexOf(attackId) !== -1) return; // duplicate — ignore
  attacker.seenAttackIds.push(attackId);
  if (attacker.seenAttackIds.length > MAX_ATTACK_IDS_REMEMBERED) attacker.seenAttackIds.shift();

  const now = Date.now();
  if (now - attacker.lastAttackAt < ATTACK_COOLDOWN_MS) return; // cooldown / rate limit
  attacker.lastAttackAt = now;

  const defender = opponentOf(room, playerId);
  if (!defender) return;

  // Let the opponent's client replay the cosmetic swing animation right away.
  sendToPlayer(room, defender.playerId, { type: "attack_started", attackerId: playerId, attackId: attackId });

  // Resolve the actual hit at roughly the same instant the sword animation
  // reaches its impact point, using the freshest position data available.
  setTimeout(function () {
    resolveAttack(room, playerId, defender.playerId, attackId);
  }, IMPACT_DELAY_MS);
}

function resolveAttack(room, attackerId, defenderId, attackId) {
  if (!rooms[room.roomCode]) return; // room was cleaned up meanwhile
  if (room.matchState !== "active") return;

  const attacker = room.players[attackerId];
  const defender = room.players[defenderId];
  if (!attacker || !defender) return;
  if (!attacker.connected || !attacker.alive) return;
  if (!defender.connected || !defender.alive) return;

  const dx = defender.x - attacker.x;
  const distance = Math.abs(dx);
  const facingOk = (dx === 0) || (Math.sign(dx) === attacker.direction);

  if (distance > ATTACK_RANGE || !facingOk) return; // miss — no message needed

  const newHp = clamp(defender.hp - ATTACK_DAMAGE, 0, 100);
  defender.hp = newHp;
  const died = newHp <= 0;
  if (died) defender.alive = false;

  broadcastRoom(room, {
    type: "damage",
    attackerId: attackerId,
    targetId: defenderId,
    damage: ATTACK_DAMAGE,
    hp: newHp,
    dead: died
  });

  if (died) {
    endMatch(room, attackerId, defenderId);
  }
}

/* ============================================================
   CONNECTION HANDLING
   ============================================================ */
function handleClose(ws) {
  const roomCode = ws.roomCode;
  const playerId = ws.playerId;
  if (!roomCode || !playerId) return;
  const room = rooms[roomCode];
  if (!room) return;
  const player = room.players[playerId];
  if (!player) return;

  player.connected = false;
  player.ws = null;

  broadcastRoom(room, { type: "player_disconnected", playerId: playerId }, playerId);

  if (room.matchState === "active") {
    player.disconnectTimer = setTimeout(function () {
      // only award a win if the player is STILL gone after the grace period
      const stillGone = room.players[playerId] && !room.players[playerId].connected;
      if (stillGone) {
        const opponent = opponentOf(room, playerId);
        if (opponent) declareWinnerByForfeit(room, opponent.playerId, playerId);
      }
    }, DISCONNECT_GRACE_MS);
  } else {
    cleanupRoomIfEmpty(room);
  }
}

/* ============================================================
   HTTP + WEBSOCKET SERVER
   ============================================================ */
const server = http.createServer(function (req, res) {
  // Minimal health-check endpoint for Render / uptime pings.
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Stickman Duel WebSocket server is running.\n");
});

const wss = new WebSocket.Server({ server });

wss.on("connection", function (ws) {
  ws.isAlive = true;
  ws.msgTimestamps = [];

  ws.on("pong", function () { ws.isAlive = true; });

  ws.on("message", function (data) {
    // ---- size limit ----
    if (typeof data !== "string" && data.length > MAX_MESSAGE_BYTES) return;
    if (typeof data === "string" && data.length > MAX_MESSAGE_BYTES) return;

    // ---- simple per-connection rate limit ----
    const now = Date.now();
    ws.msgTimestamps = ws.msgTimestamps.filter(function (t) { return now - t < 1000; });
    if (ws.msgTimestamps.length >= MAX_INBOUND_MSGS_PER_SEC) return;
    ws.msgTimestamps.push(now);

    let msg;
    try { msg = JSON.parse(data); } catch (e) {
      console.error("[WS] invalid JSON from client");
      return;
    }
    if (!msg || typeof msg.type !== "string") {
      console.error("[WS] invalid message shape");
      return;
    }

    if (msg.type === "join") {
      const roomCode = msg.room;
      const playerId = msg.playerId;
      if (!isValidRoomCode(roomCode) || !isValidPlayerId(playerId)) {
        safeSend(ws, { type: "error", message: "invalid room or player id" });
        return;
      }
      const room = getOrCreateRoom(roomCode);
      handleJoin(ws, room, msg);
      return;
    }

    // every other message type requires an established join first
    const roomCode = ws.roomCode;
    const playerId = ws.playerId;
    if (!roomCode || !playerId || !rooms[roomCode]) {
      safeSend(ws, { type: "error", message: "not joined to a room" });
      return;
    }
    const room = rooms[roomCode];

    switch (msg.type) {
      case "state":
        handleState(room, playerId, msg);
        break;
      case "attack":
        handleAttack(room, playerId, msg);
        break;
      default:
        safeSend(ws, { type: "error", message: "unknown message type" });
        break;
    }
  });

  ws.on("close", function () { handleClose(ws); });
  ws.on("error", function (err) {
    console.error("[WS] socket error:", err && err.message ? err.message : err);
  });
});

/* ============================================================
   HEARTBEAT — detect and drop stale connections
   ============================================================ */
const heartbeatInterval = setInterval(function () {
  wss.clients.forEach(function (ws) {
    if (ws.isAlive === false) {
      ws.terminate();
      return;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) { /* ignore */ }
  });
}, HEARTBEAT_INTERVAL_MS);

wss.on("close", function () { clearInterval(heartbeatInterval); });

server.listen(PORT, HOST, function () {
  console.log("Stickman Duel WebSocket server listening on " + HOST + ":" + PORT);
});
