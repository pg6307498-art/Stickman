"use strict";

/**
 * Stickman Duel — Open World — real-time WebSocket server
 * -------------------------------------------------------------------------
 * This server is now the SINGLE source of truth for everything: room
 * creation, joining, capacity, player count, spawning, 2D movement,
 * server-authoritative attack validation, HP, death/respawn, and
 * disconnect/reconnect handling.
 *
 * Firebase has been removed entirely. Keeping room/lobby state in Firebase
 * while movement/combat lived in WebSockets meant two different sources of
 * truth for "how many players are in this room right now" — which is
 * exactly the number that has to be correct for max-10-players / auto-start
 * at 2 / late-joining / ROOM FULL to behave correctly. Making the WS server
 * authoritative for membership too removes that whole class of race
 * conditions.
 *
 * Deploy target: Render Web Service. No database, no Docker, no build step.
 */

const http = require("http");
const WebSocket = require("ws");

/* ============================================================
   CONFIG / CONSTANTS
   These mirror the values baked into the client HTML. Keep in
   sync with WORLD_WIDTH / WORLD_HEIGHT / WORLD_MARGIN /
   ATTACK_RANGE / damage in the game file.
   ============================================================ */
const PORT = process.env.PORT || 10000;
const HOST = "0.0.0.0";

const WORLD_WIDTH = 3600;
const WORLD_HEIGHT = 2000;
const WORLD_MARGIN = 90;

const MIN_PLAYERS_TO_START = 2;
const MAX_PLAYERS_PER_ROOM = 10;

const ATTACK_RANGE = 140;
// Half-angle (radians) of the forward-facing hit cone. ~65 deg either side
// of the attacker's facing direction (~130 deg cone total) — generous
// enough to feel fair in a free 2D arena, not so wide it hits behind.
const ATTACK_HALF_ANGLE_COS = Math.cos((65 * Math.PI) / 180);
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

// How long a dead player waits before respawning back into the arena.
const RESPAWN_DELAY_MS = 3000;

// How long a disconnected player's slot is kept (and their body left in
// place) before other players are told they're gone for good.
const DISCONNECT_GRACE_MS = 15000;

const HEARTBEAT_INTERVAL_MS = 30000;

// Basic anti-abuse limits
const MAX_USERNAME_LEN = 24;
const MAX_ROOM_CODE_LEN = 12;
const MAX_PLAYER_ID_LEN = 64;
const MAX_MESSAGE_BYTES = 2048;
const MAX_ATTACK_IDS_REMEMBERED = 50;
const MAX_INBOUND_MSGS_PER_SEC = 30; // simple per-connection throttle

const ROOM_CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // excludes 0,O,1,I

/* ============================================================
   ROOM STATE (in-memory only)
   rooms[roomCode] = {
     roomCode, players: { playerId: Player }, matchState,
     createdAt, nextSpawnIndex
   }
   Player = {
     playerId, username, ws, connected,
     x, y, dirX, dirY, velocityX, velocityY, state,
     hp, alive, lastAttackAt, seenAttackIds (array, capped),
     disconnectTimer, respawnTimer, joinedAt
   }
   ============================================================ */
const rooms = Object.create(null);

function clamp(v, lo, hi) { return v < lo ? lo : (v > hi ? hi : v); }

// Ten hand-placed spawn spots spread across the arena (corners, edges,
// center-ish) so simultaneous joins don't stack players on top of each
// other. Expressed as fractions of the world so they scale with
// WORLD_WIDTH / WORLD_HEIGHT.
const SPAWN_FRACTIONS = [
  { x: 0.12, y: 0.50 }, // left
  { x: 0.88, y: 0.50 }, // right
  { x: 0.15, y: 0.15 }, // top-left
  { x: 0.85, y: 0.15 }, // top-right
  { x: 0.15, y: 0.85 }, // bottom-left
  { x: 0.85, y: 0.85 }, // bottom-right
  { x: 0.50, y: 0.12 }, // top-center
  { x: 0.50, y: 0.88 }, // bottom-center
  { x: 0.35, y: 0.50 }, // mid-left
  { x: 0.65, y: 0.50 }  // mid-right
];

function spawnPointFor(index) {
  var frac = SPAWN_FRACTIONS[index % SPAWN_FRACTIONS.length];
  // small deterministic-ish jitter so repeated respawns at the same slot
  // don't land pixel-identical on top of a previous body
  var jitterX = (Math.random() - 0.5) * 60;
  var jitterY = (Math.random() - 0.5) * 60;
  var x = clamp(WORLD_MARGIN + frac.x * (WORLD_WIDTH - 2 * WORLD_MARGIN) + jitterX, WORLD_MARGIN, WORLD_WIDTH - WORLD_MARGIN);
  var y = clamp(WORLD_MARGIN + frac.y * (WORLD_HEIGHT - 2 * WORLD_MARGIN) + jitterY, WORLD_MARGIN, WORLD_HEIGHT - WORLD_MARGIN);
  return { x: x, y: y };
}

function getOrCreateRoom(roomCode) {
  let room = rooms[roomCode];
  if (!room) {
    room = {
      roomCode: roomCode,
      players: Object.create(null),
      matchState: "waiting", // waiting | active
      createdAt: Date.now(),
      nextSpawnIndex: 0
    };
    rooms[roomCode] = room;
  }
  return room;
}

function generateUniqueRoomCode() {
  for (let attempt = 0; attempt < 50; attempt++) {
    let code = "";
    for (let i = 0; i < 6; i++) code += ROOM_CODE_CHARS[Math.floor(Math.random() * ROOM_CODE_CHARS.length)];
    if (!rooms[code]) return code;
  }
  // astronomically unlikely fallback
  return "R" + Date.now().toString(36).toUpperCase().slice(-8);
}

function roomPlayerList(room) {
  return Object.keys(room.players).map(function (id) { return room.players[id]; });
}

function connectedPlayerCount(room) {
  return roomPlayerList(room).filter(function (p) { return p.connected; }).length;
}

function publicPlayer(p) {
  return {
    playerId: p.playerId,
    username: p.username,
    x: p.x,
    y: p.y,
    dirX: p.dirX,
    dirY: p.dirY,
    state: p.state,
    hp: p.hp,
    alive: p.alive,
    connected: p.connected
  };
}

function cleanupRoomIfEmpty(room) {
  const anyLeft = roomPlayerList(room).some(function (p) { return p.connected; });
  if (!anyLeft) {
    // give a short window in case everyone is mid-reconnect, then drop it
    setTimeout(function () {
      const stillEmpty = roomPlayerList(room).every(function (p) { return !p.connected; });
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

function broadcastPlayerCount(room) {
  broadcastRoom(room, {
    type: "player_count",
    count: connectedPlayerCount(room),
    maxPlayers: MAX_PLAYERS_PER_ROOM
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
function isValidCoord(v, max) {
  return typeof v === "number" && isFinite(v) && v >= -50 && v <= max + 50;
}

/* ============================================================
   MATCH LIFECYCLE
   ============================================================ */
function maybeActivateMatch(room) {
  if (room.matchState === "waiting" && connectedPlayerCount(room) >= MIN_PLAYERS_TO_START) {
    room.matchState = "active";
    return true; // just activated
  }
  return false;
}

/* ============================================================
   JOIN / CREATE
   ============================================================ */
function doJoin(ws, room, playerId, username) {
  let player = room.players[playerId];

  if (player) {
    // ---- reconnect path: same playerId rejoining ----
    if (player.disconnectTimer) { clearTimeout(player.disconnectTimer); player.disconnectTimer = null; }
    player.ws = ws;
    player.connected = true;
    ws.playerId = playerId;
    ws.roomCode = room.roomCode;

    safeSend(ws, {
      type: "room_joined",
      room: room.roomCode,
      playerId: playerId,
      matchState: room.matchState,
      justActivated: false,
      maxPlayers: MAX_PLAYERS_PER_ROOM,
      players: roomPlayerList(room).map(publicPlayer)
    });
    broadcastRoom(room, { type: "player_reconnected", playerId: playerId }, playerId);
    broadcastPlayerCount(room);
    return;
  }

  if (Object.keys(room.players).length >= MAX_PLAYERS_PER_ROOM) {
    safeSend(ws, { type: "error", code: "room_full", message: "ROOM FULL" });
    return;
  }

  // ---- first-time join ----
  const wasActive = room.matchState === "active";
  const spawn = spawnPointFor(room.nextSpawnIndex++);

  player = {
    playerId: playerId,
    username: username,
    ws: ws,
    connected: true,
    x: spawn.x,
    y: spawn.y,
    dirX: 1,
    dirY: 0,
    velocityX: 0,
    velocityY: 0,
    state: "idle",
    hp: START_HP,
    alive: true,
    lastAttackAt: 0,
    seenAttackIds: [],
    disconnectTimer: null,
    respawnTimer: null,
    joinedAt: Date.now()
  };
  room.players[playerId] = player;
  ws.playerId = playerId;
  ws.roomCode = room.roomCode;

  const justActivated = maybeActivateMatch(room);

  safeSend(ws, {
    type: "room_joined",
    room: room.roomCode,
    playerId: playerId,
    matchState: room.matchState,
    justActivated: justActivated,
    maxPlayers: MAX_PLAYERS_PER_ROOM,
    players: roomPlayerList(room).map(publicPlayer)
  });

  broadcastRoom(room, {
    type: "player_joined",
    player: publicPlayer(player)
  }, playerId);

  if (justActivated) {
    // Both the waiting creator AND the player who just triggered activation
    // run the same synced 3-2-1-FIGHT countdown together.
    broadcastRoom(room, { type: "match_started" });
  }

  broadcastPlayerCount(room);
}

function handleCreateRoom(ws, msg) {
  const playerId = msg.playerId;
  if (!isValidPlayerId(playerId)) {
    safeSend(ws, { type: "error", code: "invalid_player", message: "invalid player id" });
    return;
  }
  const username = sanitizeUsername(msg.username);
  const code = generateUniqueRoomCode();
  const room = getOrCreateRoom(code);
  doJoin(ws, room, playerId, username);
}

function handleJoinRoom(ws, msg) {
  const roomCode = msg.room;
  const playerId = msg.playerId;
  if (!isValidRoomCode(roomCode) || !isValidPlayerId(playerId)) {
    safeSend(ws, { type: "error", code: "invalid", message: "invalid room or player id" });
    return;
  }
  const existing = rooms[roomCode];
  if (!existing) {
    safeSend(ws, { type: "error", code: "room_not_found", message: "ROOM NOT FOUND" });
    return;
  }
  const username = sanitizeUsername(msg.username);
  doJoin(ws, existing, playerId, username);
}

/* ============================================================
   STATE / MOVEMENT
   ============================================================ */
function handleState(room, playerId, msg) {
  const player = room.players[playerId];
  if (!player || !player.connected) return;
  if (!player.alive) return; // dead players don't keep moving the world

  if (isValidCoord(msg.x, WORLD_WIDTH)) player.x = clamp(msg.x, WORLD_MARGIN, WORLD_WIDTH - WORLD_MARGIN);
  if (isValidCoord(msg.y, WORLD_HEIGHT)) player.y = clamp(msg.y, WORLD_MARGIN, WORLD_HEIGHT - WORLD_MARGIN);

  if (typeof msg.dirX === "number" && typeof msg.dirY === "number" && isFinite(msg.dirX) && isFinite(msg.dirY)) {
    const mag = Math.sqrt(msg.dirX * msg.dirX + msg.dirY * msg.dirY);
    if (mag > 0.001) { player.dirX = msg.dirX / mag; player.dirY = msg.dirY / mag; }
  }
  if (typeof msg.velocityX === "number" && isFinite(msg.velocityX)) player.velocityX = clamp(msg.velocityX, -2000, 2000);
  if (typeof msg.velocityY === "number" && isFinite(msg.velocityY)) player.velocityY = clamp(msg.velocityY, -2000, 2000);
  if (typeof msg.state === "string" && msg.state.length <= 16) player.state = msg.state;

  broadcastRoom(room, {
    type: "state",
    playerId: playerId,
    x: player.x,
    y: player.y,
    dirX: player.dirX,
    dirY: player.dirY,
    velocityX: player.velocityX,
    velocityY: player.velocityY,
    state: player.state
  }, playerId);
}

/* ============================================================
   ATTACK / DAMAGE (server-authoritative, 2D)
   ============================================================ */
function handleAttack(room, playerId, msg) {
  const attacker = room.players[playerId];
  if (!attacker || !attacker.connected) return;
  if (room.matchState !== "active") return; // no damage before the match starts
  if (!attacker.alive) return; // dead players can't attack

  const attackId = msg.attackId;
  if (typeof attackId !== "string" || !attackId.length || attackId.length > 128) return;
  if (attacker.seenAttackIds.indexOf(attackId) !== -1) return; // duplicate — ignore
  attacker.seenAttackIds.push(attackId);
  if (attacker.seenAttackIds.length > MAX_ATTACK_IDS_REMEMBERED) attacker.seenAttackIds.shift();

  const now = Date.now();
  if (now - attacker.lastAttackAt < ATTACK_COOLDOWN_MS) return; // cooldown / rate limit
  attacker.lastAttackAt = now;

  // Let everyone else replay the cosmetic swing animation right away.
  broadcastRoom(room, { type: "attack_started", attackerId: playerId, attackId: attackId }, playerId);

  // Resolve the actual hit(s) at roughly the same instant the sword
  // animation reaches its impact point, using the freshest position data.
  setTimeout(function () {
    resolveAttack(room, playerId, attackId);
  }, IMPACT_DELAY_MS);
}

function resolveAttack(room, attackerId, attackId) {
  if (!rooms[room.roomCode]) return; // room was cleaned up meanwhile
  if (room.matchState !== "active") return;

  const attacker = room.players[attackerId];
  if (!attacker || !attacker.connected || !attacker.alive) return;

  const fdx = attacker.dirX, fdy = attacker.dirY;
  const facingMag = Math.sqrt(fdx * fdx + fdy * fdy) || 1;
  const nfx = fdx / facingMag, nfy = fdy / facingMag;

  const hits = [];

  roomPlayerList(room).forEach(function (defender) {
    if (defender.playerId === attackerId) return;
    if (!defender.connected || !defender.alive) return;

    const dx = defender.x - attacker.x;
    const dy = defender.y - attacker.y;
    const dist = Math.sqrt(dx * dx + dy * dy);
    if (dist > ATTACK_RANGE) return;

    let facingOk = true;
    if (dist > 1) {
      const dot = (dx / dist) * nfx + (dy / dist) * nfy;
      facingOk = dot >= ATTACK_HALF_ANGLE_COS;
    }
    if (!facingOk) return;

    const newHp = clamp(defender.hp - ATTACK_DAMAGE, 0, START_HP);
    defender.hp = newHp;
    const died = newHp <= 0;
    if (died) defender.alive = false;

    hits.push({ targetId: defender.playerId, hp: newHp, dead: died });

    if (died) scheduleRespawn(room, defender);
  });

  if (hits.length) {
    broadcastRoom(room, {
      type: "damage",
      attackerId: attackerId,
      attackId: attackId,
      hits: hits
    });
    hits.forEach(function (h) {
      if (h.dead) {
        broadcastRoom(room, { type: "death", playerId: h.targetId, killerId: attackerId });
      }
    });
  }
}

function scheduleRespawn(room, player) {
  if (player.respawnTimer) clearTimeout(player.respawnTimer);
  player.respawnTimer = setTimeout(function () {
    player.respawnTimer = null;
    if (!rooms[room.roomCode]) return;
    if (!room.players[player.playerId]) return;
    if (!player.connected) return; // don't respawn a slot nobody's watching; reconnect path revives instead
    const spawn = spawnPointFor(room.nextSpawnIndex++);
    player.x = spawn.x;
    player.y = spawn.y;
    player.hp = START_HP;
    player.alive = true;
    player.state = "idle";
    broadcastRoom(room, { type: "respawn", playerId: player.playerId, x: player.x, y: player.y, hp: player.hp });
  }, RESPAWN_DELAY_MS);
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
  broadcastPlayerCount(room);

  // Give them a window to reconnect. If they never come back, tell the
  // room they're gone for good so remote clients can drop the fighter.
  player.disconnectTimer = setTimeout(function () {
    const stillGone = room.players[playerId] && !room.players[playerId].connected;
    if (stillGone) {
      delete room.players[playerId];
      broadcastRoom(room, { type: "player_left", playerId: playerId });
      broadcastPlayerCount(room);
      cleanupRoomIfEmpty(room);
    }
  }, DISCONNECT_GRACE_MS);
}

/* ============================================================
   HTTP + WEBSOCKET SERVER
   ============================================================ */
const server = http.createServer(function (req, res) {
  // Minimal health-check endpoint for Render / uptime pings.
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Stickman Duel Open World WebSocket server is running.\n");
});

const wss = new WebSocket.Server({ server });

wss.on("connection", function (ws) {
  ws.isAlive = true;
  ws.msgTimestamps = [];

  ws.on("pong", function () { ws.isAlive = true; });

  ws.on("message", function (data) {
    // ---- size limit ----
    if (typeof data === "string" && data.length > MAX_MESSAGE_BYTES) return;
    if (typeof data !== "string" && data.length > MAX_MESSAGE_BYTES) return;

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

    if (msg.type === "create_room") { handleCreateRoom(ws, msg); return; }
    if (msg.type === "join_room") { handleJoinRoom(ws, msg); return; }

    // every other message type requires an established join first
    const roomCode = ws.roomCode;
    const playerId = ws.playerId;
    if (!roomCode || !playerId || !rooms[roomCode]) {
      safeSend(ws, { type: "error", code: "not_joined", message: "not joined to a room" });
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
        safeSend(ws, { type: "error", code: "unknown_type", message: "unknown message type" });
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
  console.log("Stickman Duel Open World WebSocket server listening on " + HOST + ":" + PORT);
});
