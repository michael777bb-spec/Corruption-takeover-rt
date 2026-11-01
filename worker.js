// Cloudflare Worker + Durable Object: настоящий реалтайм по WebSocket.
// Каждая комната = один Durable Object (класс Room). Профиль (необязательно) хранится в KV.
import { DurableObject } from "cloudflare:workers";
import page from "./index.html"; // страница игры лежит рядом и вшивается в воркер (текстовый модуль)

const json = (d, s = 200) =>
  new Response(JSON.stringify(d), { status: s, headers: { "content-type": "application/json" } });

const ROOM_TTL_MS = 15 * 60 * 1000; // пустая комната удаляется через 15 минут
const MAX_PLAYERS = 4;
const LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ";

const clampInt = (v, lo, hi) => Math.max(lo, Math.min(hi, Math.floor(Number(v) || 0)));
const cleanName = (s) => String(s || "Игрок").replace(/[<>&"]/g, "").slice(0, 16) || "Игрок";
const cleanId = (s) => String(s || "").replace(/[^a-zA-Z0-9]/g, "").slice(0, 24);
const genCode = () => Array.from({ length: 4 }, () => LETTERS[Math.floor(Math.random() * LETTERS.length)]).join("");
const readBody = async (req) => { try { return await req.json(); } catch { return {}; } };

/* ===================== КОМНАТА ===================== */
export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.meta = null;
    ctx.blockConcurrencyWhile(async () => { this.meta = (await ctx.storage.get("meta")) || null; });
  }

  // игроки хранятся во вложениях WebSocket-соединений (переживают «сон» объекта)
  players() {
    const list = [];
    for (const ws of this.ctx.getWebSockets()) {
      const p = ws.deserializeAttachment();
      if (p && p.id) list.push(p);
    }
    return list.sort((a, b) => a.joined - b.joined);
  }

  snapshot() {
    const players = this.players();
    const meta = { ...this.meta };
    if (players.length && !players.some((p) => p.id === meta.host)) meta.host = players[0].id;
    return JSON.stringify({ t: "state", meta, players });
  }

  broadcast() {
    const s = this.snapshot();
    for (const ws of this.ctx.getWebSockets()) { try { ws.send(s); } catch {} }
  }

  async fetch(req) {
    const url = new URL(req.url);

    if (url.pathname === "/create") {
      if (this.meta) return json({ error: "exists" }, 409);
      const b = await readBody(req);
      this.meta = { host: cleanId(b.id), level: 0, state: "lobby", seed: 0, mode: 0, imp: "" };
      await this.ctx.storage.put("meta", this.meta);
      await this.ctx.storage.setAlarm(Date.now() + ROOM_TTL_MS);
      return json({ ok: true });
    }

    if (url.pathname === "/join") {
      if (!this.meta) return json({ error: "no room" }, 404);
      const b = await readBody(req);
      const id = cleanId(b.id);
      const ps = this.players();
      if (!ps.some((p) => p.id === id) && ps.length >= MAX_PLAYERS) return json({ error: "full" }, 409);
      return json({ ok: true });
    }

    if (url.pathname === "/ws") {
      if (req.headers.get("Upgrade") !== "websocket") return new Response("expected websocket", { status: 426 });
      if (!this.meta) return json({ error: "no room" }, 404);
      const id = cleanId(url.searchParams.get("id"));
      if (!id) return json({ error: "id" }, 400);
      const ps = this.players();
      if (!ps.some((p) => p.id === id) && ps.length >= MAX_PLAYERS) return json({ error: "full" }, 409);
      // старое соединение того же игрока закрываем
      for (const ws of this.ctx.getWebSockets()) {
        const p = ws.deserializeAttachment();
        if (p && p.id === id) { try { ws.close(1000, "replaced"); } catch {} }
      }
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      this.ctx.acceptWebSocket(server);
      server.serializeAttachment({
        id,
        name: cleanName(url.searchParams.get("name")),
        char: clampInt(url.searchParams.get("char"), 0, 4),
        joined: Date.now(),
      });
      await this.ctx.storage.deleteAlarm();
      this.broadcast();
      return new Response(null, { status: 101, webSocket: client });
    }
    return json({ error: "not found" }, 404);
  }

  async webSocketMessage(ws, raw) {
    let b;
    try { b = JSON.parse(typeof raw === "string" ? raw : new TextDecoder().decode(raw)); } catch { return; }
    const me = ws.deserializeAttachment();
    if (!me || !b) return;

    if (b.t === "leave") { try { ws.close(1000, "leave"); } catch {} return; }
    if (b.t === "ping") { try { ws.send('{"t":"pong"}'); } catch {} return; }
    if (b.t !== "set") return;

    if (b.action === "meta") {
      const ps = this.players();
      const hostAlive = ps.some((p) => p.id === this.meta.host);
      if (this.meta.host !== me.id && hostAlive) return; // не хост
      this.meta.host = me.id;
      if (b.level !== undefined) this.meta.level = clampInt(b.level, 0, 3);
      if (b.state === "playing" || b.state === "lobby") this.meta.state = b.state;
      if (b.seed) this.meta.seed = clampInt(b.seed, 1, 2147483647);
      if (b.mode !== undefined) this.meta.mode = clampInt(b.mode, 0, 2);
      if (b.imp !== undefined) this.meta.imp = cleanId(b.imp) || "";
      await this.ctx.storage.put("meta", this.meta);
      this.broadcast();
      return;
    }

    // обновление своего игрока
    const p = { ...me, name: cleanName(b.name ?? me.name), char: clampInt(b.char ?? me.char, 0, 4) };
    if (b.x !== undefined) {
      p.x = clampInt(b.x, 0, 100000);
      p.y = clampInt(b.y, 0, 1000);
      p.alive = !!b.alive;
      p.finished = !!b.finished;
      p.hp = clampInt(b.hp ?? 100, 0, 100);
      if (b.pz && typeof b.pz === "object") p.pz = { to: cleanId(b.pz.to), n: clampInt(b.pz.n, 0, 9e15) };
      if (b.kill && typeof b.kill === "object") p.kill = { to: cleanId(b.kill.to), n: clampInt(b.kill.n, 0, 9e15) };
    } else {
      delete p.x; delete p.y; delete p.alive; delete p.finished; delete p.hp; delete p.pz; delete p.kill;
    }
    ws.serializeAttachment(p);
    this.broadcast();
  }

  async webSocketClose(ws) { await this.afterLeave(ws); }
  async webSocketError(ws) { await this.afterLeave(ws); }

  async afterLeave(ws) {
    try { ws.close(); } catch {}
    this.broadcast();
    if (this.ctx.getWebSockets().length === 0) await this.ctx.storage.setAlarm(Date.now() + ROOM_TTL_MS);
  }

  // пустая комната через 15 минут удаляется целиком
  async alarm() {
    if (this.ctx.getWebSockets().length === 0) { await this.ctx.storage.deleteAll(); this.meta = null; }
  }
}

/* ===================== ВОРКЕР ===================== */
const roomStub = (env, code) => env.ROOM.get(env.ROOM.idFromName(code));

const CORS = { "access-control-allow-origin": "*", "access-control-allow-methods": "GET,POST,PUT,OPTIONS", "access-control-allow-headers": "content-type" };

export default {
  async fetch(req, env) {
    if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
    const res = await this.handle(req, env);
    if (res.status === 101) return res;
    const out = new Response(res.body, res);
    for (const [k, v] of Object.entries(CORS)) out.headers.set(k, v);
    return out;
  },

  async handle(req, env) {
    const url = new URL(req.url);
    const path = url.pathname;
    if (!path.startsWith("/api/")) {
      return new Response(page, { headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-cache" } });
    }

    try {
      // ---------- ПРОФИЛЬ (KV необязателен) ----------
      if (path === "/api/profile") {
        if (!env.KV) return req.method === "GET" ? json({ profile: null }) : json({ ok: true });
        if (req.method === "GET") {
          const id = cleanId(url.searchParams.get("id"));
          if (!id) return json({ error: "id" }, 400);
          return json({ profile: await env.KV.get(`user:${id}`, "json") });
        }
        if (req.method === "PUT") {
          const b = await readBody(req);
          const id = cleanId(b.id);
          if (!id || !b.profile) return json({ error: "bad" }, 400);
          const owned = [...new Set([0, ...(b.profile.owned || [])].filter((i) => Number.isInteger(i) && i >= 0 && i <= 4))];
          const profile = {
            xp: clampInt(b.profile.xp, 0, 1e9),
            money: clampInt(b.profile.money, 0, 1e9),
            owned,
            sel: owned.includes(b.profile.sel) ? b.profile.sel : 0,
            wins: clampInt(b.profile.wins, 0, 1e6),
          };
          await env.KV.put(`user:${id}`, JSON.stringify(profile));
          return json({ ok: true });
        }
      }

      // ---------- СОЗДАНИЕ КОМНАТЫ ----------
      if (path === "/api/room" && req.method === "POST") {
        const b = await readBody(req);
        const id = cleanId(b.id);
        if (!id) return json({ error: "id" }, 400);
        for (let i = 0; i < 8; i++) {
          const code = genCode();
          const r = await roomStub(env, code).fetch("https://room/create", { method: "POST", body: JSON.stringify({ id }) });
          if (r.ok) return json({ code, ws: true });
        }
        return json({ error: "busy" }, 503);
      }

      // ---------- /api/room/CODE/(join|ws) ----------
      const m = path.match(/^\/api\/room\/([A-Za-z]{4})\/(join|ws)$/);
      if (m) {
        const stub = roomStub(env, m[1].toUpperCase());
        if (m[2] === "join" && req.method === "POST") {
          const b = await readBody(req);
          const jr = await stub.fetch("https://room/join", { method: "POST", body: JSON.stringify({ id: b.id }) });
          if (!jr.ok) return jr;
          return json({ ok: true, ws: true });
        }
        if (m[2] === "ws") return stub.fetch(new Request("https://room/ws" + url.search, req));
      }

      return json({ error: "not found" }, 404);
    } catch (e) {
      return json({ error: String(e) }, 500);
    }
  },
};
