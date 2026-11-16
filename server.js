// Authenticated proxy egress relay (single-file Node service).
// Accepts HTTPS requests from a backend, fetches the target through the
// per-request proxy supplied by the caller, and streams the response back.
// It exists because some edge runtimes cannot route fetch through a proxy
// or sustain this kind of socket usage; a plain Node process has no such
// limits.
// Egress: impit by default (Rust core, Chrome-class TLS/JA3 and HTTP/2
// fingerprint) - upstream edges filter plain HTTP clients by transport
// fingerprint. Set RELAY_FETCH_MODE=undici to use undici instead. If the
// impit native binary fails to load, the service falls back automatically
// and never refuses to start over it; /stats reports the effective mode.
// Auth: the x-relay-key header must match the RELAY_KEY env variable.
const http = require("http");
const { fetch, ProxyAgent } = require("undici");

const KEY = process.env.RELAY_KEY || "";
const PORT = Number(process.env.SERVER_PORT || process.env.PORT || 10000);
// Bind address: some hosts assign the listen IP and port; pass them in
// via the HOST and SERVER_PORT/PORT env variables. Defaults to 0.0.0.0.
const HOST = process.env.HOST || "0.0.0.0";

// ---- 出网实现选择 ----
const REQUESTED_MODE =
  String(process.env.RELAY_FETCH_MODE || "impit").trim().toLowerCase() === "undici" ? "undici" : "impit";
let Impit = null;
let impitLoadError = "";
if (REQUESTED_MODE === "impit") {
  try {
    ({ Impit } = require("impit"));
  } catch (e) {
    impitLoadError = e && e.message ? e.message : String(e);
    console.error("impit load failed, falling back to undici:", impitLoadError);
  }
}
const FETCH_MODE = Impit ? "impit" : "undici";
console.log("relay fetch mode:", FETCH_MODE, FETCH_MODE === REQUESTED_MODE ? "" : `(requested ${REQUESTED_MODE})`);

// impit 的代理是实例级配置（proxyUrl，无逐请求覆盖），按 proxy 缓存实例
// 复用其连接池；直连用空串键。实例数随代理数量走，超上限整体清空重建。
const impitClients = new Map();
function impitClient(proxy) {
  const key = proxy || "";
  let client = impitClients.get(key);
  if (!client) {
    if (impitClients.size >= 64) impitClients.clear();
    client = new Impit({
      browser: "chrome151",
      proxyUrl: proxy || undefined,
      followRedirects: false,
    });
    impitClients.set(key, client);
  }
  return client;
}

async function fetchUpstream(target, method, headers, bodyBuf, proxy) {
  const body = bodyBuf && method !== "GET" && method !== "HEAD" ? bodyBuf : undefined;
  if (FETCH_MODE === "impit") {
    return impitClient(proxy).fetch(target, {
      method,
      headers,
      body,
      redirect: "manual",
      timeout: 180000,
    });
  }
  return fetch(target, {
    method,
    headers,
    body,
    dispatcher: proxy ? new ProxyAgent(proxy) : undefined,
    redirect: "manual",
    signal: AbortSignal.timeout(180000),
  });
}

// 响应头归一：undici 给 Headers 对象、impit 给 [name, value] 对数组，
// 都收成普通对象；同名多值（如 set-cookie）收成数组，交给 writeHead。
// content-encoding 一并去掉：两个客户端都会把正文解压后交付（实测
// content-encoding: br 时拿到的已是明文），原样透传会让下游再解一次而损坏。
function collectHeaders(upstream) {
  const out = {};
  const put = (k, v) => {
    const lk = String(k).toLowerCase();
    if (lk === "connection" || lk === "transfer-encoding" || lk === "content-length" || lk === "keep-alive" || lk === "content-encoding") return;
    if (out[lk] === undefined) out[lk] = v;
    else if (Array.isArray(out[lk])) out[lk].push(v);
    else out[lk] = [out[lk], v];
  };
  const h = upstream.headers;
  if (Array.isArray(h)) {
    for (const pair of h) if (Array.isArray(pair) && pair.length >= 2) put(pair[0], pair[1]);
  } else if (h && typeof h.forEach === "function") {
    h.forEach((v, k) => put(k, v));
  }
  return out;
}

// Traffic counters: /fetch accumulates per proxy - up = request body
// bytes, down = response body bytes (counted chunk by chunk while
// streaming, bodies are never buffered whole; application-layer bytes
// only, no protocol headers). Proxy keys are masked (first 4 chars of
// the username + "***@" + host[:port]); /stats never exposes full
// credentials. Requests without a proxy are not counted.
// startedAt = 本进程启动时刻：后端收割时用它判断中转是否重启过（计数清零）。
const startedAt = Date.now();
const proxyStats = new Map();
// Total request counter for the status endpoint: every authenticated
// /fetch counts (including direct requests without a proxy - perProxy
// only tracks proxied traffic). Resets on process restart; together
// with startedAt it identifies one continuous run.
let totalRequests = 0;
let lastRequestAt = 0;

function maskProxyKey(proxy) {
  try {
    const u = new URL(proxy);
    const host = u.port ? `${u.hostname}:${u.port}` : u.hostname;
    const user = u.username ? `${u.username.slice(0, 4)}***` : "***";
    return `${user}@${host}`;
  } catch {
    return "***";
  }
}

function statsEntry(proxy) {
  const key = maskProxyKey(proxy);
  let entry = proxyStats.get(key);
  if (!entry) {
    entry = { up: 0, down: 0, requests: 0 };
    proxyStats.set(key, entry);
  }
  return entry;
}

const server = http.createServer(async (req, res) => {
  if (req.method === "GET" && req.url === "/") {
    res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
    res.end("relay ok");
    return;
  }
  if (req.method === "GET" && req.url === "/stats") {
    if (!KEY || req.headers["x-relay-key"] !== KEY) {
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "unauthorized" }));
      return;
    }
    const perProxy = {};
    for (const [k, v] of proxyStats) {
      perProxy[k] = { up: v.up, down: v.down, total: v.up + v.down, requests: v.requests };
    }
    const payload = { startedAt, uptimeMs: Date.now() - startedAt, totalRequests, lastRequestAt, mode: FETCH_MODE, perProxy };
    if (impitLoadError) payload.impitLoadError = impitLoadError;
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(payload));
    return;
  }
  if (req.method !== "POST" || req.url !== "/fetch") {
    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "not found" }));
    return;
  }
  if (!KEY || req.headers["x-relay-key"] !== KEY) {
    res.writeHead(401, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "unauthorized" }));
    return;
  }
  let raw = "";
  for await (const chunk of req) raw += chunk;
  let p;
  try {
    p = JSON.parse(raw || "{}");
  } catch {
    res.writeHead(400, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "bad json" }));
    return;
  }
  const target = String(p.url || "");
  if (!/^https?:\/\//i.test(target)) {
    res.writeHead(400, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "url must be http(s)" }));
    return;
  }
  const method = String(p.method || "GET").toUpperCase();
  const headers = p.headers && typeof p.headers === "object" ? p.headers : {};
  const bodyBuf = p.bodyB64 ? Buffer.from(String(p.bodyB64), "base64") : undefined;
  // 流量计数：带 proxy 才计；请求数与上行字节在发起前记，下行在回传时逐块记
  totalRequests += 1;
  lastRequestAt = Date.now();
  const stat = p.proxy ? statsEntry(String(p.proxy)) : null;
  if (stat) {
    stat.requests += 1;
    stat.up += bodyBuf ? bodyBuf.length : 0;
  }
  try {
    const upstream = await fetchUpstream(target, method, headers, bodyBuf, p.proxy ? String(p.proxy) : "");
    res.writeHead(upstream.status, collectHeaders(upstream));
    if (upstream.body) {
      for await (const chunk of upstream.body) {
        if (stat) stat.down += chunk.length;
        res.write(chunk);
      }
    }
    res.end();
  } catch (e) {
    if (!res.headersSent) res.writeHead(502, { "Content-Type": "application/json" });
    const cause = e && e.cause ? " | cause: " + (e.cause.message || String(e.cause)) + (e.cause.code ? " (" + e.cause.code + ")" : "") : "";
    res.end(JSON.stringify({ error: "relay upstream failed: " + (e && e.message ? e.message : String(e)) + cause }));
  }
});

server.listen(PORT, HOST, () => console.log("relay listening on", HOST + ":" + PORT));
