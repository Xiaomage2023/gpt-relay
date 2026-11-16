// GPT 号池出口中转（alwaysdata 搬家版，逻辑与 Render 现网版完全一致）
// 职责单一：接收站点引擎（Cloudflare Pages Functions）的普通 HTTPS 请求，
// 按请求里给的代理（每账号一个）去访问目标（chatgpt.com），响应流式回传。
// 为什么需要它：Cloudflare Workers 的 fetch 不能走代理、套接字 startTls
// 的应用数据通道在生产运行时不可用；本服务跑在正常 Node 环境出网，无此限制。
// 出网实现（v1.3.6）：默认 impit（Rust 内核，模拟 Chrome 151 的 TLS/JA3 与
// HTTP/2 指纹）——上游边缘按传输层指纹拦截普通 HTTP 客户端，undici 的指纹
// 不像浏览器会被 502/403。环境变量 RELAY_FETCH_MODE=undici 可切回 undici；
// impit 加载失败（原生二进制不可用等）自动回退 undici，实际生效模式见
// /stats 的 mode 字段，绝不让中转整体挂掉。
// 鉴权：请求头 x-relay-key 必须与环境变量 RELAY_KEY 一致。
const http = require("http");
const { fetch, ProxyAgent } = require("undici");

const KEY = process.env.RELAY_KEY || "";
const PORT = Number(process.env.SERVER_PORT || process.env.PORT || 10000);
// 绑定地址：alwaysdata 的 Node 站点由平台分配 IP:端口，部署时在面板核对
// 分配到的地址后用环境变量 HOST/PORT 传入（见《搬家说明.md》）；本地与
// Render 上不设则默认 0.0.0.0，与原行为一致。
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
// 复用其连接池；直连用空串键。实例数随号池规模走，超上限整体清空重建。
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

// 流量计数（v1.3.2）：/fetch 按 proxy 累计——up = 请求正文字节，down = 响应
// 正文字节（流式回传时逐块计数，不缓冲整包；只算应用层正文，不含协议头）。
// proxy 键用掩码形式，掩码规则与后端 _lib.maskProxy 严格对齐（用户名前 4
// 字符 + "***@" + host[:port]），/stats 永不出完整凭据。不带 proxy 的请求不计。
// startedAt = 本进程启动时刻：后端收割时用它判断中转是否重启过（计数清零）。
const startedAt = Date.now();
const proxyStats = new Map();
// 全量请求计数（v1.4.10，后台「中转状态」面板用）：所有通过鉴权的
// /fetch 都计（含不带 proxy 的直连请求——perProxy 只统计带代理的流量，
// 面板要的是中转本身活没活、忙不忙）。随进程重启清零，与 startedAt
// 一起判断是不是同一次运行。
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
