// security.js ────────────────────────────────────────────────────────────────
// Defence-in-depth middleware for the edge API.
//
// WHAT THIS DOES AND DOES NOT COVER
//
// This file hardens the transport and adds a coarse gate in front of the API.
// It is NOT a substitute for authentication, which this system does not yet
// have: AuthContext on the frontend is a localStorage mock, there are no
// server-side sessions, no cookies and no tokens. Several standard controls
// (CSRF tokens, session invalidation on password change, account lockout,
// secure cookie flags) are therefore not implementable here — they presuppose
// a session layer. They are listed in the thesis as future work rather than
// silently omitted.
//
// The gap that matters most is tenant SELECTION. RLS guarantees that once a
// tenant is chosen, a handler physically cannot read another tenant's rows —
// that part is enforced in PostgreSQL and is solid. But the tenant is chosen by
// an x-tenant-id header that nothing verifies, so any caller who can reach the
// API can name any tenant. API_KEY below narrows that from "anyone on the
// network" to "anyone holding the key", which is a real improvement for a
// LAN-deployed edge server but is not per-user authorisation.

import crypto from 'node:crypto';

// ── Security headers ────────────────────────────────────────────────────────
// Written out rather than pulling in helmet: every header here is one we can
// justify, and a dependency-free implementation is easier to audit and to
// explain than a library whose defaults change between majors.
export function securityHeaders(req, res, next) {
  // Don't let a browser second-guess our Content-Type. The API returns JSON;
  // sniffing it as HTML is how a stored string becomes a stored XSS.
  res.setHeader('X-Content-Type-Options', 'nosniff');

  // No framing. There's no legitimate reason to embed this API or dashboard in
  // an iframe, and refusing removes clickjacking entirely.
  res.setHeader('X-Frame-Options', 'DENY');

  // Don't leak the dashboard URL (which contains the tenant slug) to any
  // third-party host a page might reference.
  res.setHeader('Referrer-Policy', 'no-referrer');

  // Deny hardware APIs we never use.
  res.setHeader('Permissions-Policy',
    'geolocation=(), microphone=(), camera=(), payment=(), usb=()');

  // HSTS only over TLS. Sending it on plain HTTP is meaningless, and on a LAN
  // bench where the edge server may be reached by IP over http:// it would
  // pin a host the operator can then no longer reach.
  if (req.secure || req.get('x-forwarded-proto') === 'https') {
    res.setHeader('Strict-Transport-Security',
      'max-age=31536000; includeSubDomains');
  }

  // The API serves JSON only; a restrictive CSP costs nothing and blocks a
  // response ever being rendered as an active document.
  res.setHeader('Content-Security-Policy',
    "default-src 'none'; frame-ancestors 'none'; base-uri 'none'");

  // Don't advertise Express and its version.
  res.removeHeader('X-Powered-By');

  next();
}

// ── CORS ────────────────────────────────────────────────────────────────────
// `cors()` with no arguments reflects any Origin and is effectively "any
// website may read this API using the visitor's network position" — which on a
// LAN-deployed device means any page the operator visits could reach the
// dashboard API. Allowlist instead.
export function corsOptions() {
  const configured = (process.env.CORS_ORIGINS ?? '')
    .split(',').map((s) => s.trim()).filter(Boolean);

  // Sensible defaults for the two ways this is actually reached.
  const defaults = [
    'http://localhost:5173',
    'http://127.0.0.1:5173',
  ];
  const allowed = configured.length ? configured : defaults;

  return {
    origin(origin, cb) {
      // No Origin header: curl, health checks, server-to-server. Not a browser,
      // so the same-origin policy isn't what's protecting anything here.
      if (!origin) return cb(null, true);
      if (allowed.includes(origin)) return cb(null, true);
      return cb(new Error(`origin '${origin}' not allowed`));
    },
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'x-tenant-id', 'x-api-key'],
    maxAge: 600,
  };
}

// ── Security event log ──────────────────────────────────────────────────────
// Deliberately a separate, greppable prefix rather than mixed into normal
// request logging: during an incident you want "what was refused and from
// where", not the whole access log.
const recent = [];
const MAX_EVENTS = 200;

export function logSecurityEvent(kind, req, detail = '') {
  const entry = {
    at: new Date().toISOString(),
    kind,
    ip: req?.ip ?? req?.socket?.remoteAddress ?? 'unknown',
    method: req?.method,
    path: req?.originalUrl ?? req?.url,
    tenant: req?.get?.('x-tenant-id') ?? null,
    detail,
  };
  recent.push(entry);
  if (recent.length > MAX_EVENTS) recent.shift();
  console.warn(`[security] ${kind} ${entry.method} ${entry.path} from ${entry.ip}` +
               (detail ? ` — ${detail}` : ''));
}

export function getSecurityEvents() {
  return { count: recent.length, events: recent.slice(-50) };
}

// ── Rate limiting ───────────────────────────────────────────────────────────
// In-memory and per-process, which is the right scope here: the edge server is
// a single Node process on one machine. A shared store would be needed only if
// this were horizontally scaled, which the architecture explicitly does not do
// (the edge server is the on-site authority, one per site).
//
// The goal is to blunt automated scraping and brute force, not to enforce a
// quota — legitimate dashboard polling sits far below these limits.
// `max` is a getter, not a number, deliberately: reading process.env at module
// scope freezes the limit at import time, which silently ignores any value the
// process sets afterwards (dotenv loaded later, a test harness, a config
// module). A limiter that quietly runs at its default instead of the configured
// value is worse than one that fails loudly.
function makeLimiter({ windowMs, max, name }) {
  const limitOf = () => (typeof max === 'function' ? max() : max);
  const hits = new Map();                       // key -> { count, resetAt }

  // Sweep expired buckets so an attacker cycling source addresses cannot grow
  // the map without bound.
  const sweep = setInterval(() => {
    const now = Date.now();
    for (const [k, v] of hits) if (v.resetAt <= now) hits.delete(k);
  }, windowMs);
  sweep.unref?.();

  return function limiter(req, res, next) {
    const key = req.ip ?? req.socket?.remoteAddress ?? 'unknown';
    const now = Date.now();
    let bucket = hits.get(key);

    if (!bucket || bucket.resetAt <= now) {
      bucket = { count: 0, resetAt: now + windowMs };
      hits.set(key, bucket);
    }
    bucket.count += 1;

    const limit = limitOf();
    const remaining = Math.max(0, limit - bucket.count);
    res.setHeader('RateLimit-Limit', String(limit));
    res.setHeader('RateLimit-Remaining', String(remaining));
    res.setHeader('RateLimit-Reset', String(Math.ceil((bucket.resetAt - now) / 1000)));

    if (bucket.count > limit) {
      res.setHeader('Retry-After', String(Math.ceil((bucket.resetAt - now) / 1000)));
      logSecurityEvent('rate_limit', req, `${name}: ${bucket.count}/${limit}`);
      return res.status(429).json({ error: 'too many requests — slow down' });
    }
    next();
  };
}

// Reads: generous. A dashboard polling every 3s is 20/min, and several open
// tabs must not trip this.
export const readLimiter = makeLimiter({
  windowMs: 60_000,
  max: () => Number(process.env.RATE_LIMIT_READ ?? 600),
  name: 'read',
});

// Writes: much tighter. No human renames a board 60 times a minute, and this is
// the surface where a loop does real damage (formula churn, command flooding).
export const writeLimiter = makeLimiter({
  windowMs: 60_000,
  max: () => Number(process.env.RATE_LIMIT_WRITE ?? 60),
  name: 'write',
});

// Downlink commands: tightest of all. Each one publishes to the broker and
// actuates physical hardware — a pump or a valve. Flooding this is not a
// data-integrity problem, it's an equipment problem.
export const commandLimiter = makeLimiter({
  windowMs: 60_000,
  max: () => Number(process.env.RATE_LIMIT_COMMAND ?? 30),
  name: 'command',
});

// ── API key gate ────────────────────────────────────────────────────────────
// Optional and OFF by default so a bench bring-up isn't blocked, but strongly
// recommended for any deployment reachable beyond localhost.
//
// This is coarse: it authenticates the CALLER, not the user, and it does not
// establish which tenants that caller may act for. It exists because the
// alternative — an unauthenticated API where the tenant is whatever the client
// says it is — is worse. Per-user authorisation is the real fix and is future
// work.
export function apiKeyGate(req, res, next) {
  const expected = process.env.API_KEY;
  if (!expected) return next();                 // not configured — bench mode

  // Health is left open so monitoring doesn't need the key.
  if (req.path === '/health') return next();

  const provided = req.get('x-api-key') ?? '';

  // Constant-time compare. A naive === leaks the key one character at a time to
  // anyone who can measure response timing.
  const a = Buffer.from(provided);
  const b = Buffer.from(expected);
  const ok = a.length === b.length && crypto.timingSafeEqual(a, b);

  if (!ok) {
    logSecurityEvent('bad_api_key', req);
    return res.status(401).json({ error: 'unauthorized' });
  }
  next();
}

// ── Tenant guard ────────────────────────────────────────────────────────────
// Fails closed. The specific bug this replaces: POST /formulas resolved its
// tenant as `tenant || tenantOf(req) || Object.keys(store.tenants)[0]` — so a
// request that named no tenant at all silently wrote into whichever tenant
// happened to be first in the map. That is a cross-tenant write caused by an
// omission rather than an attack.
export function requireTenant(getTenant) {
  return function guard(req, res, next) {
    const slug = getTenant(req);
    if (!slug) {
      logSecurityEvent('missing_tenant', req);
      return res.status(400).json({
        error: 'tenant is required — pass x-tenant-id or ?tenant=',
      });
    }
    next();
  };
}
