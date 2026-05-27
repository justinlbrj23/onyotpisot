// ===================================================
// === OTMenT v3 — navigator.js (Autonomous Orchestrator)
// ===================================================

console.log("[OTMenT] Navigator initialized. Fetching URLs...");

async function injectScript(tabId, file) {
  try {
    if (typeof chrome !== "undefined" && chrome.scripting) {
      // Chrome MV3
      await chrome.scripting.executeScript({
        target: { tabId },
        files: [file]
      });
      console.log(`[OTMenT] ✅ Script injected via chrome.scripting into tab ${tabId}`);
    } else if (typeof browser !== "undefined" && browser.tabs) {
      // Firefox MV2
      await browser.tabs.executeScript(tabId, { file });
      console.log(`[OTMenT] ✅ Script injected via browser.tabs into tab ${tabId}`);
    } else {
      throw new Error("No supported scripting API available");
    }
  } catch (err) {
    console.error(`[OTMenT] ❌ Script injection failed for tab ${tabId}:`, err.message || err);
    throw err;
  }
}

// ===================================================
// === Background handler for CAPTCHA solve requests
// ===================================================
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.action === "solveCaptcha") {
    const { type, payload } = msg;
    const API_KEY = "a01559936e2950720a2c0126309a824e";

    (async () => {
      try {
        let result = null;

        if (type === "turnstile") {
          // Submit Turnstile task
          const params = new URLSearchParams({
            key: API_KEY,
            method: "turnstile",
            sitekey: payload.sitekey,
            pageurl: payload.pageurl,
            json: 1,
          });
          const res = await fetch("https://2captcha.com/in.php", {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body: params.toString(),
          });
          const data = await res.json();
          if (data.status === 0) throw new Error(data.request);

          // Poll for solution
          result = await pollSolution(data.request, API_KEY);
        }

        if (type === "datadome") {
          if (!payload.pageurl || !payload.captcha_id) {
            throw new Error("Missing pageurl or captcha_id for DataDome");
          }
        
          console.log("[Background] Submitting DataDome:", payload);
        
          const params = new URLSearchParams({
            key: API_KEY,
            method: "datadome",
            pageurl: payload.pageurl,
            captcha_id: payload.captcha_id, // must be cid or captchaId
            json: 1,
          });
        
          const res = await fetch("https://2captcha.com/in.php", {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body: params.toString(),
          });
          const data = await res.json();
          if (data.status === 0) throw new Error(data.request);
        
          const solution = await pollSolution(data.request, API_KEY);
          result = JSON.parse(solution);
        }

        sendResponse({ success: true, result });
      } catch (err) {
        console.error("[Background] CAPTCHA solve failed:", err);
        sendResponse({ success: false, error: err.message });
      }
    })();

    // Important: return true to keep sendResponse async
    return true;
  }
});

// Helper to poll 2Captcha result
async function pollSolution(id, apiKey) {
  while (true) {
    const res = await fetch("https://2captcha.com/res.php?" + new URLSearchParams({
      key: apiKey,
      action: "get",
      id,
      json: 1,
    }));
    const data = await res.json();
    if (data.status === 1) return data.request;
    await new Promise(r => setTimeout(r, 5000));
  }
}

// ===================================================
// === OTMenT v3 — Background Message Listener
// ===================================================
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.action === "getConfig") {
    // Load config asynchronously
    loadConfig()
      .then(cfg => {
        sendResponse(cfg); // send raw config object back
      })
      .catch(err => {
        console.error("[OTMenT] Failed to load config:", err);
        sendResponse(null);
      });
    return true; // keep channel open for async response
  }

  if (msg.action === "resetScraper") {
    // perform reset
    resetFibDelayPool();
    console.log("[OTMenT] Reset triggered from options page");

    // pull cooldown from config.json (requestOptions or rateLimit)
    const { rateLimit = {}, requestOptions = {} } = config || {};
    const cooldownMs = rateLimit.cooldownMs ?? requestOptions.cooldownMs ?? 30000;

    console.log(`⏳ Cooldown triggered — ${cooldownMs}ms`);
    console.log(`[OTMenT] Cooldown sleeping: ${cooldownMs}ms`);

    // sleep before sending response
    sleep(cooldownMs).then(() => {
      sendResponse({ ok: true });
    });

    return true; // keep async channel open for sendResponse
  }

  if (msg.action === "toggleNavigator") {
    config.extensionEnabled = !config.extensionEnabled;
    console.log("[OTMenT] Toggle triggered — now", config.extensionEnabled ? "ON" : "OFF");
    sendResponse({ ok: true, enabled: config.extensionEnabled });
  }
});

// ===============================
// ALARM KEEP-ALIVE MODULE
// ===============================

const KEEP_ALIVE_ALARM = "navigator_keep_alive";

// Create alarm on startup
chrome.runtime.onInstalled.addListener(() => {
  chrome.alarms.create(KEEP_ALIVE_ALARM, { periodInMinutes: 3 });
});

// Listener to keep service worker active
chrome.alarms.onAlarm.addListener(alarm => {
  if (alarm.name === KEEP_ALIVE_ALARM) {
    console.log("⏰ Keep-alive alarm fired — service worker stays warm");
    // Optionally run lightweight diagnostics or rotate cookies here
  }
});

// Fibonacci delay system (no repeats per session, respects cooldown + fastMode)
let fibDelayPool = null;         // current pool
const usedFibDelays = new Set(); // tracks all used delays globally
let requestCounter = 0;          // tracks how many requests have been made

/**
 * Generate a Fibonacci sequence up to a maximum value.
 * Each value is scaled by `scale` for finer control.
 */
function generateFibonacci(max, scale = 1) {
  const fib = [1, 1];
  while (true) {
    const next = fib[fib.length - 1] + fib[fib.length - 2];
    if (next * scale > max) break;
    fib.push(next);
  }
  return fib.map(n => n * scale);
}

/**
 * Initialize or reset the Fibonacci delay pool.
 * In fastMode we shrink the maximum delay automatically.
 */
function initFibDelayPool() {
  const isFast = (typeof config !== "undefined" && config.fastMode === true);

  // fallback if config not loaded yet
  const fibConfig = (typeof config !== "undefined" && config.requestOptions?.fibDelays)
    ? config.requestOptions.fibDelays
    : { max: 30000, scale: 1 };

  const { max = 30000, scale = 1 } = fibConfig;

  // fastMode: shrink max proportionally instead of hard cap
  const reductionFactor = 0.1; // keep 10% of original max
  const effectiveMax = isFast ? Math.max(500, Math.floor(max * reductionFactor)) : max;

  fibDelayPool = generateFibonacci(effectiveMax, scale);

  // Shuffle for randomness
  for (let i = fibDelayPool.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [fibDelayPool[i], fibDelayPool[j]] = [fibDelayPool[j], fibDelayPool[i]];
  }

  console.log(
    `📌 Fibonacci pool initialized — max=${effectiveMax}ms, scale=${scale}, fastMode=${isFast}`
  );
}

/**
 * Return a delay that respects cooldown settings.
 * - Every Nth request → fixed cooldown
 * - Otherwise → Fibonacci-based delay
 */
function getDelay() {
  const { cooldownEvery = 10, cooldownMs = 30000 } = config.requestOptions || {};

  requestCounter++;

  // Apply cooldown every N requests
  if (requestCounter % cooldownEvery === 0) {
    console.log(`⏳ Cooldown triggered — ${cooldownMs}ms`);
    return cooldownMs;
  }

  // Otherwise use Fibonacci delay
  if (!fibDelayPool || fibDelayPool.length === 0) {
    console.warn('Fibonacci pool exhausted, reinitializing...');
    initFibDelayPool();
  }

  while (fibDelayPool.length) {
    const delay = fibDelayPool.pop();
    if (!usedFibDelays.has(delay)) {
      usedFibDelays.add(delay);
      console.log(`🔹 Fibonacci delay selected — ${delay}ms`);
      return delay;
    }
  }

  // All values used → reset
  console.warn('All unique Fibonacci delays have been used, resetting session history...');
  usedFibDelays.clear();
  initFibDelayPool();
  return getDelay(); // retry after reset
}

/**
 * Manual reset of the Fibonacci delay pool and used delays.
 */
function resetFibDelayPool() {
  fibDelayPool = null;
  usedFibDelays.clear();
  requestCounter = 0;
}

/**
 * Return a Fibonacci-based delay.
 * Guarantees no repeats in the current session.
 * Auto-resets the pool if exhausted, skipping already-used values.
 */
function getFibDelay() {
  if (!fibDelayPool || fibDelayPool.length === 0) {
    console.warn('Fibonacci pool exhausted, reinitializing...');
    initFibDelayPool();
  }

  while (fibDelayPool.length) {
    const delay = fibDelayPool.pop();
    if (!usedFibDelays.has(delay)) {
      usedFibDelays.add(delay);
      return delay;
    }
  }

  // All values used → reset
  console.warn('All unique Fibonacci delays have been used, resetting session history...');
  usedFibDelays.clear();
  initFibDelayPool();
  return getFibDelay(); // retry after reset
}

async function resetWithCooldown(cfg) {
  // clear Fibonacci state
  fibDelayPool = null;
  usedFibDelays.clear();
  requestCounter = 0;

  // normalize cooldown values from config
  const { rateLimit = {}, requestOptions = {} } = cfg;
  const cooldownMs = rateLimit.cooldownMs ?? requestOptions.cooldownMs ?? 30000;

  console.log(`⏳ Cooldown triggered — ${cooldownMs}ms`);
  console.log(`[OTMenT] Cooldown sleeping: ${cooldownMs}ms`);

  await sleep(cooldownMs);
}

// --- Globals
let config, serviceAccount, rsaPrivateKey;

// ===================================================
// === Load Config (fetch config.json + service-account.json)
// ===================================================
async function loadConfig() {
  if (config) return config;

  const cfgUrl = chrome.runtime.getURL("config.json");
  config = await (await fetch(cfgUrl)).json();

  const saUrl = chrome.runtime.getURL("service-account.json");
  const saJson = await (await fetch(saUrl)).json();
  serviceAccount = { client_email: saJson.client_email, token_uri: saJson.token_uri };
  rsaPrivateKey = saJson.private_key;

  return config;
}

// ============================================
// === Google Sheets Auth
// ============================================
async function getServiceAccountToken() {
  const now = Math.floor(Date.now() / 1000);
  if (!serviceAccount || !rsaPrivateKey) await loadConfig();

  if (config.tokenCache && config.tokenCache.token && config.tokenCache.expiry > now + 60) {
    return config.tokenCache.token;
  }

  const hdr = { alg: "RS256", typ: "JWT" };
  const pld = {
    iss: serviceAccount.client_email,
    scope: "https://www.googleapis.com/auth/spreadsheets",
    aud: serviceAccount.token_uri,
    iat: now,
    exp: now + 3600,
  };

  const b64 = obj =>
    btoa(JSON.stringify(obj))
      .replace(/=+$/, "")
      .replace(/\+/g, "-")
      .replace(/\//g, "_");

  const unsigned = [b64(hdr), b64(pld)].join(".");
  const signature = await signJwtAssertion(unsigned);
  const assertion = `${unsigned}.${signature}`;

  const tokRes = await fetch(serviceAccount.token_uri, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${assertion}`,
  });

  if (!tokRes.ok) throw new Error(`Token request failed: ${tokRes.status} ${await tokRes.text()}`);
  const { access_token, expires_in } = await tokRes.json();
  config.tokenCache = { token: access_token, expiry: now + expires_in };
  return access_token;
}

async function signJwtAssertion(unsigned) {
  const pem = rsaPrivateKey
    .replace(/-----BEGIN PRIVATE KEY-----/, "")
    .replace(/-----END PRIVATE KEY-----/, "")
    .replace(/\s+/g, "");
  const binaryDer = Uint8Array.from(atob(pem), c => c.charCodeAt(0));

  const key = await crypto.subtle.importKey(
    "pkcs8",
    binaryDer.buffer,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["sign"]
  );

  const enc = new TextEncoder();
  const signature = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, enc.encode(unsigned));

  return btoa(String.fromCharCode(...new Uint8Array(signature)))
    .replace(/=+$/, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
}

// ===================================================
// === OTMenT v3 — getTargetURLs (Row-Aligned Refs + Site)
// ===================================================
async function getTargetURLs(cfg, resumeRow = null, processedUrls = new Set()) {
  const stored = await new Promise(resolve => {
    chrome.storage.local.get(
      ["config_spreadsheetId", "config_sheetName", "config_urlRange", "config_startRow"],
      result => resolve(result || {})
    );
  });

  cfg.spreadsheetId = stored.config_spreadsheetId || cfg.spreadsheetId;
  cfg.sheetName     = stored.config_sheetName     || cfg.sheetName;
  cfg.urlRange      = stored.config_urlRange      || cfg.urlRange;
  cfg.startRow      = stored.config_startRow      || cfg.startRow || 2;

  // ===========================================================
  // SAFE RANGE PARSING TO AVOID CRASHES
  // ===========================================================
  let startRowMatch = cfg.urlRange ? cfg.urlRange.match(/\d+/) : null;
  const startRow = startRowMatch ? Number(startRowMatch[0]) : Number(cfg.startRow) || 2;

  const spreadsheetId = cfg.spreadsheetId;
  const sheetName     = cfg.sheetName;
  const urlRange      = cfg.urlRange;

  const token = await getServiceAccountToken();

  // ===========================================================
  // FETCH URL COLUMN (T2:T)
  // ===========================================================
  const urlRes = await fetch(
    `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${encodeURIComponent(urlRange)}?majorDimension=ROWS`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const { values: urlVals = [] } = await urlRes.json();

  const endRow = startRow + urlVals.length - 1;

  // ===========================================================
  // FETCH F–L (7 columns)
  // ===========================================================
  const refRange = `${sheetName}!F${startRow}:L${endRow}`;
  const refRes = await fetch(
    `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${encodeURIComponent(refRange)}?majorDimension=ROWS`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const { values: refVals = [] } = await refRes.json();

  // ===========================================================
  // FETCH SITE COLUMN (B)
  // ===========================================================
  const siteRange = `${sheetName}!B${startRow}:B${endRow}`;
  const siteRes = await fetch(
    `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${encodeURIComponent(siteRange)}?majorDimension=ROWS`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const { values: siteVals = [] } = await siteRes.json();

  // ===========================================================
  // FETCH LOG COLUMN (U)
  // ===========================================================
  const logRange = `${sheetName}!U${startRow}:U${endRow}`;
  const logRes = await fetch(
    `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${encodeURIComponent(logRange)}?majorDimension=ROWS`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const { values: logVals = [] } = await logRes.json();

  // ===========================================================
  // BUILD RESULTS
  // ===========================================================
  let results = [];

  for (let i = 0; i < urlVals.length; i++) {
    const url = (urlVals[i] && urlVals[i][0])?.trim();
    const row = startRow + i;

    if (!url) continue;

    // Skip rows with logs already present
    const rowLog = logVals[i] || [];
    const hasLogs = rowLog.some(cell => String(cell || "").trim());
    if (hasLogs) continue;

    // Ensure F–L always length 7
    const refs = refVals[i] || [];
    while (refs.length < 7) refs.push("");

    results.push({
      url,
      row,
      refs,
      siteVal: siteVals[i]?.[0] || ""
    });
  }

  // ===========================================================
  // APPLY RESUME (if restarting)
  // ===========================================================
  if (resumeRow != null) {
    results = results.filter(e => e.row >= resumeRow);
  }

  // ===========================================================
  // REMOVE DUPLICATES
  // ===========================================================
  const seen = new Set();
  results = results.filter(e => {
    if (processedUrls.has(e.url) || seen.has(e.url)) return false;
    seen.add(e.url);
    return true;
  });

  return results;
}

async function logToSheet(detailDataArray, sourceRow, cfg, siteValFromEntry = null) {
  try {
    const token = await getServiceAccountToken();
    const { spreadsheetId, detailSheetName, sheetName } = cfg;

    if (!detailDataArray?.length) {
      console.warn("[OTMenT] No detail data to log");
      return;
    }

    // --- Use preloaded siteVal if provided, else fetch once
    let siteVal = siteValFromEntry;
    if (!siteVal) {
      const siteRange = `${sheetName}!B${sourceRow}:B${sourceRow}`;
      const siteRes = await fetch(
        `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${encodeURIComponent(siteRange)}?majorDimension=ROWS`,
        { headers: { Authorization: `Bearer ${token}` } }
      );
      const { values: siteVals = [] } = await siteRes.json();
      siteVal = siteVals[0]?.[0] || "";
    }

    // --- Build rows for each candidate
    const rows = detailDataArray.map(entry => {
      const fullName = entry.Fullname?.trim() || "";
      const phones = entry["Phone Number + Phone Type"] || [];

      // Split name
      const nameParts = fullName.split(" ").filter(Boolean);
      const firstName = nameParts.shift() || "";
      const lastName = nameParts.join(" ") || "";

      // Limit to 5 phone pairs
      const pairs = phones.slice(0, 5).map(p => {
        const match = p.match(/(\(?\d{3}\)?[ -]?\d{3}-\d{4})\s*(.*)?/);
        return match ? [match[1].trim(), (match[2] || "").trim()] : [p.trim(), ""];
      });
      while (pairs.length < 5) pairs.push(["", ""]);

      return [
        siteVal,
        firstName,
        lastName,
        pairs[0][0], pairs[0][1],
        pairs[1][0], pairs[1][1],
        pairs[2][0], pairs[2][1],
        pairs[3][0], pairs[3][1],
        pairs[4][0], pairs[4][1]
      ];
    });

    // --- Append all rows in one request
    const appendURL = `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${encodeURIComponent(detailSheetName + "!A2")}:append?valueInputOption=USER_ENTERED`;
    const appendBody = { values: rows };

    const appendRes = await fetch(appendURL, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${token}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(appendBody)
    });

    if (appendRes.ok) {
      console.log(`[OTMenT] Logged ${rows.length} candidate(s) to "${detailSheetName}" successfully (Row ${sourceRow})`);
    } else {
      console.warn(`[OTMenT] Failed to log (Row ${sourceRow})`, await appendRes.text());
    }

  } catch (err) {
    console.error("[OTMenT] logToSheet() error:", err);
  }
}

// ============================================
// === Levenshtein-based Matchmaking
// ============================================

// ─────────────────────────────
// Enhanced similarity options
const NAME_SIM_OPTIONS = {
  dropSingleLetterTokens: true,
  useTokenOverlapWeight: 0.4,
  useFullStringWeight: 0.5,
  useFirstLastBoost: 0.2
};

// Memory‑efficient Levenshtein (two‑row)
function levenshtein(a, b) {
  a = String(a || '');
  b = String(b || '');
  if (a === b) return 0;
  const n = a.length, m = b.length;
  if (!n) return m;
  if (!m) return n;
  if (n < m) [a, b] = [b, a]; // ensure b shorter

  const prev = new Array(b.length + 1);
  const cur = new Array(b.length + 1);

  for (let j = 0; j <= b.length; j++) prev[j] = j;
  for (let i = 1; i <= a.length; i++) {
    cur[0] = i;
    const ai = a.charCodeAt(i - 1);
    for (let j = 1; j <= b.length; j++) {
      const cost = ai === b.charCodeAt(j - 1) ? 0 : 1;
      const del = prev[j] + 1;
      const ins = cur[j - 1] + 1;
      const sub = prev[j - 1] + cost;
      cur[j] = del < ins ? (del < sub ? del : sub) : (ins < sub ? ins : sub);
    }
    for (let k = 0; k <= b.length; k++) prev[k] = cur[k];
  }
  return prev[b.length];
}

// Stronger normalization
function _normalizeForName(s) {
  if (!s) return '';
  const cleaned = String(s)
    .replace(/[^\p{L}\s]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
  const toks = cleaned.split(' ').filter(t => t.length);
  return NAME_SIM_OPTIONS.dropSingleLetterTokens
    ? toks.filter(t => t.length > 1).join(' ')
    : toks.join(' ');
}

function _nameTokens(s) {
  const n = _normalizeForName(s);
  return n ? n.split(' ') : [];
}

// Combined similarity
function similarity(a, b) {
  a = String(a || '').trim();
  b = String(b || '').trim();
  if (!a && !b) return 1;
  if (!a || !b) return 0;

  const aNorm = _normalizeForName(a);
  const bNorm = _normalizeForName(b);
  if (aNorm && aNorm === bNorm) return 1;

  const maxLen = Math.max(aNorm.length, bNorm.length);
  const fullSim = maxLen ? 1 - levenshtein(aNorm, bNorm) / maxLen : 0;

  const aT = _nameTokens(a);
  const bT = _nameTokens(b);
  const aSet = new Set(aT);
  let common = 0;
  for (const t of bT) if (aSet.has(t)) common++;
  const tokenOverlap = (aT.length + bT.length)
    ? (2 * common) / (aT.length + bT.length)
    : 0;

  const firstLastMatch =
    (aT.length && bT.length &&
      (aT[0] === bT[0] ||
       aT[aT.length - 1] === bT[bT.length - 1] ||
       aT[0] === bT[bT.length - 1] ||
       aT[aT.length - 1] === bT[0])) ? 1 : 0;

  const score =
    (NAME_SIM_OPTIONS.useTokenOverlapWeight * tokenOverlap) +
    (NAME_SIM_OPTIONS.useFullStringWeight * fullSim) +
    (NAME_SIM_OPTIONS.useFirstLastBoost * firstLastMatch);

  return Math.max(0, Math.min(1, score));
}

// Backwards‑compatible wrapper
function matchScoreWithExplanation(a, b) {
  if (!a || !b) return { score: 0, explanation: "Empty input" };
  const aNorm = _normalizeForName(a);
  const bNorm = _normalizeForName(b);
  const score = similarity(a, b);
  const dist = levenshtein(aNorm, bNorm);
  return {
    score,
    explanation: `Levenshtein distance: ${dist}, Normalized: "${aNorm}" vs "${bNorm}", Token overlap + first/last heuristics applied`
  };
}

// ─────────────────────────────
// Normalize hrefs helper
function normalizeHrefs(rawHrefs = [], baseUrl = '') {
  if (!Array.isArray(rawHrefs)) return [];
  const out = [];
  for (const raw of rawHrefs) {
    if (typeof raw !== 'string') continue;
    const href = raw.trim();
    if (!href) continue;
    try {
      const abs = new URL(href, baseUrl).href;
      if (abs.startsWith('http')) out.push(abs);
    } catch {
      console.warn('[OTMenT] Skipping invalid href:', href);
    }
  }
  return out;
}

function normalizeAndSort(s) {
  if (!s) return "";
  return s
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .filter(Boolean)
    .sort()
    .join(" ");
}

// ============================================
// === Write result back to Google Sheet
// ============================================
async function writeResult(row, resultText) {
  const token = await getServiceAccountToken();
  const { spreadsheetId, sheetName } = config;
  const range = `${sheetName}!U${row}`;
  const body = { values: [[resultText]] };

  const res = await fetch(
    `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${encodeURIComponent(range)}?valueInputOption=RAW`,
    {
      method: "PUT",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }
  );
  if (!res.ok) console.warn(`[OTMenT] Failed to write result for row ${row}:`, await res.text());
}

// ===================================================
// === Wait for Data Extraction Event (Challenge-Aware v2.4, Cross-Browser) ===
// ===================================================
async function waitForExtraction(tabId, options = {}) {
  const {
    timeout = 60_000,
    actions = ["contentReady", "dataExtracted", "dataError"],
    once = true,
    debug = false,
    retryOnChallenge = true,
  } = options;

  // Pick correct API references
  const tabsApi = typeof browser !== "undefined" ? browser.tabs : chrome.tabs;
  const runtimeApi = typeof browser !== "undefined" ? browser.runtime : chrome.runtime;

  // --------------------------------------------------
  // STEP 1 — Read the page title (cross-browser safe)
  // --------------------------------------------------
  const readTitle = () =>
    new Promise((resolve, reject) => {
      try {
        if (typeof chrome !== "undefined" && chrome.scripting) {
          // Chrome MV3
          chrome.scripting.executeScript(
            {
              target: { tabId },
              func: () => document.title,
            },
            (results) => {
              if (chrome.runtime.lastError) return reject(chrome.runtime.lastError);
              resolve(results && results[0] ? results[0].result : "");
            }
          );
        } else if (tabsApi && tabsApi.executeScript) {
          // Firefox MV2
          tabsApi.executeScript(tabId, { code: "document.title;" })
            .then(results => resolve(results ? results[0] : ""))
            .catch(err => reject(err));
        } else {
          reject(new Error("No supported script injection API available"));
        }
      } catch (err) {
        reject(err);
      }
    });

  try {
    const title = await readTitle();
    if (debug) console.log(`[waitForExtraction] Title before extraction: "${title}"`);

    if (title && /attention|just a moment/i.test(title)) {
      console.warn(`[waitForExtraction] Challenge title detected ("${title}") — reloading tab...`);
      await tabsApi.reload(tabId);
      await new Promise(r => setTimeout(r, 6000)); // wait 6s for Cloudflare
    }
  } catch (err) {
    console.warn(`[waitForExtraction] Title read failed:`, err);
  }

  // --------------------------------------------------
  // STEP 2 — Wait for extraction OR challenge retry
  // --------------------------------------------------
  return new Promise((resolve, reject) => {
    let settled = false;

    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      runtimeApi.onMessage.removeListener(listener);
      reject(new Error(`Timeout waiting for extraction on tab ${tabId}`));
    }, timeout);

    async function listener(msg, sender) {
      if (!sender.tab || sender.tab.id !== tabId) return;
      if (!actions.includes(msg.action)) return;

      if (debug) console.log(`[waitForExtraction] Received:`, msg);
      clearTimeout(timer);

      // --------------------------------------------------
      // Handshake logging
      // --------------------------------------------------
      if (msg.action === "contentReady") {
        console.log(`[waitForExtraction] Handshake confirmed: content script ready in tab ${tabId}`);
        // Do not resolve yet — keep listening for extraction
        return;
      }

      // --------------------------------------------------
      // CHALLENGE HANDLING
      // --------------------------------------------------
      if (
        msg.action === "dataError" &&
        retryOnChallenge &&
        /attention|just a moment|challenge/i.test(msg.error || "")
      ) {
        console.warn("[waitForExtraction] ⚠️ Challenge reported — retrying...");
        runtimeApi.onMessage.removeListener(listener);

        try {
          await tabsApi.reload(tabId);

          // Wait until title is no longer "attention"
          for (let i = 0; i < 15; i++) {
            let title = "";
            try {
              title = await readTitle();
            } catch (_) {}
            if (!/attention|just a moment/i.test(title || "")) break;
            await new Promise(r => setTimeout(r, 1000));
          }

          // Re‑inject content script
          if (typeof chrome !== "undefined" && chrome.scripting) {
            await new Promise((resolveInject, rejectInject) => {
              chrome.scripting.executeScript(
                { target: { tabId }, files: ["content.js"] },
                () => {
                  if (chrome.runtime.lastError) return rejectInject(chrome.runtime.lastError);
                  resolveInject();
                }
              );
            });
          } else if (tabsApi && tabsApi.executeScript) {
            await tabsApi.executeScript(tabId, { file: "content.js" });
          }

          console.log("[waitForExtraction] ✅ Retrying extraction after challenge recovery...");

          const retryResult = await waitForExtraction(tabId, {
            timeout,
            actions,
            once,
            debug,
            retryOnChallenge: false,
          });

          settled = true;
          resolve(retryResult);
          return;
        } catch (err) {
          settled = true;
          reject(new Error(`Retry after challenge failed: ${err.message}`));
          return;
        }
      }

      // --------------------------------------------------
      // NORMAL PATH
      // --------------------------------------------------
      if (once) runtimeApi.onMessage.removeListener(listener);
      settled = true;

      resolve({
        success: msg.action === "dataExtracted",
        data: msg.data ?? null,
        page: msg.page ?? null,
        raw: msg,
      });
    }

    runtimeApi.onMessage.addListener(listener);
    if (debug) console.log(`[waitForExtraction] Listening on tab ${tabId}...`);
  });
}

// ============================================
// === Core Navigation Loop (calibrated, row-aligned siteVal)
// ============================================

// --- Sleep helpers ---
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fibSleep() {
  const delay = getFibDelay();
  console.log(`⏱ fibSleep waiting ${delay}ms`);
  await sleep(delay);
}

// Safe tab creation with retries + diagnostics
// background.js (top of file)

// Cross‑browser safe tabs API reference
const tabsApi = typeof browser !== "undefined" ? browser.tabs : chrome.tabs;

// Safe tab creation with retries
async function createTabFirefoxSafe(url, retries = 4, baseDelayMs = 1000) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      console.log(`[OTMenT] Attempt ${attempt}/${retries} — creating tab: ${url}`);
      const tab = await tabsApi.create({ url, active: false });

      if (tab && tab.id) {
        console.log(`[OTMenT] ✅ Tab created successfully (id=${tab.id})`);
        return tab;
      }
    } catch (err) {
      console.warn(`[OTMenT] ❌ Tab creation failed on attempt ${attempt}:`, err.message || err);
    }

    if (attempt < retries) {
      const waitMs = baseDelayMs * attempt + Math.floor(Math.random() * 250);
      console.log(`[OTMenT] Waiting ${waitMs}ms before retrying...`);
      await sleep(waitMs);
    }
  }

  const errorMsg = `[OTMenT] 🚫 Cannot create tab after ${retries} attempts: ${url}`;
  console.error(errorMsg);
  throw new Error(errorMsg);
}

// Queue manager: limits concurrent tab creation
async function processUrlsWithPool(urls, concurrencyLimit = 5) {
  let active = 0;
  let index = 0;

  return new Promise((resolve, reject) => {
    const results = [];
    let finished = 0;

    async function next() {
      if (index >= urls.length && active === 0) {
        resolve(results);
        return;
      }

      while (active < concurrencyLimit && index < urls.length) {
        const url = urls[index++];
        active++;

        createTabFirefoxSafe(url)
          .then(tab => {
            results.push({ url, tabId: tab.id });
          })
          .catch(err => {
            results.push({ url, error: err.message });
          })
          .finally(() => {
            active--;
            finished++;
            console.log(`[OTMenT] Progress: ${finished}/${urls.length} processed`);
            next();
          });
      }
    }

    next();
  });
}

// ================================================
// === OTMenT v3 — Navigator Core (Stable v4)
// ================================================

async function runNavigator() {
  const cfg = await loadConfig();
  const entries = await getTargetURLs(cfg);

  if (!entries.length) {
    console.warn("[OTMenT] No URLs to process — aborting.");
    return;
  }

  if (!fibDelayPool) initFibDelayPool();

  console.log(`[OTMenT] Processing ${entries.length} URL(s)...`);

  const tabsApi =
    typeof browser !== "undefined"
      ? browser.tabs
      : chrome.tabs;

  for (const [index, entry] of entries.entries()) {
    let tabId = null;

    console.log(
      `[OTMenT] [${index + 1}/${entries.length}] Row ${entry.row}`
    );

    try {
      // ===================================================
      // CREATE TAB
      // ===================================================
      const tab = await createTabFirefoxSafe(entry.url);
      tabId = tab.id;

      // ===================================================
      // WAIT FOR EXTRACTION
      // ===================================================
      const extraction = await waitForExtraction(tabId, {
        timeout: cfg.requestOptions?.pageLoadTimeoutMs || 30000,
      });

      if (!extraction?.success) {
        throw new Error("Extraction failed");
      }

      const extracted = extraction.data || {};

      // ===================================================
      // RESULT PAGE FLOW
      // ===================================================
      if (entry.url.includes("/address/")) {

        const names = Array.isArray(extracted.Names)
          ? extracted.Names
          : [];

        const hrefs = normalizeHrefs(
          extracted.Hrefs || [],
          cfg.baseUrl
        );

        console.log("[OTMenT] Result candidates:", {
          names: names.length,
          hrefs: hrefs.length,
        });

        if (!names.length || !hrefs.length) {
          await writeResult(entry.row, "NO RESULTS");
          continue;
        }

        // ===============================================
        // BUILD MATCHES
        // ===============================================
        const matches = [];

        const maxLen = Math.min(names.length, hrefs.length);

        for (let i = 0; i < maxLen; i++) {
          const candidateName = names[i];
          const href = hrefs[i];

          if (!candidateName || !href) continue;

          for (const ref of entry.refs) {
            if (!ref) continue;

            const result = matchScoreWithExplanation(
              candidateName,
              ref
            );

            console.log(
              `[OTMenT] "${candidateName}" vs "${ref}" = ${result.score.toFixed(2)}`
            );

            if (
              result.score >=
              ((cfg.matchThreshold || 0.4) * 0.6)
            ) {
              matches.push({
                href,
                ref,
                candidateName,
                score: result.score,
              });
            }
          }
        }

        // ===============================================
        // NO MATCHES
        // ===============================================
        if (!matches.length) {
          await writeResult(entry.row, "NO MATCHES");
          continue;
        }

        // ===============================================
        // FOLLOW MATCHES
        // ===============================================
        for (const match of matches) {

          console.log(
            `[OTMenT] Following match: ${match.candidateName}`
          );

          await tabsApi.update(tabId, {
            url: match.href,
          });

          await sleep(3000);

          // reinject content script
          await injectScript(tabId, "content.js");

          const detailResult = await waitForExtraction(tabId, {
            timeout: 30000,
          });

          const detailTiles = Array.isArray(detailResult?.data)
            ? detailResult.data
            : detailResult?.data
            ? [detailResult.data]
            : [];

          if (!detailTiles.length) {
            console.warn("[OTMenT] No detail tiles");
            continue;
          }

          // ===========================================
          // DETAIL MATCHING
          // ===========================================
          const validMatches = [];

          for (const tile of detailTiles) {
            for (const ref of entry.refs) {

              if (!ref || !tile?.Fullname) continue;

              const result = matchScoreWithExplanation(
                tile.Fullname,
                ref
              );

              if (
                result.score >=
                ((cfg.matchThreshold || 0.4) * 0.6)
              ) {
                validMatches.push(tile);

                console.log(
                  `[OTMenT] Detail match: ${tile.Fullname}`
                );
              }
            }
          }

          // dedupe
          const deduped = [];
          const seen = new Set();

          for (const item of validMatches) {
            const key = JSON.stringify(item);

            if (!seen.has(key)) {
              seen.add(key);
              deduped.push(item);
            }
          }

          if (deduped.length) {
            await logToSheet(
              deduped,
              entry.row,
              cfg,
              entry.siteVal
            );

            await writeResult(
              entry.row,
              `MATCHED (${deduped.length})`
            );
          } else {
            await writeResult(
              entry.row,
              "NO DETAIL MATCH"
            );
          }

          await fibSleep();
        }
      }

      // ===================================================
      // DIRECT DETAIL PAGE
      // ===================================================
      else if (entry.url.includes("/name/")) {

        const detailTiles = Array.isArray(extracted)
          ? extracted
          : [extracted];

        const validMatches = [];

        for (const tile of detailTiles) {

          if (!tile?.Fullname) continue;

          for (const ref of entry.refs) {

            if (!ref) continue;

            const result = matchScoreWithExplanation(
              tile.Fullname,
              ref
            );

            if (
              result.score >=
              ((cfg.matchThreshold || 0.4) * 0.6)
            ) {
              validMatches.push(tile);

              console.log(
                `[OTMenT] Direct detail match: ${tile.Fullname}`
              );
            }
          }
        }

        // dedupe
        const deduped = [];
        const seen = new Set();

        for (const item of validMatches) {
          const key = JSON.stringify(item);

          if (!seen.has(key)) {
            seen.add(key);
            deduped.push(item);
          }
        }

        if (deduped.length) {

          await logToSheet(
            deduped,
            entry.row,
            cfg,
            entry.siteVal
          );

          await writeResult(
            entry.row,
            `MATCHED (${deduped.length})`
          );

        } else {

          await writeResult(
            entry.row,
            "NO DETAIL MATCH"
          );
        }
      }

      // ===================================================
      // UNKNOWN
      // ===================================================
      else {

        console.warn(
          "[OTMenT] Unknown URL type:",
          entry.url
        );

        await writeResult(
          entry.row,
          "UNKNOWN PAGE TYPE"
        );
      }

      // ===================================================
      // COOLDOWN
      // ===================================================
      const delay = getDelay();

      console.log(
        `[OTMenT] Sleeping ${delay}ms`
      );

      await sleep(delay);

    } catch (err) {

      console.error(
        `[OTMenT] Fatal row error (${entry.row}):`,
        err
      );

      await writeResult(
        entry.row,
        `ERROR: ${err.message}`
      );

    } finally {

      if (tabId) {
        try {
          await tabsApi.remove(tabId);
        } catch (_) {}
      }

      const fibDelay = getFibDelay();

      console.log(
        `[OTMenT] Fibonacci sleep: ${fibDelay}ms`
      );

      await sleep(fibDelay);
    }
  }

  console.log("[OTMenT] Navigator complete.");
}

// ===================================================
// === Startup Hook
// ===================================================
chrome.runtime.onStartup.addListener(() => {
  console.log("[OTMenT] Navigator initialized. Fetching URLs...");
  runNavigator().catch(err => console.error("[OTMenT] Navigator failed:", err));
});

// Optional: also run when extension is installed/reloaded
chrome.runtime.onInstalled.addListener(() => {
  console.log("[OTMenT] Extension installed — starting navigator.");
  runNavigator().catch(err => console.error("[OTMenT] Navigator failed:", err));
});
