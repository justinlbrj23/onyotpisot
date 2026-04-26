// solver.worker.js
// Loaded into MV3 service worker via `import './solver.worker.js'`

// ─── CORE SOLVE LOGIC ────────────────────────────────────────────────

// Minimal Turnstile solve function—swap in your real forensic fetch/XHR
async function runSolver({ challengeUrl, sitekey, pageUrl, headers }) {
  console.log('[SolverWorker] running solve:', { sitekey, challengeUrl, pageUrl });

  if (!challengeUrl) {
    throw new Error('Missing challengeUrl');
  }

  // Reconstruct the POST body
  const body = new URLSearchParams();
  body.set('sitekey', sitekey);
  body.set('pageurl', pageUrl);

  // You can spread in any extra form fields you observed
  // body.set('cdata', headers?.cdata || '');

  const resp = await fetch(challengeUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      'Accept': 'text/plain',
      // plus any other minimal headers from your capture
      ...headers
    },
    credentials: 'include',
    body: body.toString()
  });

  if (!resp.ok) {
    throw new Error(`Challenge POST failed: ${resp.status}`);
  }

  const text = await resp.text();
  let token = text.trim();

  // If Turnstile returns JSON like { r: "token" }
  try {
    const json = JSON.parse(text);
    if (json.r) token = json.r;
  } catch {
    // not JSON, assume raw text
  }

  if (!token) {
    throw new Error('No token extracted');
  }

  console.log('[SolverWorker] token:', token);
  return { solved: true, token };
}


// ─── RUNTIME MESSAGE HANDLER ────────────────────────────────────────
//
// Listens for messages from background.js via chrome.runtime.sendMessage.
// Supports: ping  ➞ handshake,  start ➞ solve,  stop ➞ no-op.

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (!msg?.__solver) {
    return; // ignore unrelated messages
  }

  switch (msg.type) {
    case 'ping':
      // handshake ping
      sendResponse({ type: 'handshake_ack' });
      break;

    case 'start':
      console.log('[SolverWorker] start received for sitekey', msg.sitekey);
      (async () => {
        try {
          const result = await runSolver({
            challengeUrl: msg.referer || msg.payload?.challengeUrl || '',
            sitekey: msg.sitekey,
            pageUrl: msg.payload?.pageUrl || '',
            headers: msg.payload?.headers || {}
          });

          // 1) notify background.js of solve result
          chrome.runtime.sendMessage({
            __solver: true,
            type: 'solver_result',
            sitekey: msg.sitekey,
            data: result,
            rowId: msg.rowId
          });

          // 2) reply on the direct channel as well
          sendResponse({ success: true, result });

        } catch (err) {
          console.error('[SolverWorker] solve error:', err);
          chrome.runtime.sendMessage({
            __solver: true,
            type: 'solver_error',
            sitekey: msg.sitekey,
            error: String(err),
            rowId: msg.rowId
          });
          sendResponse({ success: false, error: String(err) });
        }
      })();
      return true; // keep the message channel open for sendResponse()

    case 'stop':
      console.log('[SolverWorker] stop received');
      sendResponse({ type: 'stopped' });
      break;

    default:
      console.warn('[SolverWorker] unknown type:', msg.type);
  }
});