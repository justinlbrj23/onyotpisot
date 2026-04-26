console.log('[solver] content script loaded on', location.href);
const DEBUG = true;

async function getTurnstileMetaFromBg() {
  return chrome.runtime.sendMessage({ __solverGetTurnstileMeta: true });
}

(() => {
  const SOLVE_ONCE_KEY = Symbol('captchaSolveOnce');
  if (window[SOLVE_ONCE_KEY]) return;
  window[SOLVE_ONCE_KEY] = true;

  const usedDelays = new Set();
  const solvedKeys = new Set();
  const jitter = (min = 1200, max = 2400) => {
    for (let i = 0; i < 20; i++) {
      const v = Math.floor(min + Math.random() * (max - min));
      if (!usedDelays.has(v)) {
        usedDelays.add(v);
        return v;
      }
    }
    return Math.floor(min + Math.random() * (max - min));
  };
  const log = (...args) => DEBUG && console.log('[solver]', ...args);

  const overlay = (() => {
    if (!DEBUG) return { show() {}, update() {}, done() {} };
    const el = document.createElement('div');
    el.style.cssText = `
      position:fixed; z-index:2147483647;
      top:8px; right:8px;
      background:#111; color:#0f0;
      font:12px/1.4 monospace;
      padding:8px 10px; border-radius:6px;
      opacity:.9; max-width:40vw;
    `;
    el.textContent = 'solver: idle';
    document.documentElement.appendChild(el);
    return {
      show:   t => (el.textContent = `solver: ${t}`),
      update: t => (el.textContent = `solver: ${t}`),
      done:   t => (el.textContent = `solver: ${t || 'done'}`)
    };
  })();

  const nearestFormFor = el => {
    let node = el;
    for (let i = 0; i < 6 && node; i++) {
      if (node.tagName === 'FORM') return node;
      node = node.parentElement;
    }
    return (
      document.querySelector('form[method="post" i]') ||
      document.querySelector('form')
    );
  };
  const preventedByApp = form => false;

  const sanitizePageUrl = input => {
    try {
      const u = new URL(input);
      u.hash = '';
      for (const k of [...u.searchParams.keys()]) {
        if (k.startsWith('__cf_') || k.startsWith('cf_chl_')) u.searchParams.delete(k);
      }
      return u.toString();
    } catch {
      return input;
    }
  };

  async function waitForWidgetVisible(timeout = 20000) {
    const selectors = [
      'div[data-sitekey][class*="turnstile"]',
      'iframe[src*="challenges.cloudflare.com"]',
      'iframe[src*="/turnstile/if/"]',
      'input[name="cf-turnstile-response"]'
    ];
    const start = Date.now();

    while (Date.now() - start < timeout) {
      // Main DOM check
      for (const sel of selectors) {
        const el = document.querySelector(sel);
        if (el && el.offsetParent !== null && !el.disabled) return true;
      }

      // Iframe check
      const frames = document.querySelectorAll('iframe');
      for (const f of frames) {
        try {
          const doc = f.contentDocument;
          if (!doc) continue;
          for (const sel of selectors) {
            if (doc.querySelector(sel)) return true;
          }
        } catch {} // ignore cross-origin frames
      }

      await new Promise(r => setTimeout(r, 100));
    }

    throw new Error('[solver] Timeout waiting for Turnstile widget');
  }

  const extractTurnstileKeyFromUrl = url => {
    try {
      const path = new URL(url).pathname;
      return path.match(/turnstile\/if\/[^/]+\/[^/]+\/[^/]+\/(0x[0-9A-Za-z]+)/)?.[1] || null;
    } catch { return null; }
  };

  const detectTurnstile = () => {
    const tsScript = [...document.scripts].find(s =>
      (s.src || '').includes('challenges.cloudflare.com/turnstile')
    );
    const tsIframe = [...document.querySelectorAll('iframe')].find(f =>
      (f.src || '').includes('/turnstile/if/')
    );
    const tsWidget = document.querySelector(
      'div[data-sitekey][data-callback], div[data-sitekey][class*="turnstile"]'
    );
    const cfForm =
      document.querySelector('#challenge-form') ||
      document.body?.classList.contains('page-manage-challenge');

    if (tsScript || tsIframe || tsWidget || cfForm) {
      const sitekey =
        tsWidget?.getAttribute('data-sitekey') ||
        (tsIframe && extractTurnstileKeyFromUrl(tsIframe.src)) ||
        (tsScript && extractTurnstileKeyFromUrl(tsScript.src)) || null;
      return sitekey ? { sitekey } : null;
    }
    return null;
  };

  const injectTurnstile = token => {
    const anchor =
      document.querySelector('div[data-sitekey][class*="turnstile"]') ||
      document.querySelector('iframe[src*="turnstile"]') || document.body;
    const form =
      nearestFormFor(anchor) ||
      document.querySelector('form[action*="/cdn-cgi/"]') ||
      document.querySelector('form');

    let input = document.querySelector('input[name="cf-turnstile-response"]');
    if (!input) {
      input = document.createElement('input');
      input.type = 'hidden';
      input.name = 'cf-turnstile-response';
      (form || document.body).appendChild(input);
    }
    input.value = token;

    let alt = document.querySelector(
      'textarea[name="g-recaptcha-response"], input[name="g-recaptcha-response"]'
    );
    if (!alt) {
      alt = document.createElement('textarea');
      alt.name = 'g-recaptcha-response';
      alt.style.display = 'none';
      (form || document.body).appendChild(alt);
    }
    alt.value = token;

    if (form && !preventedByApp(form)) form.submit?.();
    else document.querySelector('button[type="submit"], input[type="submit"]')?.click();
  };

  async function waitForCData(timeoutMs = 5000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const widgetEl = document.querySelector('div[data-sitekey][class*="turnstile"]');
      let cdata = widgetEl?.getAttribute('data-cdata') || null;

      if (!cdata) {
        const tsIframe = document.querySelector('iframe[src*="/turnstile/if/"]');
        if (tsIframe?.src) {
          const parts = tsIframe.src.split('/');
          const idx = parts.indexOf('rcv');
          if (idx !== -1 && parts[idx + 1]) {
            cdata = parts[idx + 1];
            log('[solver] extracted cData from iframe src:', cdata);
          }
        }
      }

      if (!cdata) {
        const meta = await getTurnstileMetaFromBg();
        if (meta?.cdata) {
          cdata = meta.cdata;
          log('[solver] got cData from background meta:', cdata);
        }
      }

      if (cdata) return cdata;
      await new Promise(r => setTimeout(r, 100));
    }
    return null;
  }

  const solveTurnstile = async sitekey => {
    const keyId = `${location.hostname}|turnstile|${sitekey || 'no-dom-key'}`;
    if (solvedKeys.has(keyId)) return;
    solvedKeys.add(keyId);

    overlay.show('solving turnstile…');
    await waitForWidgetVisible();

    const widgetEl = document.querySelector('div[data-sitekey][class*="turnstile"]');
    let cdata    = widgetEl?.getAttribute('data-cdata')    || null;
    let pagedata = widgetEl?.getAttribute('data-pagedata') || null;
    let action   = widgetEl?.getAttribute('data-action')   || null;

    if (!cdata) cdata = await waitForCData(5000);

    const meta = await getTurnstileMetaFromBg();
    if (meta) {
      if (!sitekey && meta.sitekey) sitekey = meta.sitekey;
      if (!pagedata && meta.pagedata) pagedata = meta.pagedata;
      if (!action && meta.action) action = meta.action;
    }

    const payload = {
      method: 'turnstile',
      sitekey,
      pageUrl: sanitizePageUrl(window.location.href.trim()),
      userAgent: navigator.userAgent
    };
    if (action)   payload.action   = action;
    if (cdata)    payload.cdata    = cdata;
    if (pagedata) payload.pagedata = pagedata;

    if (!payload.sitekey) throw new Error('[solver] Missing sitekey for Turnstile');
    if (!payload.cdata)   throw new Error('[solver] Missing cData for Turnstile');
    if (!payload.pagedata) throw new Error('[solver] Missing pageData for Turnstile');

    log('[solver] sending captcha:solve payload', payload);

    const token = await chrome.runtime.sendMessage({
      __solver: true,
      type: 'captcha:solve',
      payload
    });

    if (typeof token === 'string') {
      await new Promise(r => setTimeout(r, jitter(300, 900)));
      injectTurnstile(token);
      overlay.done('token injected');
    } else {
      overlay.update('solve failed, retrying…');
      setTimeout(maybeSolve, 3000);
    }
  };

  const detectRecaptcha = () => {
    const rcScript = [...document.scripts].find(s =>
      (s.src || '').includes('www.google.com/recaptcha')
    );
    const rcIframe = [...document.querySelectorAll('iframe')].find(f =>
      (f.src || '').includes('www.google.com/recaptcha')
    );
    const rcWidget = document.querySelector(
      '.g-recaptcha[data-sitekey], .grecaptcha-badge'
    );
    if (window.grecaptcha || rcScript || rcIframe || rcWidget) {
      const invisible = !!document.querySelector('.grecaptcha-badge');
      const sitekey =
        rcWidget?.getAttribute('data-sitekey') ||
        (rcIframe ? new URL(rcIframe.src).searchParams.get('k') : null);
      return sitekey ? { sitekey, variant: invisible ? 'invisible' : 'v2' } : null;
    }
    return null;
  };

  const injectRecaptcha = token => {
    let ta = document.querySelector('textarea[name="g-recaptcha-response"]');
    if (!ta) {
      ta = document.createElement('textarea');
      ta.name = 'g-recaptcha-response';
      ta.style.display = 'none';
      (document.forms[0] || document.body).appendChild(ta);
    }
    ta.value = token;

    const taEnt = document.querySelector('textarea[name="g-recaptcha-response-100000"]');
    if (taEnt) taEnt.value = token;

    const widget = document.querySelector(
      '.g-recaptcha[data-callback], div[data-callback].g-recaptcha'
    );
    const cbName = widget?.getAttribute('data-callback');
    if (cbName && typeof window[cbName] === 'function') {
      try { window[cbName](token); } catch {}
    }

    const form = nearestFormFor(widget || ta) || document.querySelector('form');
    if (form && !preventedByApp(form)) form.submit?.();
    else document.querySelector('button[type="submit"], input[type="submit"]')?.click();
  };

  const solveRecaptcha = async ({ sitekey, variant }) => {
    const keyId = `${location.hostname}|recaptcha|${sitekey}`;
    if (solvedKeys.has(keyId)) return;
    solvedKeys.add(keyId);

    overlay.show('solving recaptcha…');

    const payload = {
      method: 'recaptcha',
      sitekey,
      pageUrl: sanitizePageUrl(window.location.href.trim()),
      variant
    };

    log('[solver] sending captcha:solve payload', payload);

    const token = await chrome.runtime.sendMessage({
      __solver: true,
      type: 'captcha:solve',
      payload
    });

    if (typeof token === 'string') {
      await new Promise(r => setTimeout(r, jitter(300, 900)));
      injectRecaptcha(token);
      overlay.done('token injected');
    } else {
      overlay.update('solve failed, retrying…');
      setTimeout(maybeSolve, 3000);
    }
  };

  const maybeSolve = async () => {
    let ts = detectTurnstile();

    if (!ts?.sitekey) {
      const meta = await getTurnstileMetaFromBg();
      if (meta?.sitekey) {
        log('[solver] sitekey from network log', meta);
        ts = { sitekey: meta.sitekey, cdata: meta.cdata, pagedata: meta.pagedata, action: meta.action };
      }
    }

    if (ts?.sitekey) return solveTurnstile(ts.sitekey);

    const rc = detectRecaptcha();
    if (rc?.sitekey) return solveRecaptcha(rc);
  };

  const run = () => {
    let kicked = false;
    const kick = () => {
      if (kicked) return;
      kicked = true;
      maybeSolve();
    };
    setTimeout(kick, 50);

    new MutationObserver(() => {
      if (detectTurnstile() || detectRecaptcha()) kick();
    }).observe(document.documentElement, { childList: true, subtree: true });
  };

  run();
})();

const observer = new MutationObserver(() => {
  if (document.querySelector('iframe[src*="challenges.cloudflare.com"]')) {
    chrome.runtime.sendMessage({ challengeDetected: true });
    observer.disconnect();
  }
});

observer.observe(document, { childList: true, subtree: true });
