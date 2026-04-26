// solver.helper.js
// Minimal 2Captcha integration for Turnstile/ReCAPTCHA

export class TwoCaptchaProvider {
    constructor(apiKey) {
      this.apiKey = apiKey;
      this.baseUrl = "https://2captcha.com";
    }
  
    async createTask(sitekey, url) {
      const res = await fetch(`${this.baseUrl}/in.php`, {
        method: "POST",
        body: new URLSearchParams({
          key: this.apiKey,
          method: "turnstile",
          sitekey,
          pageurl: url,
          json: 1
        })
      });
      const data = await res.json();
      if (data.status !== 1) throw new Error("2Captcha task creation failed");
      return data.request;
    }
  
    async getResult(requestId, retries = 30, delayMs = 5000) {
      for (let i = 0; i < retries; i++) {
        await new Promise(r => setTimeout(r, delayMs));
        const res = await fetch(`${this.baseUrl}/res.php?key=${this.apiKey}&action=get&id=${requestId}&json=1`);
        const data = await res.json();
        if (data.status === 1) return data.request;
        if (data.request !== "CAPCHA_NOT_READY") throw new Error("2Captcha error: " + data.request);
      }
      throw new Error("2Captcha timeout");
    }
  }
  
  export async function solveIfPresent(tabId, { provider }) {
    // Ask content script for sitekey
    const [{ result: sitekey }] = await chrome.scripting.executeScript({
      target: { tabId },
      func: () => {
        const el = document.querySelector("div[data-sitekey]");
        return el ? el.getAttribute("data-sitekey") : null;
      }
    });
  
    if (!sitekey) return { ok: false, reason: "NO_SITEKEY" };
  
    const [tab] = await chrome.tabs.get(tabId).then(t => [t]);
    const taskId = await provider.createTask(sitekey, tab.url);
    const token = await provider.getResult(taskId);
  
    // Inject token into page
    await chrome.scripting.executeScript({
      target: { tabId },
      func: (tok) => {
        const input = document.createElement("input");
        input.type = "hidden";
        input.name = "cf-turnstile-response";
        input.value = tok;
        document.body.appendChild(input);
      },
      args: [token]
    });
  
    return { ok: true, token };
  }