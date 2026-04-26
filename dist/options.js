document.addEventListener("DOMContentLoaded", async () => {
  const apiKeyInput   = document.getElementById("apiKey");
  const apiModeSelect = document.getElementById("apiMode");
  const saveBtn       = document.getElementById("save");
  const resetBtn      = document.getElementById("reset");
  const updateBtn     = document.getElementById("update");
  const toggleBtn     = document.getElementById("toggle");
  const statusEl      = document.getElementById("status");

  const sheetIdInput  = document.getElementById("sheetId");
  const sheetNameInput= document.getElementById("sheetName");
  const urlRangeInput = document.getElementById("urlRange");
  const startRowInput = document.getElementById("startRow");

  const MIN_KEY_LENGTH = 32;

  const storageGet = (k) => new Promise(r => chrome.storage.local.get(k, r));
  const storageSet = (o) => new Promise((r, j) =>
    chrome.storage.local.set(o, () =>
      chrome.runtime.lastError ? j(chrome.runtime.lastError) : r()
    )
  );

  /* --------------------------------------------------
   * LOAD DEFAULTS FROM config.json + STORAGE OVERRIDES
   * -------------------------------------------------- */
  const defaults = await fetch(chrome.runtime.getURL("config.json")).then(r => r.json());
  const stored   = await storageGet(null);
  const cfg      = { ...defaults, ...stored };

  apiKeyInput.value    = cfg.solver_api_key || "";
  apiModeSelect.value  = cfg.solver_api_mode || "json";
  sheetIdInput.value   = cfg.config_spreadsheetId || "";
  sheetNameInput.value = cfg.config_sheetName || "";
  urlRangeInput.value  = cfg.config_urlRange || "";
  startRowInput.value  = cfg.config_startRow || 2;

  if (cfg.extension_enabled === undefined) {
    await storageSet({ extension_enabled: true });
  }

  updateToggleUI(cfg.extension_enabled !== false);

  /* --------------------------------------------------
   * VALIDATION
   * -------------------------------------------------- */
  apiKeyInput.addEventListener("input", () => {
    saveBtn.disabled = apiKeyInput.value.trim().length < MIN_KEY_LENGTH;
    clearStatus();
  });

  /* --------------------------------------------------
   * SAVE CONFIG
   * -------------------------------------------------- */
  saveBtn.addEventListener("click", async () => {
    const key = apiKeyInput.value.trim();

    if (key.length < MIN_KEY_LENGTH) {
      showStatus("⚠️ API key must be at least 32 characters", "orange");
      return;
    }

    if (!sheetIdInput.value.trim()) {
      showStatus("⚠️ Spreadsheet ID required", "orange");
      return;
    }

    saveBtn.disabled = true;
    showStatus("💾 Saving…");

    try {
      await storageSet({
        solver_api_key: key,
        solver_api_mode: apiModeSelect.value,
        config_spreadsheetId: sheetIdInput.value.trim(),
        config_sheetName: sheetNameInput.value.trim(),
        config_urlRange: urlRangeInput.value.trim(),
        config_startRow: Number(startRowInput.value) || 2
      });

      chrome.runtime.sendMessage({ action: "START_NAVIGATOR" });

      showStatus("✅ Saved & navigator triggered", "green");
    } catch (err) {
      console.error(err);
      showStatus("❌ Failed to save", "red");
    } finally {
      saveBtn.disabled = false;
    }
  });

  /* --------------------------------------------------
   * RESET
   * -------------------------------------------------- */
  resetBtn.addEventListener("click", async () => {
    if (!confirm("Reset scraper configuration?")) return;

    await storageSet({
      solver_api_key: "",
      solver_api_mode: "json",
      config_spreadsheetId: "",
      config_sheetName: "",
      config_urlRange: "",
      config_startRow: 2
    });

    chrome.runtime.sendMessage({ action: "resetScraper" });
    showStatus("♻️ Reset complete", "green");
  });

  updateBtn.addEventListener("click", () => {
    showStatus("🔄 Reloading extension…", "blue");
    chrome.runtime.reload();
  });

  toggleBtn.addEventListener("click", async () => {
    const { extension_enabled } = await storageGet("extension_enabled");
    const newState = !(extension_enabled !== false);
    await storageSet({ extension_enabled: newState });
    updateToggleUI(newState);
    showStatus(newState ? "✅ Extension ON" : "⏻ Extension OFF", newState ? "green" : "red");
  });

  function updateToggleUI(state) {
    toggleBtn.textContent = state ? "⏻ Turn OFF" : "⏻ Turn ON";
  }

  function showStatus(msg, color) {
    statusEl.textContent = msg;
    statusEl.style.color = color || "";
    statusEl.style.opacity = "1";
    setTimeout(() => (statusEl.style.opacity = "0"), 5000);
  }

  function clearStatus() {
    statusEl.textContent = "";
    statusEl.style.opacity = "1";
    statusEl.style.color = "";
  }
});