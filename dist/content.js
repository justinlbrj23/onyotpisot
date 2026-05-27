// ===================================================
// === OTMenT v3 — content.js (Behavioral Hybrid v7)
// ===================================================

(function ($) {

  // ===================================================
  // === Runtime / Debug
  // ===================================================

  const DEBUG = Math.random() < 0.12;

  function log(...args) {
    if (DEBUG) console.log(...args);
  }

  function warn(...args) {
    if (DEBUG) console.warn(...args);
  }

  function error(...args) {
    console.error(...args);
  }

  log("[OTMenT] Content script loaded");

  // ===================================================
  // === Randomization Utilities
  // ===================================================

  function rand(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }

  function chance(percent) {
    return Math.random() * 100 < percent;
  }

  function sleep(ms) {
    return new Promise((res) => setTimeout(res, ms));
  }

  async function sleepRandom(min, max) {
    await sleep(rand(min, max));
  }

  async function behavioralDomDelay(mode = "normal") {

    switch (mode) {

      case "micro":
        await sleepRandom(80, 700);
        break;

      case "short":
        await sleepRandom(700, 2500);
        break;

      case "thinking":
        await sleepRandom(2500, 9000);
        break;

      case "reading":
        await sleepRandom(5000, 18000);
        break;

      case "idle":
        await sleepRandom(15000, 60000);
        break;

      default:
        await sleepRandom(500, 4500);
    }
  }

  // ===================================================
  // === Human-like Reading Simulation
  // ===================================================

  async function simulateReadingBehavior() {

    const actions = rand(1, 5);

    for (let i = 0; i < actions; i++) {

      const scrollAmount = rand(200, 1600);

      try {
        window.scrollBy({
          top: scrollAmount,
          behavior: "smooth"
        });
      } catch (_) {}

      await behavioralDomDelay("micro");

      if (chance(25)) {
        await behavioralDomDelay("thinking");
      }
    }

    // Occasional upward correction scroll
    if (chance(30)) {

      try {
        window.scrollBy({
          top: -rand(100, 900),
          behavior: "smooth"
        });
      } catch (_) {}

      await behavioralDomDelay("micro");
    }

    // Rare idle distraction
    if (chance(6)) {
      log("[OTMenT] Simulated distraction");
      await behavioralDomDelay("idle");
    }
  }

  // ===================================================
  // === Wait For DOM Stability
  // ===================================================

  async function waitForDomStability(duration = 1500) {

    return new Promise((resolve) => {

      let timeout;

      const observer = new MutationObserver(() => {

        clearTimeout(timeout);

        timeout = setTimeout(() => {
          observer.disconnect();
          resolve();
        }, duration);

      });

      observer.observe(document.body, {
        childList: true,
        subtree: true,
        attributes: true
      });

      timeout = setTimeout(() => {
        observer.disconnect();
        resolve();
      }, duration);

    });
  }

  // ===================================================
  // === Delayed Handshake
  // ===================================================

  async function sendHandshake() {

    await behavioralDomDelay("micro");

    chrome.runtime.sendMessage({
      action: "contentReady",
      page: location.href,
    });

    log("[OTMenT] Handshake sent");
  }

  // ===================================================
  // === Selector Wait
  // ===================================================

  async function waitForSelector(selector, timeout = 5000) {

    const start = Date.now();

    while (Date.now() - start < timeout) {

      if ($(selector).length > 0) {
        return true;
      }

      await sleep(rand(120, 900));
    }

    warn("[OTMenT] Timeout waiting for selector:", selector);

    return false;
  }

  // ===================================================
  // === UNIVERSAL EXTRACTOR
  // ===================================================

  async function runExtractor(selectors, config, $scope = $("body")) {

    const extracted = {};

    try {

      // Simulate reading before extraction
      if (chance(75)) {
        await simulateReadingBehavior();
      }

      await behavioralDomDelay("thinking");

      for (const sel of selectors || []) {

        if (!sel.selector) continue;

        let value = null;

        try {

          await behavioralDomDelay("micro");

          if (sel.type === "SelectorText") {

            value = sel.multiple
              ? $scope.find(sel.selector)
                  .map((i, el) => $(el).text().trim())
                  .get()
              : $scope.find(sel.selector)
                  .first()
                  .text()
                  .trim() || null;

          } else if (sel.type === "SelectorHTML") {

            value = sel.multiple
              ? $scope.find(sel.selector)
                  .map((i, el) => $(el).html()?.trim())
                  .get()
              : $scope.find(sel.selector)
                  .first()
                  .html()?.trim() || null;

          } else if (
            sel.type === "SelectorElementAttribute" &&
            sel.extractAttribute
          ) {

            value = sel.multiple
              ? $scope.find(sel.selector)
                  .map((i, el) => $(el).attr(sel.extractAttribute))
                  .get()
              : $scope.find(sel.selector)
                  .first()
                  .attr(sel.extractAttribute) || null;

          } else if (sel.type === "SelectorElement") {

            value = sel.multiple
              ? $scope.find(sel.selector)
                  .map((i, el) => $(el).html())
                  .get()
              : $scope.find(sel.selector)
                  .first()
                  .html();

          }

          if (Array.isArray(value)) {
            value = value.filter(v => v && v.trim() !== "");
            if (!value.length) value = null;
          }

          extracted[sel.id] = value;

        } catch (err) {

          warn(`[OTMenT] Selector extraction failed for ${sel.id}:`, err.message);

          extracted[sel.id] = null;
        }
      }

      if (DEBUG && chance(40)) {
        console.table(extracted);
      }

      await behavioralDomDelay("micro");

      chrome.runtime.sendMessage({
        action: "dataExtracted",
        data: extracted || {},
        page: location.href,
      });

      log("[OTMenT] Extraction complete");

    } catch (err) {

      error("[OTMenT] Extraction error:", err);

      chrome.runtime.sendMessage({
        action: "dataError",
        error: err.message,
        page: location.href,
      });
    }
  }

  // ===================================================
  // === MAIN EXECUTION
  // ===================================================

  (async () => {

    try {

      // Random initial delay
      await behavioralDomDelay("short");

      // Wait for rendering stabilization
      await waitForDomStability(rand(1000, 3500));

      // Delayed handshake
      await sendHandshake();

      // Occasional early fake reading
      if (chance(35)) {
        await simulateReadingBehavior();
      }

      chrome.runtime.sendMessage(
        { action: "getConfig" },

        async (config) => {

          try {

            if (!config) {

              error("[OTMenT] No config received");

              chrome.runtime.sendMessage({
                action: "dataError",
                error: "No config received",
                page: location.href,
              });

              return;
            }

            // ============================================
            // === Challenge Detection
            // ============================================

            const titleText = document.title.trim();

            if (/attention|just a moment/i.test(titleText)) {

              warn(`[OTMenT] Challenge detected: ${titleText}`);

              await behavioralDomDelay("thinking");

              chrome.runtime.sendMessage({
                action: "dataError",
                error: `Challenge page detected (${titleText})`,
                page: location.href,
              });

              return;
            }

            // ============================================
            // === Page Classification
            // ============================================

            let isDetail = false;
            let isResult = false;

            const path = location.pathname.toLowerCase();

            try {

              const hasDetailNodes =
                document.querySelector("div[itemtype='https://schema.org/Person']") ||
                document.querySelector("h1");

              isDetail =
                (
                  config.detailSelectors?.some(
                    (sel) =>
                      sel.selector &&
                      document.querySelector(sel.selector)
                  ) ||
                  hasDetailNodes
                ) &&
                path.includes("/name/");

              isResult =
                config.selectors?.some(
                  (sel) =>
                    sel.selector &&
                    document.querySelector(sel.selector)
                ) &&
                path.includes("/address/");

              if (!isDetail && path.includes("/name/")) {
                isDetail = true;
              }

              if (!isResult && path.includes("/address/")) {
                isResult = true;
              }

              log("[OTMenT] Classification:", {
                isResult,
                isDetail,
                path
              });

            } catch (err) {

              warn("[OTMenT] Classification error:", err.message);
            }

            // ============================================
            // === SINGLE SITEMAP MODE
            // ============================================

            if (
              config.selectors?.length &&
              !config.detailSelectors?.length
            ) {

              log("[OTMenT] Single sitemap mode");

              const firstSel =
                config.selectors?.[0]?.selector || "body";

              const ok = await waitForSelector(
                firstSel,
                20000
              );

              if (!ok) {

                chrome.runtime.sendMessage({
                  action: "dataError",
                  error: `Selector ${firstSel} not found`,
                  page: location.href,
                });

                return;
              }

              await runExtractor(config.selectors, config);

              return;
            }

            // ============================================
            // === RESULT PAGE
            // ============================================

            if (isResult) {

              log("[OTMenT] Result page detected");

              await simulateReadingBehavior();

              await behavioralDomDelay("thinking");

              const resultParentSel =
                config.selectors.find(
                  (s) => s.id === "results"
                );

              if (!resultParentSel) {

                warn("[OTMenT] No result selector");

                await runExtractor(config.selectors, config);

                return;
              }

              const ok = await waitForSelector(
                resultParentSel.selector,
                config.requestOptions?.pageLoadTimeoutMs || 20000
              );

              if (!ok) {

                chrome.runtime.sendMessage({
                  action: "dataError",
                  error: "Results selector timeout",
                  page: location.href,
                });

                return;
              }

              await waitForDomStability(rand(800, 3000));

              const $results = $(resultParentSel.selector);

              const Names = [];
              const Hrefs = [];

              $results.each((i, el) => {

                const $el = $(el);

                const nameSel =
                  config.selectors.find(
                    (s) => s.id === "Names"
                  );

                const hrefSel =
                  config.selectors.find(
                    (s) => s.id === "Hrefs"
                  );

                const name = nameSel
                  ? $el.find(nameSel.selector)
                      .first()
                      .text()
                      .trim() || null
                  : null;

                if (name) {
                  Names.push(name);
                }

                const hrefs = hrefSel
                  ? $el.find(hrefSel.selector)
                      .map((j, link) =>
                        $(link).attr(hrefSel.extractAttribute)
                      )
                      .get()
                  : [];

                Hrefs.push(...hrefs.filter(Boolean));
              });

              await behavioralDomDelay("micro");

              chrome.runtime.sendMessage({
                action: "dataExtracted",
                data: { Names, Hrefs },
                page: location.href,
              });

              return;
            }

            // ============================================
            // === DETAIL PAGE
            // ============================================

            if (isDetail) {

              log("[OTMenT] Detail page detected");

              await simulateReadingBehavior();

              await behavioralDomDelay("reading");

              const personTileSel =
                config.detailSelectors?.find(
                  (s) => s.id === "Person-Contact-Tile"
                )?.selector ||
                "div.clearfix.psn-results-container";

              const ok = await waitForSelector(
                personTileSel,
                config.requestOptions?.pageLoadTimeoutMs || 20000
              );

              if (!ok) {
                warn("[OTMenT] Person tile timeout");
              }

              await waitForDomStability(rand(1000, 4000));

              const $tiles =
                document.querySelectorAll(personTileSel);

              if (!$tiles.length) {

                chrome.runtime.sendMessage({
                  action: "dataError",
                  error: "No person tiles found",
                  page: location.href,
                });

                return;
              }

              const extracted = [];

              for (const $tile of $tiles) {

                await behavioralDomDelay("micro");

                const FullnameSel =
                  config.detailSelectors.find(
                    (s) => s.id === "Fullname"
                  )?.selector || "h1";

                const PhoneSel =
                  config.detailSelectors.find(
                    (s) => s.id === "Phone Number + Phone Type"
                  )?.selector ||
                  "span[itemprop='telephone'], div:nth-of-type(6) a";

                const fullName =
                  $tile.querySelector(FullnameSel)
                    ?.textContent
                    .trim() || null;

                const phoneNodes =
                  Array.from(
                    $tile.querySelectorAll(PhoneSel)
                  );

                const phoneList = phoneNodes
                  .map((n) => n.textContent.trim())
                  .filter(Boolean);

                extracted.push({
                  Fullname: fullName,
                  "Phone Number + Phone Type": phoneList,
                });
              }

              await behavioralDomDelay("micro");

              chrome.runtime.sendMessage({
                action: "dataExtracted",
                data: extracted,
                page: location.href,
              });

              return;
            }

            // ============================================
            // === Unknown Page
            // ============================================

            warn("[OTMenT] Unknown page type");

            await behavioralDomDelay("short");

            chrome.runtime.sendMessage({
              action: "dataError",
              error: "Unknown page type",
              page: location.href,
            });

          } catch (err) {

            error("[OTMenT] Main execution error:", err);

            chrome.runtime.sendMessage({
              action: "dataError",
              error: err.message,
              page: location.href,
            });
          }
        }
      );

    } catch (err) {

      error("[OTMenT] Fatal content error:", err);

      chrome.runtime.sendMessage({
        action: "dataError",
        error: err.message,
        page: location.href,
      });
    }

  })();

})(jQuery);
