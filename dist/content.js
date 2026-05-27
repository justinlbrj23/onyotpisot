// ===================================================
// === OTMenT v3 — content.js (Behavioral Hybrid v8)
// === Optimized ~20% Faster
// ===================================================

(function ($) {

  // ===================================================
  // === Runtime / Debug
  // ===================================================

  const DEBUG = Math.random() < 0.08;

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
    return (Math.random() * (max - min + 1) + min) | 0;
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

  // ===================================================
  // === OPTIMIZED Behavioral Delays
  // === Reduced ~20-30%
  // ===================================================

  async function behavioralDomDelay(mode = "normal") {

    switch (mode) {

      case "micro":
        await sleepRandom(40, 450);
        break;

      case "short":
        await sleepRandom(450, 1800);
        break;

      case "thinking":
        await sleepRandom(1200, 4200);
        break;

      case "reading":
        await sleepRandom(1200, 3800);
        break;

      case "idle":
        await sleepRandom(2500, 9000);
        break;

      default:
        await sleepRandom(300, 2200);
    }
  }

  // ===================================================
  // === Faster Human-like Reading Simulation
  // ===================================================

  async function simulateReadingBehavior() {

    const actions = rand(1, 3);

    for (let i = 0; i < actions; i++) {

      try {
        window.scrollBy({
          top: rand(250, 1200),
          behavior: "smooth"
        });
      } catch (_) {}

      await behavioralDomDelay("micro");

      if (chance(18)) {
        await behavioralDomDelay("short");
      }
    }

    // Reduced upward correction frequency
    if (chance(18)) {

      try {
        window.scrollBy({
          top: -rand(100, 600),
          behavior: "smooth"
        });
      } catch (_) {}

      await behavioralDomDelay("micro");
    }

    // Rare idle distraction
    if (chance(2)) {
      log("[OTMenT] Simulated distraction");
      await behavioralDomDelay("idle");
    }
  }

  // ===================================================
  // === Faster DOM Stability Wait
  // ===================================================

  async function waitForDomStability(duration = 900) {

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
        subtree: true
      });

      timeout = setTimeout(() => {
        observer.disconnect();
        resolve();
      }, duration);

    });
  }

  // ===================================================
  // === Faster Handshake
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
  // === Optimized Selector Wait
  // ===================================================

  async function waitForSelector(selector, timeout = 5000) {

    // Fast immediate check
    if (document.querySelector(selector)) {
      return true;
    }

    const start = Date.now();

    while (Date.now() - start < timeout) {

      if (document.querySelector(selector)) {
        return true;
      }

      await sleep(rand(60, 300));
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

      // Reduced behavior simulation
      if (chance(45)) {
        await simulateReadingBehavior();
      }

      await behavioralDomDelay("short");

      for (const sel of selectors || []) {

        if (!sel.selector) continue;

        let value = null;

        try {

          if (chance(35)) {
            await behavioralDomDelay("micro");
          }

          if (sel.type === "SelectorText") {

            value = sel.multiple
              ? $scope.find(sel.selector)
                  .map((i, el) => el.textContent.trim())
                  .get()
              : $scope.find(sel.selector)
                  .first()[0]
                  ?.textContent
                  ?.trim() || null;

          } else if (sel.type === "SelectorHTML") {

            value = sel.multiple
              ? $scope.find(sel.selector)
                  .map((i, el) => el.innerHTML.trim())
                  .get()
              : $scope.find(sel.selector)
                  .first()[0]
                  ?.innerHTML
                  ?.trim() || null;

          } else if (
            sel.type === "SelectorElementAttribute" &&
            sel.extractAttribute
          ) {

            value = sel.multiple
              ? $scope.find(sel.selector)
                  .map((i, el) => el.getAttribute(sel.extractAttribute))
                  .get()
              : $scope.find(sel.selector)
                  .first()[0]
                  ?.getAttribute(sel.extractAttribute) || null;

          } else if (sel.type === "SelectorElement") {

            value = sel.multiple
              ? $scope.find(sel.selector)
                  .map((i, el) => el.innerHTML)
                  .get()
              : $scope.find(sel.selector)
                  .first()[0]
                  ?.innerHTML || null;
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

      if (DEBUG && chance(20)) {
        console.table(extracted);
      }

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

      // Faster initial delay
      await behavioralDomDelay("micro");

      // Faster DOM stabilization
      await waitForDomStability(rand(500, 1600));

      // Handshake
      await sendHandshake();

      // Reduced fake reading
      if (chance(20)) {
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

              chrome.runtime.sendMessage({
                action: "dataError",
                error: `Challenge page detected (${titleText})`,
                page: location.href,
              });

              return;
            }

            // ============================================
            // === Faster Page Classification
            // ============================================

            const path = location.pathname.toLowerCase();

            const isDetail =
              path.includes("/name/");

            const isResult =
              path.includes("/address/");

            log("[OTMenT] Classification:", {
              isResult,
              isDetail,
              path
            });

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
                12000
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

              if (chance(30)) {
                await simulateReadingBehavior();
              }

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
                config.requestOptions?.pageLoadTimeoutMs || 15000
              );

              if (!ok) {

                chrome.runtime.sendMessage({
                  action: "dataError",
                  error: "Results selector timeout",
                  page: location.href,
                });

                return;
              }

              await waitForDomStability(rand(300, 1200));

              const results =
                document.querySelectorAll(resultParentSel.selector);

              const Names = [];
              const Hrefs = [];

              const nameSel =
                config.selectors.find(
                  (s) => s.id === "Names"
                );

              const hrefSel =
                config.selectors.find(
                  (s) => s.id === "Hrefs"
                );

              for (const el of results) {

                const name =
                  nameSel
                    ? el.querySelector(nameSel.selector)
                        ?.textContent
                        ?.trim()
                    : null;

                if (name) {
                  Names.push(name);
                }

                if (hrefSel) {

                  const hrefNodes =
                    el.querySelectorAll(hrefSel.selector);

                  for (const link of hrefNodes) {

                    const href =
                      link.getAttribute(
                        hrefSel.extractAttribute
                      );

                    if (href) {
                      Hrefs.push(href);
                    }
                  }
                }
              }

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

              if (chance(35)) {
                await simulateReadingBehavior();
              }

              const personTileSel =
                config.detailSelectors?.find(
                  (s) => s.id === "Person-Contact-Tile"
                )?.selector ||
                "div.clearfix.psn-results-container";

              const ok = await waitForSelector(
                personTileSel,
                config.requestOptions?.pageLoadTimeoutMs || 15000
              );

              if (!ok) {
                warn("[OTMenT] Person tile timeout");
              }

              await waitForDomStability(rand(400, 1400));

              const tiles =
                document.querySelectorAll(personTileSel);

              if (!tiles.length) {

                chrome.runtime.sendMessage({
                  action: "dataError",
                  error: "No person tiles found",
                  page: location.href,
                });

                return;
              }

              const extracted = [];

              const FullnameSel =
                config.detailSelectors.find(
                  (s) => s.id === "Fullname"
                )?.selector || "h1";

              const PhoneSel =
                config.detailSelectors.find(
                  (s) => s.id === "Phone Number + Phone Type"
                )?.selector ||
                "span[itemprop='telephone'], div:nth-of-type(6) a";

              for (const tile of tiles) {

                if (chance(20)) {
                  await behavioralDomDelay("micro");
                }

                const fullName =
                  tile.querySelector(FullnameSel)
                    ?.textContent
                    ?.trim() || null;

                const phoneNodes =
                  tile.querySelectorAll(PhoneSel);

                const phoneList = [];

                for (const node of phoneNodes) {

                  const txt =
                    node.textContent?.trim();

                  if (txt) {
                    phoneList.push(txt);
                  }
                }

                extracted.push({
                  Fullname: fullName,
                  "Phone Number + Phone Type": phoneList,
                });
              }

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
