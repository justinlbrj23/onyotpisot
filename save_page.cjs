const { chromium } = require("playwright");
const fs = require("node:fs/promises");

const CONFIG = {
  targetUrl:
    "https://lee.realtaxdeed.com/index.cfm?zaction=USER&zmethod=CALENDAR",

  parentSelector: "div.CALDAYBOX",
  textSelector: "span.CALTEXT",

  timeoutMs: 60000,
  outputFile: "lee-taxdeed-calendar-links.json",

  // Change to true if you want to watch the browser operate.
  headless: false
};

async function main() {
  const browser = await chromium.launch({
    headless: CONFIG.headless
  });

  const context = await browser.newContext({
    viewport: {
      width: 1440,
      height: 900
    }
  });

  const page = await context.newPage();

  try {
    console.log(`Navigating to: ${CONFIG.targetUrl}`);

    await page.goto(CONFIG.targetUrl, {
      waitUntil: "domcontentloaded",
      timeout: CONFIG.timeoutMs
    });

    await page.waitForSelector(CONFIG.parentSelector, {
      state: "attached",
      timeout: CONFIG.timeoutMs
    });

    // Allow client-side calendar rendering to settle.
    await page.waitForTimeout(1500);

    const results = await page.locator(CONFIG.parentSelector).evaluateAll(
      (calendarBoxes, selectors) => {
        return calendarBoxes
          .map((calendarBox, boxIndex) => {
            const calendarTexts = [
              ...calendarBox.querySelectorAll(selectors.textSelector)
            ]
              .map((element) => element.textContent.trim())
              .filter(Boolean);

            // Ignore this box when all CALTEXT elements are empty.
            if (calendarTexts.length === 0) {
              return null;
            }

            const links = [...calendarBox.querySelectorAll("a[href]")]
              .map((anchor) => {
                const rawHref = anchor.getAttribute("href")?.trim();

                if (!rawHref) {
                  return null;
                }

                let absoluteUrl;

                try {
                  absoluteUrl = new URL(rawHref, document.baseURI).href;
                } catch {
                  return null;
                }

                return {
                  text: anchor.textContent.trim(),
                  href: rawHref,
                  url: absoluteUrl,
                  title: anchor.getAttribute("title")?.trim() || null,
                  target: anchor.getAttribute("target") || null
                };
              })
              .filter(Boolean);

            return {
              boxIndex,
              calendarText: calendarTexts.join(" "),
              links
            };
          })
          .filter(Boolean)
          .filter((result) => result.links.length > 0);
      },
      {
        textSelector: CONFIG.textSelector
      }
    );

    const flatResults = results.flatMap((result) =>
      result.links.map((link) => ({
        boxIndex: result.boxIndex,
        calendarText: result.calendarText,
        ...link
      }))
    );

    const uniqueUrls = [...new Set(flatResults.map((item) => item.url))];

    const output = {
      sourceUrl: CONFIG.targetUrl,
      parsedAt: new Date().toISOString(),
      matchingCalendarBoxes: results.length,
      linkCount: flatResults.length,
      uniqueUrlCount: uniqueUrls.length,
      uniqueUrls,
      results
    };

    await fs.writeFile(
      CONFIG.outputFile,
      JSON.stringify(output, null, 2),
      "utf8"
    );

    console.log("\nExtracted links:");
    console.table(flatResults);

    console.log(`\nUnique URLs found: ${uniqueUrls.length}`);
    console.log(`Saved output to: ${CONFIG.outputFile}`);
  } catch (error) {
    console.error("Unable to parse the calendar page:", error);
    process.exitCode = 1;
  } finally {
    await browser.close();
  }
}

main();
