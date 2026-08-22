const fs = require('fs');
const { chromium } = require('playwright');

const START_URL = process.env.START_URL ||
  'https://www.publicnoticeoregon.com/(S(lps2vkd2pk5xopff0o1dvpzj))/Search.aspx';

const OUTPUT_JSON = 'notices.json';
const OUTPUT_TEXT = 'notices.txt';
const TIMEOUT = 120000;

const config = {
  fastMode: process.env.FAST_MODE === 'true',
  requestOptions: {
    fibDelays: {
      max: Number(process.env.FIB_MAX_DELAY_MS) || 34000,
      scale: Number(process.env.FIB_DELAY_SCALE) || 1000,
      minimum: Number(process.env.MIN_DELAY_MS) || 2000
    },
    cooldownEvery: Number(process.env.COOLDOWN_EVERY) || 10,
    cooldownMs: Number(process.env.COOLDOWN_MS) || 30000
  }
};

function cleanText(value = '') {
  return String(value)
    .replace(/\u00a0/g, ' ')
    .replace(/\r/g, '')
    .replace(/[ \t]+/g, ' ')
    .replace(/ *\n */g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function normalizeIdentifier(value = '') {
  return cleanText(value)
    .replace(/^[#:\-\s]+|[#:\-\s]+$/g, '')
    .toUpperCase();
}

function extractNoticeIdentifier(text = '') {
  const patterns = [
    /\bCase\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bT\.?S\.?\s*(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bTrustee(?:'s)?\s+Sale\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bSale\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bFile\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bLoan\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i
  ];

  const normalized = cleanText(text);
  for (const pattern of patterns) {
    const match = normalized.match(pattern);
    if (match?.[1]) return normalizeIdentifier(match[1]);
  }
  return '';
}

function extractTrustor(text = '') {
  const patterns = [
    /\b(?:original\s+)?trustor(?:s)?\s*[:,]\s*(.+?)(?=\s*,?\s*(?:whose|who|as|beneficiary|trustee|recorded|the))/i,
    /\bTrustor(?:s)?\s*[:,]\s*([^.;]+)/i,
    /\bdeed\s+made\s+by\s+(.+?)\s+as\s+grantor\b/i
  ];

  const normalized = cleanText(text);
  for (const pattern of patterns) {
    const match = normalized.match(pattern);
    if (match?.[1]) {
      return cleanText(match[1])
        .replace(/^[,;:\-\s]+|[,;:\-\s]+$/g, '');
    }
  }
  return '';
}

function fallbackIdentifier(notice) {
  const source = [notice.publication, notice.publishedDate, notice.text].join('|');
  let hash = 2166136261;

  for (let index = 0; index < source.length; index += 1) {
    hash ^= source.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }

  return `NOTICE-${(hash >>> 0).toString(16).toUpperCase().padStart(8, '0')}`;
}

/*
 * Fibonacci courtesy-pacing system.
 *
 * Every returned delay is unique for the complete browser session. The used
 * delay history is never automatically cleared. When a shuffled pool is
 * exhausted, the next generation expands the range and applies a generation
 * offset. Cooldown durations also receive unique offsets.
 *
 * This is for courteous rate limiting. It does not bypass CAPTCHA or access
 * controls. Challenge pages are saved and the run stops for manual review.
 */
let fibDelayPool = null;
const usedFibDelays = new Set();
let requestCounter = 0;
let fibPoolGeneration = 0;
let previousDelay = null;

function shuffleArray(values) {
  for (let index = values.length - 1; index > 0; index -= 1) {
    const randomIndex = Math.floor(Math.random() * (index + 1));
    [values[index], values[randomIndex]] = [values[randomIndex], values[index]];
  }
  return values;
}

function generateFibonacci(maximum, scale = 1000, minimum = 0) {
  const fibonacci = [1, 2];

  while (true) {
    const next = fibonacci.at(-1) + fibonacci.at(-2);
    if (next * scale > maximum) break;
    fibonacci.push(next);
  }

  return [...new Set(
    fibonacci
      .map(number => number * scale)
      .filter(delay => delay >= minimum && delay <= maximum)
  )];
}

function getFibonacciConfiguration() {
  const settings = config.requestOptions?.fibDelays || {};
  const maximum = Number(settings.max) || 34000;
  const scale = Number(settings.scale) || 1000;
  const minimum = Number(settings.minimum) || 2000;
  const effectiveMaximum = config.fastMode
    ? Math.max(minimum, Math.floor(maximum * 0.1))
    : maximum;

  return { maximum, effectiveMaximum, scale, minimum };
}

function initFibDelayPool() {
  const { effectiveMaximum, scale, minimum } = getFibonacciConfiguration();
  const expansion = fibPoolGeneration * Math.max(scale * 2, 1000);
  const generationMaximum = effectiveMaximum + expansion;
  const generationOffset = fibPoolGeneration * 101;

  fibDelayPool = generateFibonacci(generationMaximum, scale, minimum)
    .map(delay => delay + generationOffset)
    .filter(delay => !usedFibDelays.has(delay));

  if (fibDelayPool.length === 0) {
    const fallbackBase = Math.max(generationMaximum, minimum);
    fibDelayPool = Array.from(
      { length: 20 },
      (_, index) => fallbackBase + generationOffset + index + 1
    ).filter(delay => !usedFibDelays.has(delay));
  }

  shuffleArray(fibDelayPool);

  console.log(
    `Fibonacci pool initialized: generation=${fibPoolGeneration}, ` +
    `max=${generationMaximum}ms, scale=${scale}, ` +
    `fastMode=${config.fastMode}, available=${fibDelayPool.length}`
  );

  fibPoolGeneration += 1;
}

function getUniqueFibDelay() {
  while (!fibDelayPool || fibDelayPool.length === 0) {
    initFibDelayPool();
  }

  while (fibDelayPool.length > 0) {
    const delay = fibDelayPool.pop();
    if (!usedFibDelays.has(delay) && delay !== previousDelay) {
      usedFibDelays.add(delay);
      previousDelay = delay;
      return delay;
    }
  }

  initFibDelayPool();
  return getUniqueFibDelay();
}

function getUniqueCooldownDelay() {
  const configured = Number(config.requestOptions?.cooldownMs) || 30000;
  let candidate = configured + requestCounter;

  while (usedFibDelays.has(candidate) || candidate === previousDelay) {
    candidate += 1;
  }

  usedFibDelays.add(candidate);
  previousDelay = candidate;
  return candidate;
}

function getDelay() {
  const cooldownEvery = Number(config.requestOptions?.cooldownEvery) || 10;
  requestCounter += 1;

  if (cooldownEvery > 0 && requestCounter % cooldownEvery === 0) {
    const delay = getUniqueCooldownDelay();
    console.log(`Cooldown triggered: ${delay}ms`);
    return delay;
  }

  const delay = getUniqueFibDelay();
  console.log(`Fibonacci delay selected: ${delay}ms`);
  return delay;
}

async function courtesyWait(page, reason = 'the next navigation') {
  const delay = getDelay();
  console.log(
    `[pacing request ${requestCounter}] Waiting ` +
    `${(delay / 1000).toFixed(3)}s before ${reason}.`
  );
  await page.waitForTimeout(delay);
}

function increaseCourtesyDelay() {
  fibDelayPool = null;
  fibPoolGeneration += 1;
  console.log('Transient failure detected; next wait uses a later pool.');
}

function resetCourtesyDelay() {
  // Intentionally retained for compatibility. Run-wide history is not reset.
}

function resetFibDelayPool() {
  fibDelayPool = null;
  usedFibDelays.clear();
  requestCounter = 0;
  fibPoolGeneration = 0;
  previousDelay = null;
}

async function detectAccessChallenge(page) {
  return page.evaluate(() => {
    const combined = `${document.title || ''}\n${document.body?.innerText || ''}`;
    const indicators = [
      /\bcaptcha\b/i,
      /verify you are human/i,
      /human verification/i,
      /security check/i,
      /unusual traffic/i,
      /automated requests/i,
      /temporarily blocked/i,
      /too many requests/i,
      /rate limit/i,
      /access denied/i
    ];

    const matched = indicators.find(pattern => pattern.test(combined));
    const captchaElement = document.querySelector([
      'iframe[src*="captcha" i]',
      'iframe[title*="captcha" i]',
      '[class*="captcha" i]',
      '[id*="captcha" i]',
      'textarea[name="g-recaptcha-response"]',
      '[data-sitekey]'
    ].join(', '));

    return {
      detected: Boolean(matched || captchaElement),
      indicator: matched?.source || (captchaElement ? 'captcha DOM element' : '')
    };
  });
}

async function stopIfAccessChallenge(page, location) {
  const challenge = await detectAccessChallenge(page);
  if (!challenge.detected) return;

  const safeLocation = String(location || 'unknown')
    .replace(/[^a-z0-9_-]+/gi, '-')
    .replace(/^-+|-+$/g, '')
    .toLowerCase();

  await saveDebug(page, `challenge-${safeLocation}`);
  throw new Error(
    `Access challenge detected during ${location}. ` +
    `Indicator: ${challenge.indicator}. The scraper stopped without bypassing it.`
  );
}

function getViewButtons(page) {
  return page.locator([
    '[id^="ctl00_ContentPlaceHolder1_WSExtendedGridNP1_GridView1_ctl"][id$="_btnView2"]',
    '#ctl00_ContentPlaceHolder1_WSExtendedGridNP1_GridView1 [id$="_btnView2"]',
    '[id$="_btnView2"]'
  ].join(', '));
}

async function getPageInfo(page) {
  const text = await page.locator('body').innerText();
  const match = text.match(/Page\s+(\d+)\s+of\s+(\d+)\s+Pages?/i);
  return match ? { current: Number(match[1]), total: Number(match[2]) } : null;
}

async function saveDebug(page, baseName) {
  fs.writeFileSync(`${baseName}.html`, await page.content(), 'utf8');
  await page.screenshot({ path: `${baseName}.png`, fullPage: true });
}

async function waitForResults(page, expectedPage = null) {
  try {
    await page.waitForFunction(
      expected => {
        const text = document.body?.innerText || '';
        const match = text.match(/Page\s+(\d+)\s+of\s+(\d+)\s+Pages?/i);
        const hasViews = document.querySelectorAll('[id$="_btnView2"]').length > 0;
        return hasViews && (
          expected === null ||
          (match && Number(match[1]) === expected)
        );
      },
      expectedPage,
      { timeout: TIMEOUT }
    );
  } catch (error) {
    const label = expectedPage === null ? 'unknown' : expectedPage;
    await saveDebug(page, `wait-timeout-page-${label}`);
    console.error('waitForResults URL:', page.url());
    console.error('VIEW controls:', await getViewButtons(page).count());
    throw error;
  }

  await page.waitForTimeout(500);
}

async function extractSummaryAroundViewButton(button) {
  return button.evaluate(element => {
    const normalize = value => String(value || '')
      .replace(/\u00a0/g, ' ')
      .replace(/\r/g, '')
      .replace(/[ \t]+/g, ' ')
      .replace(/ *\n */g, '\n')
      .replace(/\n{3,}/g, '\n\n')
      .trim();

    const datePattern = /\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b/i;
    const row = element.closest('tr');
    const parts = [];

    if (row) {
      parts.push(normalize(row.innerText));
      let sibling = row.nextElementSibling;
      for (let index = 0; sibling && index < 2; index += 1) {
        parts.push(normalize(sibling.innerText));
        if (/click\s+['"]?view['"]?\s+to\s+open/i.test(parts.at(-1))) break;
        sibling = sibling.nextElementSibling;
      }
    }

    const text = normalize(parts.join('\n'));
    const dateMatch = text.match(datePattern);
    let publication = '';

    if (dateMatch) {
      publication = text.slice(0, dateMatch.index)
        .split('\n')
        .map(line => line.trim())
        .filter(Boolean)
        .filter(line => !/^view$/i.test(line))
        .at(-1) || '';
    }

    return {
      publication,
      publishedDate: dateMatch?.[0] || '',
      text
    };
  });
}

async function waitForDetail(page, oldUrl, oldText) {
  await page.waitForFunction(
    ({ previousUrl, previousText }) => {
      const text = document.body?.innerText || '';
      const changed = text.trim() !== previousText.trim();
      return changed && (
        window.location.href !== previousUrl ||
        /Public Notice Detail|(?:TRUSTEE(?:'S)?\s+)?NOTICE OF SALE/i.test(text)
      );
    },
    { previousUrl: oldUrl, previousText: oldText },
    { timeout: TIMEOUT }
  );

  await page.waitForTimeout(500);
}

async function extractFullNotice(page) {
  const result = await page.evaluate(() => {
    const normalize = value => String(value || '')
      .replace(/\u00a0/g, ' ')
      .replace(/\r/g, '')
      .replace(/[ \t]+/g, ' ')
      .replace(/ *\n */g, '\n')
      .replace(/\n{3,}/g, '\n\n')
      .trim();

    const salePattern = /(?:TRUSTEE(?:'S)?\s+)?NOTICE OF SALE/i;
    const datePattern = /\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b/i;
    const selectors = [
      '[id*="NoticeText"]', '[id*="FullText"]', '[id*="NoticeDetail"]',
      '[class*="notice-text"]', '[class*="notice-detail"]',
      'article', 'main', 'section', 'td', 'div'
    ].join(', ');

    const candidates = Array.from(document.querySelectorAll(selectors))
      .map(element => normalize(element.innerText))
      .filter(text => salePattern.test(text) && text.length >= 100)
      .sort((left, right) => right.length - left.length);

    let text = candidates[0] || normalize(document.body?.innerText || '');
    const start = text.search(salePattern);
    if (start >= 0) text = text.slice(start);

    text = text
      .replace(/\.{3}\s*click\s+['"]?view['"]?\s+to\s+open\s+the\s+full\s+text\.?/gi, '')
      .trim();

    const body = normalize(document.body?.innerText || '');
    const dateMatch = body.match(datePattern);
    let publication = '';

    if (dateMatch) {
      publication = body.slice(0, dateMatch.index)
        .split('\n')
        .map(line => line.trim())
        .filter(Boolean)
        .filter(line => !/^(view|back|previous|next|search results)$/i.test(line))
        .at(-1) || '';
    }

    return {
      publication,
      publishedDate: dateMatch?.[0] || '',
      text: normalize(text)
    };
  });

  return {
    publication: cleanText(result.publication),
    publishedDate: cleanText(result.publishedDate),
    text: cleanText(result.text)
  };
}

async function isExpectedResultsPage(page, expectedPage) {
  const info = await getPageInfo(page);
  return Boolean(
    info &&
    info.current === expectedPage &&
    await getViewButtons(page).count() > 0
  );
}

async function returnToResultsPage(page, resultsUrl, expectedPage) {
  await courtesyWait(page, `returning to results page ${expectedPage}`);

  const historyResult = await page.goBack({
    waitUntil: 'domcontentloaded',
    timeout: 30000
  }).catch(() => null);

  if (historyResult) {
    await stopIfAccessChallenge(page, `return-to-results-page-${expectedPage}`);
    await page.waitForTimeout(500);
    if (await isExpectedResultsPage(page, expectedPage)) return true;
  }

  await courtesyWait(page, `reloading results page ${expectedPage}`);
  await page.goto(resultsUrl, {
    waitUntil: 'domcontentloaded',
    timeout: TIMEOUT
  });
  await stopIfAccessChallenge(page, `reload-results-page-${expectedPage}`);
  await page.waitForTimeout(500);
  return isExpectedResultsPage(page, expectedPage);
}

async function extractAllFullNoticesFromCurrentPage(page, currentPage) {
  const notices = [];
  const resultsUrl = page.url();
  let buttons = getViewButtons(page);
  const count = await buttons.count();

  console.log(`Results page ${currentPage}: found ${count} VIEW button(s).`);

  for (let index = 0; index < count; index += 1) {
    await waitForResults(page, currentPage);
    buttons = getViewButtons(page);

    if (index >= await buttons.count()) {
      throw new Error(`VIEW ${index + 1} disappeared on page ${currentPage}.`);
    }

    console.log(`Opening VIEW ${index + 1} of ${count} on page ${currentPage}...`);

    const button = buttons.nth(index);
    const summary = await extractSummaryAroundViewButton(button);
    const oldUrl = page.url();
    const oldText = cleanText(await page.locator('body').innerText());
    const navigation = page.waitForNavigation({
      waitUntil: 'domcontentloaded',
      timeout: 30000
    }).catch(() => null);

    await courtesyWait(page, `opening VIEW ${index + 1} on results page ${currentPage}`);
    await button.scrollIntoViewIfNeeded();
    await button.click();
    await navigation;
    await stopIfAccessChallenge(page, `page-${currentPage}-view-${index + 1}`);
    await waitForDetail(page, oldUrl, oldText);

    fs.writeFileSync(
      `detail-page-${currentPage}-${index + 1}.html`,
      await page.content(),
      'utf8'
    );

    const detail = await extractFullNotice(page);
    const text = detail.text || summary.text;
    let caseNumber =
      extractNoticeIdentifier(text) ||
      extractNoticeIdentifier(summary.text);

    const notice = {
      page: currentPage,
      publication: cleanText(detail.publication || summary.publication),
      publishedDate: cleanText(detail.publishedDate || summary.publishedDate),
      caseNumber,
      noticeId: caseNumber,
      trustor: cleanText(extractTrustor(text)),
      text: cleanText(text),
      detailUrl: page.url()
    };

    if (!notice.caseNumber) notice.caseNumber = fallbackIdentifier(notice);
    notice.noticeId = notice.caseNumber;

    notices.push(notice);
    console.log(`Extracted ${notice.caseNumber} (${notice.text.length} characters).`);

    if (!await returnToResultsPage(page, resultsUrl, currentPage)) {
      await saveDebug(page, `return-failed-page-${currentPage}-view-${index + 1}`);
      throw new Error(`Could not return to results page ${currentPage}.`);
    }
  }

  return notices;
}

async function clickAndWaitForPageChange(page, locator, currentPage, expectedPage) {
  const previousUrl = page.url();
  const navigation = page.waitForNavigation({
    waitUntil: 'domcontentloaded',
    timeout: 30000
  }).catch(() => null);

  console.log(`Navigating from page ${currentPage} to ${expectedPage}...`);
  await courtesyWait(
    page,
    `navigating from results page ${currentPage} to ${expectedPage}`
  );

  await locator.scrollIntoViewIfNeeded();
  await locator.click();
  await navigation;
  await stopIfAccessChallenge(page, `pagination-${currentPage}-to-${expectedPage}`);

  try {
    await waitForResults(page, expectedPage);
  } catch (error) {
    increaseCourtesyDelay();
    await saveDebug(page, `pagination-timeout-${currentPage}-to-${expectedPage}`);
    console.error(`Previous URL: ${previousUrl}`);
    console.error(`Current URL: ${page.url()}`);
    throw error;
  }

  const info = await getPageInfo(page);
  const viewCount = await getViewButtons(page).count();
  if (!info || info.current !== expectedPage || viewCount === 0) return false;

  console.log(
    `Pagination confirmed: page ${info.current} of ${info.total}; ` +
    `${viewCount} VIEW button(s).`
  );
  return true;
}

async function goToNextPage(page, currentPage, nextPageNumber) {
  const candidates = page.locator([
    `a[href*="Page%24${nextPageNumber}"]`,
    `a[href*="Page$${nextPageNumber}"]`,
    `a[onclick*="Page$${nextPageNumber}"]`,
    `input[onclick*="Page$${nextPageNumber}"]`
  ].join(', '));

  for (let index = 0; index < await candidates.count(); index += 1) {
    const candidate = candidates.nth(index);
    if (!await candidate.isVisible()) continue;
    return clickAndWaitForPageChange(page, candidate, currentPage, nextPageNumber);
  }

  const exactLinks = page.getByRole('link', {
    name: String(nextPageNumber),
    exact: true
  });

  for (let index = 0; index < await exactLinks.count(); index += 1) {
    const link = exactLinks.nth(index);
    if (!await link.isVisible()) continue;

    const evidence = await link.evaluate(element => [
      element.getAttribute('href') || '',
      element.getAttribute('onclick') || '',
      element.id || '',
      element.parentElement?.innerText || ''
    ].join(' '));

    if (/Page\$|page|pager|__doPostBack/i.test(evidence)) {
      return clickAndWaitForPageChange(page, link, currentPage, nextPageNumber);
    }
  }

  const nextControls = page
    .getByRole('link', { name: /^(next|>|›|»|next page)$/i })
    .or(page.getByRole('button', { name: /^(next|>|›|»|next page)$/i }));

  for (let index = 0; index < await nextControls.count(); index += 1) {
    const control = nextControls.nth(index);
    if (!await control.isVisible()) continue;
    return clickAndWaitForPageChange(page, control, currentPage, nextPageNumber);
  }

  return false;
}

function writeOutput(notices) {
  fs.writeFileSync(OUTPUT_JSON, JSON.stringify(notices, null, 2), 'utf8');

  const output = notices.map((notice, index) => [
    `NOTICE ${index + 1}`,
    `Page: ${notice.page}`,
    notice.publication ? `Publication: ${notice.publication}` : null,
    notice.publishedDate ? `Published: ${notice.publishedDate}` : null,
    `Notice ID: ${notice.noticeId}`,
    notice.trustor ? `Trustor: ${notice.trustor}` : null,
    notice.detailUrl ? `Detail URL: ${notice.detailUrl}` : null,
    notice.text
  ].filter(Boolean).join('\n')).join('\n\n----------------------------------------\n\n');

  fs.writeFileSync(OUTPUT_TEXT, output, 'utf8');
}

(async () => {
  let browser;

  try {
    browser = await chromium.launch({
      headless: true,
      args: [
        '--incognito',
        '--disable-dev-shm-usage',
        '--no-first-run',
        '--no-default-browser-check'
      ]
    });

    const context = await browser.newContext({
      viewport: { width: 1440, height: 1000 },
      userAgent:
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ' +
        '(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
      storageState: { cookies: [], origins: [] }
    });

    const page = await context.newPage();
    page.setDefaultTimeout(TIMEOUT);
    page.setDefaultNavigationTimeout(TIMEOUT);

    console.log('Browser launched with a fresh incognito-style context.');
    console.log(`Opening: ${START_URL}`);

    await courtesyWait(page, 'opening the initial search page');
    await page.goto(START_URL, {
      waitUntil: 'domcontentloaded',
      timeout: TIMEOUT
    });
    await stopIfAccessChallenge(page, 'initial-navigation');
    await waitForResults(page);

    fs.writeFileSync('page.html', await page.content(), 'utf8');
    await page.screenshot({ path: 'page.png', fullPage: true });

    const notices = [];
    const seen = new Set();
    let loopGuard = 0;

    while (true) {
      loopGuard += 1;
      if (loopGuard > 250) throw new Error('Pagination safety limit reached.');

      await waitForResults(page);
      const info = await getPageInfo(page);
      if (!info) throw new Error('Results page label was not found.');

      const currentPage = info.current;
      const totalPages = info.total;
      console.log(`Processing results page ${currentPage} of ${totalPages}...`);

      const pageNotices = await extractAllFullNoticesFromCurrentPage(
        page,
        currentPage
      );

      console.log(
        `Extracted ${pageNotices.length} full notice(s) from page ${currentPage}.`
      );

      if (pageNotices.length === 0) {
        await saveDebug(page, `failed-page-${currentPage}`);
        throw new Error(`No notices extracted from page ${currentPage}.`);
      }

      for (const notice of pageNotices) {
        const key = notice.caseNumber || notice.detailUrl || notice.text;
        if (seen.has(key)) continue;
        seen.add(key);
        notices.push(notice);
      }

      writeOutput(notices);
      console.log(
        `Checkpoint saved after page ${currentPage}: ${notices.length} notice(s).`
      );

      if (currentPage >= totalPages) break;

      if (!await goToNextPage(page, currentPage, currentPage + 1)) {
        await saveDebug(page, `pagination-failed-${currentPage}`);
        throw new Error(
          `Could not move from page ${currentPage} to ${currentPage + 1}.`
        );
      }
    }

    writeOutput(notices);
    await page.screenshot({ path: 'final-page.png', fullPage: true });
    console.log(`Finished. Extracted ${notices.length} unique full notice(s).`);
  } finally {
    if (browser) await browser.close();
  }
})().catch(error => {
  console.error('Scraper failed:', error);
  process.exitCode = 1;
});
