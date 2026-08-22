const fs = require('fs');
const { chromium } = require('playwright');

const START_URL =
  process.env.START_URL ||
  'https://www.publicnoticeoregon.com/(S(b13eg0ew25gdpkwpknkphdvy))/Search.aspx';

const OUTPUT_JSON = 'notices.json';
const OUTPUT_TEXT = 'notices.txt';
const DEBUG_HTML = 'page.html';
const DEBUG_SCREENSHOT = 'page.png';
const FINAL_SCREENSHOT = 'final-page.png';

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
  const normalized = cleanText(text);
  const patterns = [
    /\bCase\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bT\.?S\.?\s*(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bTrustee(?:'s)?\s+Sale\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bSale\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bFile\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bLoan\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i
  ];

  for (const pattern of patterns) {
    const match = normalized.match(pattern);
    if (match?.[1]) return normalizeIdentifier(match[1]);
  }

  return '';
}

function extractTrustor(text = '') {
  const normalized = cleanText(text);
  const patterns = [
    /\b(?:original\s+)?trustor(?:s)?\s*[:,]\s*(.+?)(?=\s*,?\s*(?:whose|who|as|beneficiary|trustee|recorded|the))/i,
    /\bTrustor(?:s)?\s*[:,]\s*([^.;]+)/i,
    /\bdeed\s+made\s+by\s+(.+?)\s+as\s+grantor\b/i
  ];

  for (const pattern of patterns) {
    const match = normalized.match(pattern);
    if (match?.[1]) {
      return cleanText(match[1])
        .replace(/^[,;:\-\s]+|[,;:\-\s]+$/g, '')
        .trim();
    }
  }

  return '';
}

function fallbackIdentifier(notice) {
  const source = [
    notice.publication,
    notice.publishedDate,
    notice.text
  ].join('|');

  let hash = 2166136261;

  for (let index = 0; index < source.length; index += 1) {
    hash ^= source.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }

  return `NOTICE-${(hash >>> 0)
    .toString(16)
    .toUpperCase()
    .padStart(8, '0')}`;
}

(async () => {
  let browser;

  try {
    browser = await chromium.launch({ headless: true });

    const context = await browser.newContext({
      viewport: { width: 1440, height: 1000 },
      userAgent:
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ' +
        '(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
    });

    const page = await context.newPage();
    page.setDefaultTimeout(120000);
    page.setDefaultNavigationTimeout(120000);

    console.log(`Opening: ${START_URL}`);

    await page.goto(START_URL, {
      waitUntil: 'domcontentloaded',
      timeout: 120000
    });

    await waitForResults(page);

    fs.writeFileSync(DEBUG_HTML, await page.content(), 'utf8');
    await page.screenshot({ path: DEBUG_SCREENSHOT, fullPage: true });

    const notices = [];
    const seen = new Set();
    let loopGuard = 0;

    while (true) {
      loopGuard += 1;

      if (loopGuard > 250) {
        throw new Error('Pagination safety limit reached (250 pages).');
      }

      await waitForResults(page);

      const pageInfo = await getPageInfo(page);
      const currentPage = pageInfo?.current || loopGuard;
      const totalPages = pageInfo?.total || currentPage;

      console.log(`Processing results page ${currentPage} of ${totalPages}...`);

      const pageNotices = await extractAllFullNoticesFromCurrentPage(
        page,
        currentPage
      );

      console.log(
        `Extracted ${pageNotices.length} full notice(s) ` +
        `from results page ${currentPage}.`
      );

      if (pageNotices.length === 0) {
        await saveFailureFiles(page, `failed-page-${currentPage}`);
        throw new Error(
          `No full sale notices were extracted from page ${currentPage}.`
        );
      }

      for (const notice of pageNotices) {
        const key =
          notice.caseNumber ||
          notice.noticeId ||
          notice.detailUrl ||
          notice.text;

        if (seen.has(key)) continue;

        seen.add(key);
        notices.push(notice);
      }

      if (currentPage >= totalPages) break;

      const previousMarker =
        pageNotices[0]?.caseNumber ||
        pageNotices[0]?.noticeId ||
        pageNotices[0]?.text.slice(0, 160) ||
        `page-${currentPage}`;

      const moved = await goToNextPage(
        page,
        currentPage,
        currentPage + 1,
        previousMarker
      );

      if (!moved) {
        await saveFailureFiles(page, `pagination-failed-${currentPage}`);
        throw new Error(
          `Could not move from page ${currentPage} ` +
          `to page ${currentPage + 1}.`
        );
      }
    }

    writeOutput(notices);

    console.log('');
    console.log(
      `Finished. Extracted ${notices.length} unique full sale notice(s).`
    );
    console.log(`JSON output: ${OUTPUT_JSON}`);
    console.log(`Text output: ${OUTPUT_TEXT}`);

    await page.screenshot({ path: FINAL_SCREENSHOT, fullPage: true });
  } finally {
    if (browser) await browser.close();
  }
})().catch(error => {
  console.error('Scraper failed:', error);
  process.exitCode = 1;
});

async function saveFailureFiles(page, baseName) {
  fs.writeFileSync(`${baseName}.html`, await page.content(), 'utf8');
  await page.screenshot({ path: `${baseName}.png`, fullPage: true });
}

async function waitForResults(page) {
  await page.waitForFunction(() => {
    const text = document.body?.innerText || '';

    const hasViewControl = Array.from(
      document.querySelectorAll('input, button, a')
    ).some(element => {
      const label = String(
        element.value ||
        element.innerText ||
        element.getAttribute('aria-label') ||
        element.getAttribute('title') ||
        ''
      ).trim();

      return /^view$/i.test(label);
    });

    return (
      hasViewControl ||
      /Page\s+\d+\s+of\s+\d+\s+Pages?/i.test(text) ||
      /click\s+['"]?view['"]?\s+to\s+open/i.test(text)
    );
  });

  await page.waitForTimeout(750);
}

async function getPageInfo(page) {
  const bodyText = await page.locator('body').innerText();
  const match = bodyText.match(/Page\s+(\d+)\s+of\s+(\d+)\s+Pages?/i);

  return match
    ? { current: Number(match[1]), total: Number(match[2]) }
    : null;
}

function getViewButtons(page) {
  /*
   * The site is an older ASP.NET application. Depending on the session
   * and browser, VIEW can be rendered as an input, image input, button,
   * link, or a clickable element with a nested VIEW label.
   *
   * XPath is used here because it can compare input values, alt text,
   * title text, aria labels, and visible text case-insensitively.
   */
  const upper = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  const lower = 'abcdefghijklmnopqrstuvwxyz';

  const viewExpression =
    `translate(normalize-space(.), '${lower}', '${upper}') = 'VIEW' or ` +
    `translate(normalize-space(@value), '${lower}', '${upper}') = 'VIEW' or ` +
    `translate(normalize-space(@alt), '${lower}', '${upper}') = 'VIEW' or ` +
    `translate(normalize-space(@title), '${lower}', '${upper}') = 'VIEW' or ` +
    `translate(normalize-space(@aria-label), '${lower}', '${upper}') = 'VIEW'`;

  return page.locator(
    'xpath=(' +
      `//input[${viewExpression}] | ` +
      `//button[${viewExpression}] | ` +
      `//a[${viewExpression}] | ` +
      `//*[@role='button' and (${viewExpression})] | ` +
      `//*[contains(concat(' ', normalize-space(@class), ' '), ' view ') and (${viewExpression})]` +
    ')'
  );
}

async function logViewControlDiagnostics(page) {
  const diagnostics = await page.locator(
    'input, button, a, [role="button"]'
  ).evaluateAll(elements => elements.map((element, index) => ({
    index,
    tag: element.tagName,
    type: element.getAttribute('type') || '',
    id: element.id || '',
    name: element.getAttribute('name') || '',
    value: element.getAttribute('value') || '',
    text: (element.innerText || '').trim().slice(0, 100),
    alt: element.getAttribute('alt') || '',
    title: element.getAttribute('title') || '',
    ariaLabel: element.getAttribute('aria-label') || '',
    href: element.getAttribute('href') || '',
    onclick: element.getAttribute('onclick') || ''
  })));

  fs.writeFileSync(
    'view-control-diagnostics.json',
    JSON.stringify(diagnostics, null, 2),
    'utf8'
  );

  console.log(
    `Wrote view-control-diagnostics.json with ${diagnostics.length} ` +
    'button/link/input candidate(s).'
  );
}

async function extractAllFullNoticesFromCurrentPage(page, currentPage) {
  const notices = [];
  const resultsUrl = page.url();
  const expectedPageInfo = await getPageInfo(page);

  let viewButtons = getViewButtons(page);
  const viewCount = await viewButtons.count();

  console.log(
    `Results page ${currentPage}: found ${viewCount} VIEW button(s).`
  );

  if (viewCount === 0) {
    await logViewControlDiagnostics(page);

    fs.writeFileSync(
      `no-view-buttons-page-${currentPage}.html`,
      await page.content(),
      'utf8'
    );

    await page.screenshot({
      path: `no-view-buttons-page-${currentPage}.png`,
      fullPage: true
    });

    return notices;
  }

  for (let viewIndex = 0; viewIndex < viewCount; viewIndex += 1) {
    console.log(
      `Opening VIEW ${viewIndex + 1} of ${viewCount} ` +
      `on results page ${currentPage}...`
    );

    await waitForResults(page);

    viewButtons = getViewButtons(page);
    const currentViewCount = await viewButtons.count();

    if (viewIndex >= currentViewCount) {
      throw new Error(
        `VIEW button ${viewIndex + 1} is unavailable after returning ` +
        `to results page ${currentPage}; ${currentViewCount} button(s) found.`
      );
    }

    const viewButton = viewButtons.nth(viewIndex);
    const summary = await extractSummaryAroundViewButton(viewButton);

    const oldUrl = page.url();
    const oldText = cleanText(await page.locator('body').innerText());

    const navigationPromise = page
      .waitForNavigation({
        waitUntil: 'domcontentloaded',
        timeout: 30000
      })
      .catch(() => null);

    await viewButton.scrollIntoViewIfNeeded();
    await viewButton.click();
    await navigationPromise;

    await waitForDetailPage(page, oldUrl, oldText);

    fs.writeFileSync(
      `detail-page-${currentPage}-${viewIndex + 1}.html`,
      await page.content(),
      'utf8'
    );

    const detail = await extractFullNoticeFromDetailPage(page);

    let combinedNotice = {
      page: currentPage,
      publication: detail.publication || summary.publication,
      publishedDate: detail.publishedDate || summary.publishedDate,
      caseNumber:
        detail.caseNumber ||
        summary.caseNumber ||
        extractNoticeIdentifier(detail.text || summary.text),
      noticeId: '',
      trustor:
        detail.trustor ||
        summary.trustor ||
        extractTrustor(detail.text || summary.text),
      text: detail.text || summary.text,
      detailUrl: page.url()
    };

    if (!combinedNotice.caseNumber) {
      combinedNotice.caseNumber = fallbackIdentifier(combinedNotice);
    }

    combinedNotice.noticeId = combinedNotice.caseNumber;
    combinedNotice = cleanNotice(combinedNotice);

    notices.push(combinedNotice);

    console.log(
      `Extracted ${combinedNotice.caseNumber} ` +
      `(${combinedNotice.text.length} characters).`
    );

    const returned = await returnToResultsPage(
      page,
      resultsUrl,
      expectedPageInfo?.current || currentPage
    );

    if (!returned) {
      throw new Error(
        `Could not return to results page ${currentPage} ` +
        `after opening VIEW ${viewIndex + 1}.`
      );
    }
  }

  return notices;
}

async function waitForDetailPage(page, oldUrl, oldText) {
  await page.waitForFunction(
    ({ previousUrl, previousText }) => {
      const currentText = document.body?.innerText || '';
      const urlChanged = window.location.href !== previousUrl;
      const bodyChanged = currentText.trim() !== previousText.trim();
      const looksLikeDetail =
        /Public Notice Detail/i.test(currentText) ||
        /(?:TRUSTEE(?:'S)?\s+)?NOTICE OF SALE/i.test(currentText);

      return bodyChanged && (urlChanged || looksLikeDetail);
    },
    { previousUrl: oldUrl, previousText: oldText },
    { timeout: 120000 }
  );

  await page.waitForTimeout(750);
}

async function extractSummaryAroundViewButton(viewButton) {
  return viewButton.evaluate(button => {
    const normalize = (value = '') =>
      String(value)
        .replace(/\u00a0/g, ' ')
        .replace(/\r/g, '')
        .replace(/[ \t]+/g, ' ')
        .replace(/ *\n */g, '\n')
        .replace(/\n{3,}/g, '\n\n')
        .trim();

    const datePattern = /\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b/i;

    const identifierPatterns = [
      /\bCase\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
      /\bT\.?S\.?\s*(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
      /\bTrustee(?:'s)?\s+Sale\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
      /\bSale\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
      /\bFile\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
      /\bLoan\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i
    ];

    const buttonRow = button.closest('tr');
    let containerText = '';

    if (buttonRow) {
      const rowTexts = [normalize(buttonRow.innerText)];
      let sibling = buttonRow.nextElementSibling;

      for (let index = 0; sibling && index < 2; index += 1) {
        rowTexts.push(normalize(sibling.innerText));
        if (/click\s+['"]?view['"]?\s+to\s+open/i.test(rowTexts.at(-1))) {
          break;
        }
        sibling = sibling.nextElementSibling;
      }

      containerText = normalize(rowTexts.join('\n'));
    }

    if (!containerText || !/NOTICE OF SALE/i.test(containerText)) {
      let container = button.parentElement;

      while (container && container !== document.body) {
        const candidateText = normalize(container.innerText);
        const noticeCount = (
          candidateText.match(/(?:TRUSTEE(?:'S)?\s+)?NOTICE OF SALE/gi) || []
        ).length;

        if (/NOTICE OF SALE/i.test(candidateText) && noticeCount === 1) {
          containerText = candidateText;
          break;
        }

        container = container.parentElement;
      }
    }

    const dateMatch = containerText.match(datePattern);
    const publishedDate = dateMatch?.[0] || '';
    let publication = '';

    if (dateMatch) {
      const lines = containerText
        .slice(0, dateMatch.index)
        .split('\n')
        .map(line => line.trim())
        .filter(Boolean)
        .filter(line => !/^view$/i.test(line));

      publication = lines.at(-1) || '';
    }

    let caseNumber = '';

    for (const pattern of identifierPatterns) {
      const match = containerText.match(pattern);
      if (match?.[1]) {
        caseNumber = match[1].toUpperCase();
        break;
      }
    }

    const trustorPatterns = [
      /\b(?:original\s+)?trustor(?:s)?\s*[:,]\s*(.+?)(?=\s*,?\s*(?:whose|who|as|beneficiary|trustee|recorded|the))/i,
      /\bdeed\s+made\s+by\s+(.+?)\s+as\s+grantor\b/i
    ];

    let trustor = '';

    for (const pattern of trustorPatterns) {
      const match = containerText.match(pattern);
      if (match?.[1]) {
        trustor = normalize(match[1]);
        break;
      }
    }

    return {
      publication,
      publishedDate,
      caseNumber,
      trustor,
      text: containerText
    };
  });
}

async function extractFullNoticeFromDetailPage(page) {
  const result = await page.evaluate(() => {
    const normalize = (value = '') =>
      String(value)
        .replace(/\u00a0/g, ' ')
        .replace(/\r/g, '')
        .replace(/[ \t]+/g, ' ')
        .replace(/ *\n */g, '\n')
        .replace(/\n{3,}/g, '\n\n')
        .trim();

    const datePattern = /\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b/i;
    const salePattern = /(?:TRUSTEE(?:'S)?\s+)?NOTICE OF SALE/i;

    const identifierPatterns = [
      /\bCase\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
      /\bT\.?S\.?\s*(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
      /\bTrustee(?:'s)?\s+Sale\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
      /\bSale\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
      /\bFile\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
      /\bLoan\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i
    ];

    const preferredSelectors = [
      '[id*="NoticeText"]',
      '[id*="FullText"]',
      '[id*="PublicNotice"]',
      '[id*="NoticeDetail"]',
      '[class*="notice-text"]',
      '[class*="NoticeText"]',
      '[class*="notice-detail"]',
      '[class*="NoticeDetail"]',
      'article',
      'main'
    ];

    const candidates = [];

    for (const selector of preferredSelectors) {
      for (const element of document.querySelectorAll(selector)) {
        const text = normalize(element.innerText);
        if (salePattern.test(text) && text.length >= 100) {
          candidates.push({ element, text });
        }
      }
    }

    if (candidates.length === 0) {
      for (const element of document.querySelectorAll(
        'div, td, section, article, p'
      )) {
        const text = normalize(element.innerText);
        if (salePattern.test(text) && text.length >= 100) {
          candidates.push({ element, text });
        }
      }
    }

    candidates.sort((left, right) => right.text.length - left.text.length);

    let fullText = candidates[0]?.text || '';
    const noticeStart = fullText.search(salePattern);

    if (noticeStart >= 0) {
      fullText = fullText.slice(noticeStart);
    }

    fullText = fullText
      .replace(/\.{3}\s*click\s+['"]?view['"]?\s+to\s+open\s+the\s+full\s+text\.?/gi, '')
      .trim();

    let caseNumber = '';

    for (const pattern of identifierPatterns) {
      const match = fullText.match(pattern);
      if (match?.[1]) {
        caseNumber = match[1].toUpperCase();
        break;
      }
    }

    const trustorPatterns = [
      /\b(?:original\s+)?trustor(?:s)?\s*[:,]\s*(.+?)(?=\s*,?\s*(?:whose|who|as|beneficiary|trustee|recorded|the))/i,
      /\bdeed\s+made\s+by\s+(.+?)\s+as\s+grantor\b/i
    ];

    let trustor = '';

    for (const pattern of trustorPatterns) {
      const match = fullText.match(pattern);
      if (match?.[1]) {
        trustor = normalize(match[1]);
        break;
      }
    }

    const bodyText = normalize(document.body?.innerText || '');
    const dateMatch = bodyText.match(datePattern);
    const publishedDate = dateMatch?.[0] || '';
    let publication = '';

    if (dateMatch) {
      const lines = bodyText
        .slice(0, dateMatch.index)
        .split('\n')
        .map(line => line.trim())
        .filter(Boolean)
        .filter(line =>
          !/^(view|back|previous|next|search results)$/i.test(line)
        );

      publication = lines.at(-1) || '';
    }

    return {
      publication,
      publishedDate,
      caseNumber,
      trustor,
      text: normalize(fullText)
    };
  });

  return {
    publication: cleanText(result.publication),
    publishedDate: cleanText(result.publishedDate),
    caseNumber: normalizeIdentifier(result.caseNumber),
    trustor: cleanText(result.trustor),
    text: cleanText(result.text)
  };
}

function cleanNotice(notice) {
  return {
    page: notice.page,
    publication: cleanText(notice.publication),
    publishedDate: cleanText(notice.publishedDate),
    caseNumber: normalizeIdentifier(notice.caseNumber),
    noticeId: normalizeIdentifier(notice.noticeId || notice.caseNumber),
    trustor: cleanText(notice.trustor),
    text: cleanText(notice.text),
    detailUrl: cleanText(notice.detailUrl)
  };
}

async function returnToResultsPage(page, resultsUrl, expectedPage) {
  const historyResult = await page
    .goBack({ waitUntil: 'domcontentloaded', timeout: 30000 })
    .catch(() => null);

  if (historyResult) {
    await page.waitForTimeout(750);

    if (await isExpectedResultsPage(page, expectedPage)) {
      return true;
    }
  }

  const backControl = page
    .getByRole('link', {
      name: /back to search|search results|return to results|back/i
    })
    .or(page.getByRole('button', {
      name: /back to search|search results|return to results|back/i
    }))
    .first();

  if (await backControl.count()) {
    const navigationPromise = page
      .waitForNavigation({
        waitUntil: 'domcontentloaded',
        timeout: 30000
      })
      .catch(() => null);

    await backControl.click();
    await navigationPromise;
    await page.waitForTimeout(750);

    if (await isExpectedResultsPage(page, expectedPage)) {
      return true;
    }
  }

  await page.goto(resultsUrl, {
    waitUntil: 'domcontentloaded',
    timeout: 120000
  });

  await page.waitForTimeout(750);
  return isExpectedResultsPage(page, expectedPage);
}

async function isExpectedResultsPage(page, expectedPage) {
  const viewCount = await getViewButtons(page).count();
  if (viewCount === 0) return false;

  const pageInfo = await getPageInfo(page);
  return !pageInfo || pageInfo.current === expectedPage;
}

async function goToNextPage(
  page,
  currentPage,
  nextPageNumber,
  previousMarker
) {
  const selectors = [
    `a[href*="Page%24${nextPageNumber}"]`,
    `a[href*="Page$${nextPageNumber}"]`,
    `a[onclick*="Page$${nextPageNumber}"]`,
    `input[onclick*="Page$${nextPageNumber}"]`
  ].join(',');

  const aspNetPager = page.locator(selectors);

  if (await aspNetPager.count()) {
    return clickAndWaitForPageChange(
      page,
      aspNetPager.first(),
      currentPage,
      nextPageNumber,
      previousMarker
    );
  }

  const exactLinks = page.getByRole('link', {
    name: String(nextPageNumber),
    exact: true
  });

  const exactLinkCount = await exactLinks.count();

  for (let index = 0; index < exactLinkCount; index += 1) {
    const link = exactLinks.nth(index);
    const evidence = await link.evaluate(element => [
      element.getAttribute('href') || '',
      element.getAttribute('onclick') || '',
      element.getAttribute('title') || '',
      element.parentElement?.innerText || ''
    ].join(' '));

    if (/page|pager|next|__doPostBack/i.test(evidence)) {
      return clickAndWaitForPageChange(
        page,
        link,
        currentPage,
        nextPageNumber,
        previousMarker
      );
    }
  }

  const nextControl = page
    .getByRole('link', { name: /^(next|>|›|»|next page)$/i })
    .or(page.getByRole('button', {
      name: /^(next|>|›|»|next page)$/i
    }))
    .first();

  if (await nextControl.count()) {
    return clickAndWaitForPageChange(
      page,
      nextControl,
      currentPage,
      nextPageNumber,
      previousMarker
    );
  }

  return false;
}

async function clickAndWaitForPageChange(
  page,
  locator,
  currentPage,
  expectedPage,
  previousMarker
) {
  const navigationPromise = page
    .waitForNavigation({
      waitUntil: 'domcontentloaded',
      timeout: 30000
    })
    .catch(() => null);

  await locator.scrollIntoViewIfNeeded();
  await locator.click();
  await navigationPromise;

  await page.waitForFunction(
    ({ expected, marker }) => {
      const text = document.body?.innerText || '';
      const pageChanged = new RegExp(
        `Page\\s+${expected}\\s+of\\s+\\d+\\s+Pages?`,
        'i'
      ).test(text);
      const resultChanged = Boolean(marker) && !text.includes(marker);
      return pageChanged || resultChanged;
    },
    { expected: expectedPage, marker: previousMarker },
    { timeout: 120000 }
  );

  await page.waitForTimeout(750);

  const info = await getPageInfo(page);
  return !info || info.current !== currentPage;
}

function writeOutput(notices) {
  fs.writeFileSync(
    OUTPUT_JSON,
    JSON.stringify(notices, null, 2),
    'utf8'
  );

  const textOutput = notices
    .map((notice, index) => [
      `NOTICE ${index + 1}`,
      `Page: ${notice.page}`,
      notice.publication ? `Publication: ${notice.publication}` : null,
      notice.publishedDate ? `Published: ${notice.publishedDate}` : null,
      `Notice ID: ${notice.noticeId}`,
      notice.trustor ? `Trustor: ${notice.trustor}` : null,
      notice.detailUrl ? `Detail URL: ${notice.detailUrl}` : null,
      notice.text
    ].filter(Boolean).join('\n'))
    .join('\n\n----------------------------------------\n\n');

  fs.writeFileSync(OUTPUT_TEXT, textOutput, 'utf8');
}
