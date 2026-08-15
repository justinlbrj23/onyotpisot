const fs = require('fs');
const { chromium } = require('playwright');

const START_URL =
  process.env.START_URL ||
  'https://www.publicnoticeoregon.com/Search.aspx';

const OUTPUT_JSON = 'notices.json';
const OUTPUT_TEXT = 'notices.txt';
const DEBUG_HTML = 'page.html';
const DEBUG_SCREENSHOT = 'page.png';

function cleanText(value = '') {
  return String(value)
    .replace(/\u00a0/g, ' ')
    .replace(/\r/g, '')
    .replace(/[ \t]+/g, ' ')
    .replace(/ *\n */g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function normalizeCaseNumber(value = '') {
  return cleanText(value).toUpperCase();
}

(async () => {
  let browser;

  try {
    browser = await chromium.launch({
      headless: true
    });

    const context = await browser.newContext({
      viewport: {
        width: 1440,
        height: 1000
      },
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

    /*
     * Preserve the initial page HTML and screenshot for debugging.
     * These files are uploaded by the GitHub Actions workflow.
     */
    fs.writeFileSync(
      DEBUG_HTML,
      await page.content(),
      'utf8'
    );

    await page.screenshot({
      path: DEBUG_SCREENSHOT,
      fullPage: true
    });

    const notices = [];
    const seen = new Set();

    let loopGuard = 0;

    while (true) {
      loopGuard += 1;

      /*
       * Prevent an infinite pagination loop if the website returns
       * unexpected page information.
       */
      if (loopGuard > 250) {
        throw new Error(
          'Pagination safety limit reached (250 pages).'
        );
      }

      await waitForResults(page);

      const pageInfo = await getPageInfo(page);

      const currentPage =
        pageInfo?.current || loopGuard;

      const totalPages =
        pageInfo?.total || currentPage;

      console.log(
        `Extracting page ${currentPage} of ${totalPages}...`
      );

      const pageNotices = await extractNotices(page);

      console.log(
        `Found ${pageNotices.length} notice(s) on this page.`
      );

      if (pageNotices.length === 0) {
        throw new Error(
          `No notice records were extracted from results page ${currentPage}.`
        );
      }

      for (const notice of pageNotices) {
        /*
         * Case number is used as the primary unique key.
         * Notice text is used only if no case number is available.
         */
        const key =
          notice.caseNumber ||
          notice.text;

        if (seen.has(key)) {
          continue;
        }

        seen.add(key);

        notices.push({
          page: currentPage,
          publication: notice.publication,
          publishedDate: notice.publishedDate,
          caseNumber: notice.caseNumber,
          text: notice.text
        });
      }

      if (currentPage >= totalPages) {
        break;
      }

      /*
       * This marker helps verify that the result records changed
       * after clicking the next-page control.
       */
      const previousMarker =
        pageNotices[0]?.caseNumber ||
        pageNotices[0]?.text.slice(0, 120) ||
        `page-${currentPage}`;

      const nextPageNumber =
        currentPage + 1;

      const moved = await goToNextPage(
        page,
        currentPage,
        nextPageNumber,
        previousMarker
      );

      if (!moved) {
        throw new Error(
          `Could not move from page ${currentPage} ` +
          `to page ${nextPageNumber}.`
        );
      }
    }

    writeOutput(notices);

    console.log('');
    console.log(
      `Finished. Extracted ${notices.length} unique notice(s).`
    );
    console.log(`JSON output: ${OUTPUT_JSON}`);
    console.log(`Text output: ${OUTPUT_TEXT}`);

    await page.screenshot({
      path: 'final-page.png',
      fullPage: true
    });
  } finally {
    if (browser) {
      await browser.close();
    }
  }
})().catch(error => {
  console.error('Scraper failed:', error);
  process.exitCode = 1;
});

async function waitForResults(page) {
  await page.waitForFunction(() => {
    const text =
      document.body?.innerText || '';

    return (
      /Page\s+\d+\s+of\s+\d+\s+Pages?/i.test(text) ||
      /NOTICE TO INTERESTED PERSONS/i.test(text) ||
      /PROBATE DEPARTMENT/i.test(text)
    );
  });

  /*
   * Let asynchronous page updates settle before extraction.
   */
  await page.waitForTimeout(750);
}

async function getPageInfo(page) {
  const bodyText =
    await page.locator('body').innerText();

  const match = bodyText.match(
    /Page\s+(\d+)\s+of\s+(\d+)\s+Pages?/i
  );

  if (!match) {
    return null;
  }

  return {
    current: Number(match[1]),
    total: Number(match[2])
  };
}

async function extractNotices(page) {
  const rawNotices = await page.evaluate(() => {
    const normalize = (value = '') =>
      String(value)
        .replace(/\u00a0/g, ' ')
        .replace(/\r/g, '')
        .replace(/[ \t]+/g, ' ')
        .replace(/ *\n */g, '\n')
        .replace(/\n{3,}/g, '\n\n')
        .trim();

    const noticePattern =
      /NOTICE TO INTERESTED PERSONS/i;

    const casePattern =
      /\bCase\s+No\.?\s*([A-Z0-9-]+)/i;

    const datePattern =
      /\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b/i;

    const looksLikeNotice = text => {
      return (
        noticePattern.test(text) &&
        casePattern.test(text)
      );
    };

    const countDistinctCases = text => {
      const matches = [
        ...text.matchAll(
          /\bCase\s+No\.?\s*([A-Z0-9-]+)/gi
        )
      ];

      return new Set(
        matches.map(match =>
          match[1].toUpperCase()
        )
      ).size;
    };

    /*
     * Search likely result-container elements.
     */
    const allElements = Array.from(
      document.querySelectorAll(
        'article, li, tr, td, div, section, p'
      )
    );

    /*
     * Keep the smallest individual element containing exactly
     * one case number and one probate-notice summary.
     */
    const leafNotices = allElements.filter(element => {
      const text =
        normalize(element.innerText);

      if (
        !looksLikeNotice(text) ||
        countDistinctCases(text) !== 1
      ) {
        return false;
      }

      const hasMatchingChild = Array.from(
        element.querySelectorAll(
          'article, li, tr, td, div, section, p'
        )
      ).some(child => {
        if (child === element) {
          return false;
        }

        return looksLikeNotice(
          normalize(child.innerText)
        );
      });

      return !hasMatchingChild;
    });

    return leafNotices.map(leaf => {
      const leafText =
        normalize(leaf.innerText);

      const caseMatch =
        leafText.match(casePattern);

      const caseNumber =
        caseMatch?.[1]?.toUpperCase() || '';

      /*
       * The publication and date may be stored in sibling
       * elements. Walk upward until they are found, but stop
       * before entering a container holding multiple notices.
       */
      let recordElement = leaf;
      let parent = leaf.parentElement;

      while (
        parent &&
        parent !== document.body
      ) {
        const parentText =
          normalize(parent.innerText);

        if (
          countDistinctCases(parentText) !== 1
        ) {
          break;
        }

        recordElement = parent;

        if (datePattern.test(parentText)) {
          break;
        }

        parent = parent.parentElement;
      }

      const recordText =
        normalize(recordElement.innerText);

      const dateMatch =
        recordText.match(datePattern);

      const publishedDate =
        dateMatch?.[0] || '';

      let publication = '';

      if (dateMatch) {
        const linesBeforeDate = recordText
          .slice(0, dateMatch.index)
          .split('\n')
          .map(line => line.trim())
          .filter(Boolean)
          .filter(line =>
            !/^view$/i.test(line)
          );

        publication =
          linesBeforeDate.at(-1) || '';
      }

      /*
       * Prefer the smallest notice element so surrounding
       * navigation, pagination, and filter text are excluded.
       */
      let text = leafText;

      if (!noticePattern.test(text)) {
        text = recordText;
      }

      return {
        publication,
        publishedDate,
        caseNumber,
        text
      };
    });
  });

  /*
   * Perform secondary cleanup and deduplication outside
   * the browser context.
   */
  const unique = new Map();

  for (const notice of rawNotices) {
    const cleaned = {
      publication:
        cleanText(notice.publication),

      publishedDate:
        cleanText(notice.publishedDate),

      caseNumber:
        normalizeCaseNumber(notice.caseNumber),

      text:
        cleanText(notice.text)
    };

    /*
     * Skip unusable records.
     */
    if (
      !cleaned.caseNumber ||
      !cleaned.text
    ) {
      continue;
    }

    /*
     * Store only one record per case number.
     */
    if (!unique.has(cleaned.caseNumber)) {
      unique.set(
        cleaned.caseNumber,
        cleaned
      );
    }
  }

  return [...unique.values()];
}

async function goToNextPage(
  page,
  currentPage,
  nextPageNumber,
  previousMarker
) {
  /*
   * ASP.NET sites commonly paginate with a postback:
   *
   * javascript:__doPostBack(
   *   'controlName',
   *   'Page$2'
   * )
   */
  const aspNetPager = page.locator(
    `a[href*="Page%24${nextPageNumber}"], ` +
    `a[href*="Page$${nextPageNumber}"], ` +
    `a[onclick*="Page$${nextPageNumber}"], ` +
    `input[onclick*="Page$${nextPageNumber}"]`
  );

  if (await aspNetPager.count()) {
    return clickAndWaitForPageChange(
      page,
      aspNetPager.first(),
      currentPage,
      nextPageNumber,
      previousMarker
    );
  }

  /*
   * Find the visible "Page X of Y Pages" label and look
   * for a nearby numeric page link.
   */
  const pageLabel = page
    .getByText(
      new RegExp(
        `Page\\s+${currentPage}` +
        `\\s+of\\s+\\d+\\s+Pages?`,
        'i'
      )
    )
    .first();

  if (await pageLabel.count()) {
    const nearbyPager = pageLabel.locator(
      'xpath=ancestor::*[' +
      'self::div or ' +
      'self::td or ' +
      'self::tr or ' +
      'self::section' +
      '][1]'
    );

    const numericLink =
      nearbyPager.getByRole('link', {
        name: String(nextPageNumber),
        exact: true
      });

    if (await numericLink.count()) {
      return clickAndWaitForPageChange(
        page,
        numericLink.first(),
        currentPage,
        nextPageNumber,
        previousMarker
      );
    }
  }

  /*
   * Search for all links whose visible text exactly matches
   * the next page number.
   */
  const exactLinks = page.getByRole(
    'link',
    {
      name: String(nextPageNumber),
      exact: true
    }
  );

  const exactLinkCount =
    await exactLinks.count();

  for (
    let index = 0;
    index < exactLinkCount;
    index += 1
  ) {
    const link =
      exactLinks.nth(index);

    const attributes =
      await link.evaluate(element => ({
        href:
          element.getAttribute('href') || '',

        onclick:
          element.getAttribute('onclick') || '',

        title:
          element.getAttribute('title') || '',

        parentText:
          element.parentElement?.innerText || ''
      }));

    const evidence =
      Object.values(attributes).join(' ');

    if (/page|pager|next/i.test(evidence)) {
      return clickAndWaitForPageChange(
        page,
        link,
        currentPage,
        nextPageNumber,
        previousMarker
      );
    }
  }

  /*
   * Final fallback for a visible Next link or button.
   */
  const nextControl = page
    .getByRole('link', {
      name: /^(next|>|›|»|next page)$/i
    })
    .or(
      page.getByRole('button', {
        name: /^(next|>|›|»|next page)$/i
      })
    )
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
  /*
   * Start waiting before clicking. The site may use either
   * a full ASP.NET navigation or a partial JavaScript update.
   */
  const navigationPromise = page
    .waitForNavigation({
      waitUntil: 'domcontentloaded',
      timeout: 30000
    })
    .catch(() => null);

  await locator.scrollIntoViewIfNeeded();
  await locator.click();

  await navigationPromise;

  /*
   * Wait for either the page number or the first notice
   * marker to change.
   */
  await page.waitForFunction(
    ({ expected, marker }) => {
      const text =
        document.body?.innerText || '';

      const pageChanged = new RegExp(
        `Page\\s+${expected}` +
        `\\s+of\\s+\\d+\\s+Pages?`,
        'i'
      ).test(text);

      const firstNoticeChanged =
        Boolean(marker) &&
        !text.includes(marker);

      return (
        pageChanged ||
        firstNoticeChanged
      );
    },
    {
      expected: expectedPage,
      marker: previousMarker
    },
    {
      timeout: 120000
    }
  );

  await page.waitForTimeout(750);

  /*
   * Perform an additional page-number consistency check.
   */
  const info =
    await getPageInfo(page);

  if (
    info &&
    info.current !== expectedPage
  ) {
    console.warn(
      `Expected page ${expectedPage}, ` +
      `but the page label reports ${info.current}.`
    );
  }

  return (
    !info ||
    info.current !== currentPage
  );
}

function writeOutput(notices) {
  fs.writeFileSync(
    OUTPUT_JSON,
    JSON.stringify(
      notices,
      null,
      2
    ),
    'utf8'
  );

  const textOutput = notices
    .map((notice, index) => {
      return [
        `NOTICE ${index + 1}`,
        `Page: ${notice.page}`,

        notice.publication
          ? `Publication: ${notice.publication}`
          : null,

        notice.publishedDate
          ? `Published: ${notice.publishedDate}`
          : null,

        `Case number: ${notice.caseNumber}`,
        notice.text
      ]
        .filter(Boolean)
        .join('\n');
    })
    .join(
      '\n\n' +
      '----------------------------------------' +
      '\n\n'
    );

  fs.writeFileSync(
    OUTPUT_TEXT,
    textOutput,
    'utf8'
  );
}
