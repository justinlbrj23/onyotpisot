const fs = require('fs');
const { chromium } = require('playwright');

const START_URL =
  process.env.START_URL ||
  'https://www.publicnoticeoregon.com/(S(ayzufrbyvqdnk3fqqajn0b4c))/Search.aspx';

const OUTPUT_JSON = 'notices.json';
const OUTPUT_TEXT = 'notices.txt';

function cleanText(value) {
  return value
    .replace(/\u00a0/g, ' ')
    .replace(/\r/g, '')
    .replace(/[ \t]+/g, ' ')
    .replace(/ *\n */g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

(async () => {
  const browser = await chromium.launch({
    headless: true
  });

  const context = await browser.newContext({
    viewport: {
      width: 1440,
      height: 1000
    }
  });

  const page = await context.newPage();

  console.log(`Opening: ${START_URL}`);

  await page.goto(START_URL, {
    waitUntil: 'domcontentloaded',
    timeout: 120000
  });

  /*
   * If the search is performed by your script, automate the search/filter
   * controls here before starting pagination.
   *
   * This script assumes the loaded page is already showing the desired
   * search results.
   */

  await waitForResults(page);

  const notices = [];
  const seen = new Set();

  let currentPage = 1;
  let totalPages = 1;

  while (true) {
    await waitForResults(page);

    const pageInfo = await getPageInfo(page);

    if (pageInfo) {
      currentPage = pageInfo.current;
      totalPages = pageInfo.total;
    }

    console.log(`Extracting page ${currentPage} of ${totalPages}...`);

    const pageNotices = await extractNotices(page);

    console.log(`Found ${pageNotices.length} notice(s) on this page.`);

    for (const notice of pageNotices) {
      /*
       * Case number is normally the best unique key.
       * If it is unavailable, use the complete notice text.
       */
      const key = notice.caseNumber || notice.text;

      if (!seen.has(key)) {
        seen.add(key);

        notices.push({
          page: currentPage,
          publication: notice.publication,
          publishedDate: notice.publishedDate,
          caseNumber: notice.caseNumber,
          text: notice.text
        });
      }
    }

    if (currentPage >= totalPages) {
      break;
    }

    const previousMarker =
      pageNotices[0]?.caseNumber ||
      pageNotices[0]?.text ||
      `page-${currentPage}`;

    const nextPageNumber = currentPage + 1;

    const clicked = await goToNextPage(
      page,
      currentPage,
      nextPageNumber,
      previousMarker
    );

    if (!clicked) {
      console.warn(
        `Could not locate the pagination control for page ${nextPageNumber}.`
      );
      break;
    }

    currentPage = nextPageNumber;
  }

  writeOutput(notices);

  console.log('');
  console.log(`Finished. Extracted ${notices.length} unique notice(s).`);
  console.log(`JSON output: ${OUTPUT_JSON}`);
  console.log(`Text output: ${OUTPUT_TEXT}`);

  await page.screenshot({
    path: 'final-page.png',
    fullPage: true
  });

  await browser.close();
})().catch(error => {
  console.error(error);
  process.exit(1);
});

async function waitForResults(page) {
  await page.waitForFunction(
    () => {
      const text = document.body?.innerText || '';

      return (
        /Page\s+\d+\s+of\s+\d+\s+Pages?/i.test(text) ||
        /NOTICE TO INTERESTED PERSONS/i.test(text) ||
        /PROBATE DEPARTMENT/i.test(text)
      );
    },
    {
      timeout: 120000
    }
  );

  /*
   * Allow client-side DOM updates to settle.
   */
  await page.waitForTimeout(750);
}

async function getPageInfo(page) {
  const bodyText = await page.locator('body').innerText();

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
    const normalize = value =>
      (value || '')
        .replace(/\u00a0/g, ' ')
        .replace(/\r/g, '')
        .replace(/[ \t]+/g, ' ')
        .replace(/ *\n */g, '\n')
        .replace(/\n{3,}/g, '\n\n')
        .trim();

    const isNoticeText = text => {
      return (
        /NOTICE TO INTERESTED PERSONS/i.test(text) &&
        (
          /\bCase\s+No\.?\s*[A-Z0-9-]+/i.test(text) ||
          /\bCASE\s+No\.?\s*[A-Z0-9-]+/i.test(text)
        )
      );
    };

    const elements = Array.from(
      document.querySelectorAll(
        'article, li, tr, td, div, section'
      )
    );

    /*
     * Find every element that looks like it contains a notice.
     */
    const candidates = elements.filter(element => {
      const text = normalize(element.innerText);

      if (!isNoticeText(text)) {
        return false;
      }

      /*
       * Result summaries generally contain the "click view" message.
       * Keep NOTICE TO INTERESTED PERSONS as a fallback in case the
       * site's wording changes.
       */
      return (
        /click\s+['"]?view['"]?\s+to\s+open/i.test(text) ||
        /NOTICE TO INTERESTED PERSONS/i.test(text)
      );
    });

    /*
     * Keep only the smallest matching element. This prevents extracting
     * a parent container that contains several notices.
     */
    const smallestCandidates = candidates.filter(element => {
      return !Array.from(
        element.querySelectorAll('article, li, tr, td, div, section')
      ).some(child => {
        if (child === element) {
          return false;
        }

        return isNoticeText(normalize(child.innerText));
      });
    });

    return smallestCandidates.map(element => {
      const text = normalize(element.innerText);

      const dateMatch = text.match(
        /\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b/i
      );

      const caseMatch = text.match(
        /\bCase\s+No\.?\s*([A-Z0-9-]+)/i
      );

      let publication = '';

      if (dateMatch) {
        const beforeDate = text
          .slice(0, dateMatch.index)
          .split('\n')
          .map(line => line.trim())
          .filter(Boolean);

        publication = beforeDate.at(-1) || '';
      }

      return {
        publication,
        publishedDate: dateMatch?.[0] || '',
        caseNumber: caseMatch?.[1] || '',
        text
      };
    });
  });

  /*
   * Secondary cleanup and deduplication within the page.
   */
  const unique = new Map();

  for (const notice of rawNotices) {
    const cleaned = {
      publication: cleanText(notice.publication),
      publishedDate: cleanText(notice.publishedDate),
      caseNumber: cleanText(notice.caseNumber),
      text: cleanText(notice.text)
    };

    const key = cleaned.caseNumber || cleaned.text;

    if (!unique.has(key)) {
      unique.set(key, cleaned);
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
   * PublicNotice sites commonly use ASP.NET postbacks such as:
   *
   * javascript:__doPostBack('...', 'Page$2')
   *
   * First, look for a link whose href or onclick references Page$2.
   */
  const aspNetPager = page.locator(
    `a[href*="Page%24${nextPageNumber}"], ` +
    `a[href*="Page$${nextPageNumber}"], ` +
    `a[onclick*="Page$${nextPageNumber}"], ` +
    `input[onclick*="Page$${nextPageNumber}"]`
  );

  if (await aspNetPager.count()) {
    await clickAndWaitForPageChange(
      page,
      aspNetPager.first(),
      currentPage,
      previousMarker
    );

    return true;
  }

  /*
   * Next, locate the "Page X of Y Pages" text and search its nearby
   * container for a numeric page link.
   */
  const pageLabel = page.getByText(
    new RegExp(
      `Page\\s+${currentPage}\\s+of\\s+\\d+\\s+Pages?`,
      'i'
    )
  ).first();

  if (await pageLabel.count()) {
    const nearbyPager = pageLabel.locator(
      `xpath=ancestor::*[self::div or self::td or self::tr or self::section][1]`
    );

    const numericLink = nearbyPager.getByRole('link', {
      name: String(nextPageNumber),
      exact: true
    });

    if (await numericLink.count()) {
      await clickAndWaitForPageChange(
        page,
        numericLink.first(),
        currentPage,
        previousMarker
      );

      return true;
    }
  }

  /*
   * Fallback: search all exact numeric links. Filter out page-size
   * controls by preferring links with pager-related href/onclick values.
   */
  const exactLinks = page.getByRole('link', {
    name: String(nextPageNumber),
    exact: true
  });

  const linkCount = await exactLinks.count();

  for (let index = 0; index < linkCount; index++) {
    const link = exactLinks.nth(index);

    const attributes = await link.evaluate(element => ({
      href: element.getAttribute('href') || '',
      onclick: element.getAttribute('onclick') || '',
      title: element.getAttribute('title') || '',
      parentText: element.parentElement?.innerText || ''
    }));

    const pagerEvidence = [
      attributes.href,
      attributes.onclick,
      attributes.title,
      attributes.parentText
    ].join(' ');

    if (/page|pager|next/i.test(pagerEvidence)) {
      await clickAndWaitForPageChange(
        page,
        link,
        currentPage,
        previousMarker
      );

      return true;
    }
  }

  /*
   * Final fallback: locate an explicit Next link/button.
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
    await clickAndWaitForPageChange(
      page,
      nextControl,
      currentPage,
      previousMarker
    );

    return true;
  }

  return false;
}

async function clickAndWaitForPageChange(
  page,
  locator,
  currentPage,
  previousMarker
) {
  /*
   * ASP.NET pagination may perform a full navigation or update the page
   * through JavaScript. Start waiting before clicking so neither event is
   * missed.
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

  await page.waitForFunction(
    ({ oldPage, marker }) => {
      const text = document.body?.innerText || '';

      const pageChanged = new RegExp(
        `Page\\s+(?!${oldPage}\\b)\\d+\\s+of\\s+\\d+\\s+Pages?`,
        'i'
      ).test(text);

      const noticeChanged =
        marker && !text.includes(marker);

      return pageChanged || noticeChanged;
    },
    {
      oldPage: currentPage,
      marker: previousMarker
    },
    {
      timeout: 120000
    }
  );

  await page.waitForTimeout(750);
}

function writeOutput(notices) {
  fs.writeFileSync(
    OUTPUT_JSON,
    JSON.stringify(notices, null, 2),
    'utf8'
  );

  const textOutput = notices
    .map((notice, index) => {
      return [
        `NOTICE ${index + 1}`,
        `Page: ${notice.page}`,
        notice.text,
        ''
      ].join('\n');
    })
    .join('\n----------------------------------------\n\n');

  fs.writeFileSync(
    OUTPUT_TEXT,
    textOutput,
    'utf8'
  );
}
