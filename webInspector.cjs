// inspectWebpage.cjs
//
// STAGE 1
// Reads calendar URLs from web_tda!G2:G.
// Opens each calendar page.
// Finds links associated with populated auction dates.
// Replaces web_tda!C2:C with the discovered auction URLs.
//
// STAGE 2
// Reads the refreshed URLs from web_tda!C2:C.
// Opens each auction result page.
// Navigates through the site's pagination controls.
// Parses, validates, calculates, deduplicates, and exports auction data.
//
// Required packages:
// npm install puppeteer-extra puppeteer-extra-plugin-stealth cheerio googleapis
//
// Required local file:
// ./service-account.json
//
// Important:
// Share the Google Sheet with the service-account email as an Editor.

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const cheerio = require('cheerio');
const fs = require('fs');
const { google } = require('googleapis');

puppeteer.use(StealthPlugin());

// ============================================================
// CONFIGURATION
// ============================================================

const SERVICE_ACCOUNT_FILE = './service-account.json';

const SPREADSHEET_ID =
  '1DvpL59xxVVihpRVhxKUomCpugHLPeokvjKVWKMPAcnw';

const SHEET_NAME = 'web_tda';

// Stage 1 input:
// Calendar-page URLs.
const CALENDAR_URL_RANGE = 'G2:G';

// Stage 1 output and Stage 2 input:
// Individual auction-date URLs.
const AUCTION_URL_RANGE = 'C2:C';

const OUTPUT_ELEMENTS_FILE = 'raw-elements.json';
const OUTPUT_ROWS_FILE = 'parsed-auctions.json';
const OUTPUT_ERRORS_FILE = 'errors.json';
const OUTPUT_SUMMARY_FILE = 'summary.json';
const OUTPUT_CALENDAR_LINKS_FILE = 'calendar-links.json';
const OUTPUT_REJECTIONS_FILE = 'rejected-auctions.json';

const MIN_SURPLUS = 25000;
const MAX_PAGES = 50;

const PAGE_LOAD_TIMEOUT = 120000;
const ELEMENT_WAIT_TIMEOUT = 60000;
const PAGE_CHANGE_TIMEOUT = 30000;

// Exclude auction records when opening/minimum bid is:
// - missing
// - invalid
// - zero
// - negative
const EXCLUDE_ZERO_OR_MISSING_OPENING_BID = true;

// false:
// Retain auctions below the $25,000 threshold and mark Yes/No.
//
// true:
// Retain only records with confirmed sale surplus >= $25,000.
const FILTER_BY_MINIMUM_SURPLUS = false;

// If true, Stage 1 clears C2:C and writes the newly discovered URLs.
const WRITE_DISCOVERED_URLS_TO_SHEET = true;

// Safety feature:
// If all calendars fail, preserve existing values in C2:C.
const PRESERVE_COLUMN_C_IF_ALL_CALENDARS_FAIL = true;

const VIEWPORT = {
  width: 1366,
  height: 768,
  deviceScaleFactor: 1,
};

const TIMEZONE = 'America/New_York';

const BROWSER_USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
  'AppleWebKit/537.36 (KHTML, like Gecko) ' +
  'Chrome/121.0.0.0 Safari/537.36';

const sleep = milliseconds =>
  new Promise(resolve =>
    setTimeout(resolve, milliseconds)
  );

// ============================================================
// GOOGLE SHEETS AUTHENTICATION
// ============================================================

// Full spreadsheets scope is required because Stage 1 writes
// the discovered URLs to column C.
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: [
    'https://www.googleapis.com/auth/spreadsheets',
  ],
});

const sheets = google.sheets({
  version: 'v4',
  auth,
});

// ============================================================
// GENERAL HELPERS
// ============================================================

function clean(value) {
  return String(value || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeAmpersands(value) {
  return String(value || '')
    .replace(/&amp;amp;/gi, '&amp;')
    .replace(/&amp;/gi, '&')
    .replace(/%26amp%3B/gi, '&')
    .trim();
}

function isValidHttpUrl(value) {
  try {
    const parsedUrl = new URL(
      normalizeAmpersands(value)
    );

    return (
      parsedUrl.protocol === 'http:' ||
      parsedUrl.protocol === 'https:'
    );
  } catch {
    return false;
  }
}

function parseCurrency(value) {
  if (
    value === null ||
    value === undefined
  ) {
    return null;
  }

  if (typeof value === 'number') {
    return Number.isFinite(value)
      ? value
      : null;
  }

  const text = String(value).trim();

  if (!text) {
    return null;
  }

  // Converts accounting notation:
  // ($1,250.00) -> -1250.00
  const normalized = text.replace(
    /\(([^)]+)\)/g,
    '-$1'
  );

  const numericText = normalized.replace(
    /[^0-9.-]/g,
    ''
  );

  if (
    !numericText ||
    numericText === '-' ||
    numericText === '.' ||
    numericText === '-.'
  ) {
    return null;
  }

  const parsed =
    Number.parseFloat(numericText);

  return Number.isFinite(parsed)
    ? parsed
    : null;
}

function escapeRegex(value) {
  return String(value).replace(
    /[.*+?^${}()|[\]\\]/g,
    '\\$&'
  );
}

function normalizeLabel(value) {
  return clean(value)
    .toLowerCase()
    .replace(/\s*#\s*$/, '')
    .replace(/\s*:\s*$/, '')
    .trim();
}

function createCanonicalUrlKey(value) {
  try {
    const parsedUrl = new URL(
      normalizeAmpersands(value)
    );

    parsedUrl.hash = '';

    const sortedParameters = [
      ...parsedUrl.searchParams.entries(),
    ].sort(
      ([keyA, valueA], [keyB, valueB]) => {
        const keyComparison =
          keyA.localeCompare(keyB);

        if (keyComparison !== 0) {
          return keyComparison;
        }

        return valueA.localeCompare(valueB);
      }
    );

    parsedUrl.search = '';

    for (
      const [key, parameterValue]
      of sortedParameters
    ) {
      parsedUrl.searchParams.append(
        key,
        parameterValue
      );
    }

    return parsedUrl.toString();
  } catch {
    return clean(value);
  }
}

function deduplicateUrls(urls) {
  const unique = new Map();

  for (const value of urls) {
    const normalized =
      normalizeAmpersands(value);

    if (!isValidHttpUrl(normalized)) {
      continue;
    }

    const key =
      createCanonicalUrlKey(normalized);

    if (!unique.has(key)) {
      unique.set(key, normalized);
    }
  }

  return [...unique.values()];
}

function writeJson(fileName, data) {
  fs.writeFileSync(
    fileName,
    JSON.stringify(data, null, 2),
    'utf8'
  );
}

function removeFileIfExists(fileName) {
  if (fs.existsSync(fileName)) {
    fs.unlinkSync(fileName);
  }
}

// ============================================================
// BROWSER PAGE SETUP
// ============================================================

async function configurePage(page) {
  page.setDefaultNavigationTimeout(
    PAGE_LOAD_TIMEOUT
  );

  page.setDefaultTimeout(
    ELEMENT_WAIT_TIMEOUT
  );

  await page.setViewport(VIEWPORT);

  try {
    await page.emulateTimezone(TIMEZONE);
  } catch (error) {
    console.warn(
      `Timezone warning: ${error.message}`
    );
  }

  await page.setUserAgent(
    BROWSER_USER_AGENT
  );

  await page.setExtraHTTPHeaders({
    'Accept-Language':
      'en-US,en;q=0.9',

    Accept:
      'text/html,' +
      'application/xhtml+xml,' +
      'application/xml;q=0.9,' +
      'image/avif,' +
      'image/webp,' +
      '*/*;q=0.8',
  });
}

// ============================================================
// BLOCKED-PAGE DETECTION
// ============================================================

function detectBlockedPage(
  html,
  statusCode
) {
  const lower = String(html || '')
    .toLowerCase();

  if (statusCode === 403) {
    return 'HTTP 403 Forbidden';
  }

  if (statusCode === 429) {
    return 'HTTP 429 Too Many Requests';
  }

  const blockedPatterns = [
    {
      pattern: '403 forbidden',
      message:
        '403 Forbidden page detected',
    },
    {
      pattern: 'access denied',
      message:
        'Access Denied page detected',
    },
    {
      pattern: 'request blocked',
      message:
        'Request Blocked page detected',
    },
    {
      pattern: 'verify you are human',
      message:
        'Human verification page detected',
    },
    {
      pattern: 'captcha',
      message:
        'CAPTCHA page detected',
    },
    {
      pattern: 'cf-chl-',
      message:
        'Cloudflare challenge detected',
    },
    {
      pattern: 'attention required',
      message:
        'Security challenge page detected',
    },
  ];

  for (const item of blockedPatterns) {
    if (lower.includes(item.pattern)) {
      return item.message;
    }
  }

  return '';
}

// ============================================================
// GOOGLE SHEETS RANGE HELPERS
// ============================================================

async function loadUrlsFromRange(range) {
  const response =
    await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!${range}`,
    });

  return deduplicateUrls(
    (response.data.values || [])
      .flat()
      .map(value =>
        normalizeAmpersands(value)
      )
  );
}

async function loadCalendarUrls() {
  return loadUrlsFromRange(
    CALENDAR_URL_RANGE
  );
}

async function loadTargetUrls() {
  return loadUrlsFromRange(
    AUCTION_URL_RANGE
  );
}

async function writeAuctionUrlsToSheet(
  urls
) {
  const uniqueUrls =
    deduplicateUrls(urls);

  await sheets.spreadsheets.values.clear({
    spreadsheetId: SPREADSHEET_ID,
    range:
      `${SHEET_NAME}!${AUCTION_URL_RANGE}`,
  });

  if (!uniqueUrls.length) {
    console.log(
      `Cleared ${SHEET_NAME}!` +
      `${AUCTION_URL_RANGE}; ` +
      'no auction URLs were discovered.'
    );

    return;
  }

  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!C2`,
    valueInputOption: 'RAW',

    requestBody: {
      values: uniqueUrls.map(
        url => [url]
      ),
    },
  });

  console.log(
    `Wrote ${uniqueUrls.length} unique ` +
    `auction URL(s) to ` +
    `${SHEET_NAME}!${AUCTION_URL_RANGE}`
  );
}

// ============================================================
// STAGE 1: CALENDAR LINK VALIDATION
// ============================================================

function isLikelyAuctionDateUrl(value) {
  try {
    const parsedUrl = new URL(
      normalizeAmpersands(value)
    );

    const action = clean(
      parsedUrl.searchParams.get(
        'zaction'
      )
    ).toUpperCase();

    const method = clean(
      parsedUrl.searchParams.get(
        'zmethod'
      )
    ).toUpperCase();

    const parameterNames = [
      ...parsedUrl.searchParams.keys(),
    ].map(key =>
      key
        .replace(/[^a-z0-9]/gi, '')
        .toLowerCase()
    );

    const hasAuctionDateParameter =
      parameterNames.includes(
        'auctiondate'
      );

    const dateListingMethods =
      new Set([
        'DAYLIST',
        'PREVIEW',
        'AUCTION',
        'BID',
        'SALELIST',
      ]);

    // Reject the calendar URL itself.
    if (
      method === 'CALENDAR' &&
      !hasAuctionDateParameter
    ) {
      return false;
    }

    return (
      hasAuctionDateParameter ||
      dateListingMethods.has(method) ||
      (
        action === 'USER' &&
        /auctiondate=/i.test(
          parsedUrl.search
        )
      )
    );
  } catch {
    return false;
  }
}

// ============================================================
// STAGE 1: LIVE CALENDAR DOM EXTRACTION
// ============================================================

async function extractCalendarAuctionLinks(
  page
) {
  return page.evaluate(() => {
    function normalizeText(value) {
      return String(value || '')
        .replace(/\u00a0/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
    }

    function hasAuctionText(value) {
      const text =
        normalizeText(value);

      return (
        /\btax\s*deed\b/i.test(text) ||
        /\bforeclosure\b/i.test(text) ||
        /\bauction\b/i.test(text) ||
        /\bactive\b/i.test(text) ||
        /\bscheduled\b/i.test(text) ||
        /\b\d+\s*\/\s*\d+\s*TD\b/i.test(
          text
        ) ||
        /\b\d+\s*\/\s*\d+\s*FC\b/i.test(
          text
        )
      );
    }

    function elementLooksLikeCalendar(
      element
    ) {
      const text = normalizeText(
        element.innerText ||
        element.textContent ||
        ''
      );

      const hasWeekdayNames =
        /\bSunday\b/i.test(text) &&
        /\bMonday\b/i.test(text) &&
        /\bTuesday\b/i.test(text) &&
        /\bSaturday\b/i.test(text);

      const hasAuctionCalendarText =
        /\bAuction Calendar\b/i.test(
          text
        ) ||
        /\bTax Deed\b/i.test(text) ||
        /\bForeclosure\b/i.test(text);

      return (
        hasWeekdayNames &&
        hasAuctionCalendarText
      );
    }

    const candidateSelectors = [
      '#CALENDAR',
      '#calendar',
      '.CALENDAR',
      '.calendar',
      '[id*="CALENDAR" i]',
      '[class*="CALENDAR" i]',
      'table',
    ];

    const calendarRoots = [];

    for (
      const selector
      of candidateSelectors
    ) {
      let elements = [];

      try {
        elements = [
          ...document.querySelectorAll(
            selector
          ),
        ];
      } catch {
        elements = [];
      }

      for (const element of elements) {
        if (
          elementLooksLikeCalendar(
            element
          ) &&
          !calendarRoots.includes(
            element
          )
        ) {
          calendarRoots.push(element);
        }
      }
    }

    const rootsToSearch =
      calendarRoots.length
        ? calendarRoots
        : [document.body];

    const found = [];

    for (const root of rootsToSearch) {
      const anchors = [
        ...root.querySelectorAll(
          'a[href]'
        ),
      ];

      for (const anchor of anchors) {
        const rawHref =
          anchor.getAttribute('href') ||
          '';

        const resolvedHref =
          anchor.href ||
          rawHref;

        if (!resolvedHref) {
          continue;
        }

        const anchorText =
          normalizeText(
            anchor.innerText ||
            anchor.textContent ||
            anchor.getAttribute(
              'title'
            ) ||
            ''
          );

        const calendarCell =
          anchor.closest(
            'td, th, ' +
            'div[class*="CAL" i], ' +
            'div[id*="CAL" i], li'
          );

        const cellText =
          normalizeText(
            calendarCell?.innerText ||
            calendarCell?.textContent ||
            anchorText
          );

        const hrefLooksLikeAuction =
          /auctiondate=/i.test(
            resolvedHref
          ) ||
          /zmethod=(daylist|preview|auction|bid|salelist)/i.test(
            resolvedHref
          );

        const textLooksLikeAuction =
          hasAuctionText(anchorText) ||
          hasAuctionText(cellText);

        if (
          hrefLooksLikeAuction ||
          textLooksLikeAuction
        ) {
          found.push({
            href: resolvedHref,
            rawHref,
            anchorText,
            cellText,
          });
        }
      }
    }

    return found;
  });
}

// ============================================================
// STAGE 1: INSPECT ONE CALENDAR
// ============================================================

async function inspectCalendarPage(
  browser,
  calendarUrl
) {
  const page = await browser.newPage();

  try {
    await configurePage(page);

    const normalizedCalendarUrl =
      normalizeAmpersands(calendarUrl);

    console.log(
      `Opening calendar: ` +
      `${normalizedCalendarUrl}`
    );

    const response = await page.goto(
      normalizedCalendarUrl,
      {
        waitUntil: 'networkidle2',
        timeout: PAGE_LOAD_TIMEOUT,
      }
    );

    const statusCode =
      response
        ? response.status()
        : null;

    const initialHtml =
      await page.content();

    const blockedReason =
      detectBlockedPage(
        initialHtml,
        statusCode
      );

    if (blockedReason) {
      throw new Error(blockedReason);
    }

    // Wait for either a recognizable calendar or a link
    // containing an auction date.
    await page.waitForFunction(
      () => {
        const bodyText =
          document.body?.innerText ||
          document.body?.textContent ||
          '';

        const hasCalendarHeaders =
          /\bSunday\b/i.test(bodyText) &&
          /\bMonday\b/i.test(bodyText) &&
          /\bSaturday\b/i.test(bodyText);

        const hasAuctionDateLink =
          Boolean(
            document.querySelector(
              'a[href*="auctiondate=" i], ' +
              'a[href*="zmethod=DAYLIST" i], ' +
              'a[href*="zmethod=PREVIEW" i]'
            )
          );

        return (
          hasCalendarHeaders ||
          hasAuctionDateLink
        );
      },
      {
        timeout:
          ELEMENT_WAIT_TIMEOUT,
      }
    );

    // Allow late calendar scripts to finish rendering.
    await sleep(1200);

    const candidates =
      await extractCalendarAuctionLinks(
        page
      );

    const accepted = [];
    const rejected = [];

    for (const candidate of candidates) {
      let absoluteUrl = '';

      try {
        absoluteUrl = new URL(
          normalizeAmpersands(
            candidate.href
          ),
          normalizedCalendarUrl
        ).toString();
      } catch {
        rejected.push({
          calendarUrl:
            normalizedCalendarUrl,

          href:
            candidate.href,

          reason:
            'InvalidDiscoveredUrl',
        });

        continue;
      }

      if (
        !isLikelyAuctionDateUrl(
          absoluteUrl
        )
      ) {
        rejected.push({
          calendarUrl:
            normalizedCalendarUrl,

          href:
            absoluteUrl,

          anchorText:
            candidate.anchorText,

          cellText:
            candidate.cellText,

          reason:
            'NotAnAuctionDateUrl',
        });

        continue;
      }

      accepted.push({
        calendarUrl:
          normalizedCalendarUrl,

        url:
          absoluteUrl,

        anchorText:
          candidate.anchorText,

        cellText:
          candidate.cellText,
      });
    }

    const uniqueAccepted = new Map();

    for (const item of accepted) {
      const key =
        createCanonicalUrlKey(
          item.url
        );

      if (!uniqueAccepted.has(key)) {
        uniqueAccepted.set(
          key,
          item
        );
      }
    }

    const details = [
      ...uniqueAccepted.values(),
    ];

    console.log(
      `Calendar links accepted: ` +
      `${details.length}`
    );

    return {
      urls:
        details.map(
          item => item.url
        ),

      details,
      rejected,
    };
  } catch (error) {
    const message =
      error?.message ||
      String(error);

    console.error(
      `Calendar error on ` +
      `${calendarUrl}: ${message}`
    );

    return {
      urls: [],
      details: [],
      rejected: [],

      error: {
        url: calendarUrl,
        stage:
          'CalendarDiscovery',
        message,
      },
    };
  } finally {
    try {
      await page.close();
    } catch {
      // Ignore page-close errors.
    }
  }
}

// ============================================================
// STAGE 1: CONTROLLER
// ============================================================

async function refreshAuctionUrlsFromCalendars(
  browser
) {
  console.log('');
  console.log(
    '================================================'
  );
  console.log(
    'STAGE 1: CALENDAR URL DISCOVERY'
  );
  console.log(
    '================================================'
  );

  const calendarUrls =
    await loadCalendarUrls();

  console.log(
    `Found ${calendarUrls.length} ` +
    `unique calendar URL(s) in ` +
    `${SHEET_NAME}!${CALENDAR_URL_RANGE}`
  );

  if (!calendarUrls.length) {
    throw new Error(
      `No valid calendar URLs found in ` +
      `${SHEET_NAME}!` +
      `${CALENDAR_URL_RANGE}`
    );
  }

  const allAuctionUrls = [];
  const discoveryDetails = [];
  const rejectedLinks = [];
  const errors = [];

  let successfulCalendars = 0;

  for (
    let index = 0;
    index < calendarUrls.length;
    index += 1
  ) {
    const calendarUrl =
      calendarUrls[index];

    console.log('');
    console.log(
      `Calendar ${index + 1}/` +
      `${calendarUrls.length}: ` +
      `${calendarUrl}`
    );

    const result =
      await inspectCalendarPage(
        browser,
        calendarUrl
      );

    if (result.error) {
      errors.push(result.error);
    } else {
      successfulCalendars += 1;
    }

    allAuctionUrls.push(
      ...result.urls
    );

    discoveryDetails.push(
      ...result.details
    );

    rejectedLinks.push(
      ...result.rejected
    );

    await sleep(1000);
  }

  const uniqueAuctionUrls =
    deduplicateUrls(
      allAuctionUrls
    );

  if (
    successfulCalendars === 0 &&
    errors.length > 0 &&
    PRESERVE_COLUMN_C_IF_ALL_CALENDARS_FAIL
  ) {
    throw new Error(
      'All calendar URLs failed. ' +
      'Column C was not modified.'
    );
  }

  if (
    WRITE_DISCOVERED_URLS_TO_SHEET
  ) {
    await writeAuctionUrlsToSheet(
      uniqueAuctionUrls
    );
  }

  console.log('');
  console.log(
    `Stage 1 complete: ` +
    `${uniqueAuctionUrls.length} ` +
    'unique auction URL(s) discovered.'
  );

  return {
    calendarUrls,
    auctionUrls:
      uniqueAuctionUrls,
    discoveryDetails,
    rejectedLinks,
    errors,
  };
}

// ============================================================
// STRUCTURED AUCTION FIELD EXTRACTION
// ============================================================

function getByLabel(
  $,
  $context,
  labels
) {
  const targets = (
    Array.isArray(labels)
      ? labels
      : [labels]
  ).map(normalizeLabel);

  let result = '';

  $context
    .find('th, td')
    .each((_, element) => {
      if (result) {
        return false;
      }

      const $element =
        $(element);

      const ownText = clean(
        $element
          .clone()
          .children()
          .remove()
          .end()
          .text()
      );

      const elementText =
        normalizeLabel(
          ownText ||
          $element.text()
        );

      const matched =
        targets.some(
          target =>
            elementText === target
        );

      if (!matched) {
        return;
      }

      const $next =
        $element.next('td, th');

      if ($next.length) {
        result = clean(
          $next.text()
        );

        if (result) {
          return false;
        }
      }

      const cells =
        $element
          .closest('tr')
          .find('th, td')
          .toArray();

      const index =
        cells.indexOf(element);

      if (
        index >= 0 &&
        cells[index + 1]
      ) {
        result = clean(
          $(cells[index + 1])
            .text()
        );

        if (result) {
          return false;
        }
      }
    });

  return result;
}

function extractTextField(
  blockText,
  labels,
  stopLabels = []
) {
  const labelList =
    Array.isArray(labels)
      ? labels
      : [labels];

  for (const label of labelList) {
    const start =
      escapeRegex(label);

    const stops =
      stopLabels
        .map(escapeRegex)
        .join('|');

    const pattern = stops
      ? new RegExp(
          `${start}\\s*#?\\s*:?\\s*` +
          `(.*?)` +
          `(?=(?:${stops})` +
          `\\s*#?\\s*:|$)`,
          'i'
        )
      : new RegExp(
          `${start}\\s*#?\\s*:?\\s*` +
          `(.+)$`,
          'i'
        );

    const match =
      blockText.match(pattern);

    if (
      match &&
      clean(match[1])
    ) {
      return clean(match[1]);
    }
  }

  return '';
}

function extractCurrencyField(
  blockText,
  labels
) {
  for (const label of labels) {
    const escapedLabel =
      escapeRegex(label);

    const pattern = new RegExp(
      `${escapedLabel}` +
      `\\s*#?\\s*:?\\s*` +
      `(\\(?\\s*\\$?\\s*` +
      `-?[0-9][0-9,]*` +
      `(?:\\.[0-9]{1,2})?` +
      `\\s*\\)?)`,
      'i'
    );

    const match =
      blockText.match(pattern);

    if (
      match &&
      parseCurrency(
        match[1]
      ) !== null
    ) {
      return clean(match[1]);
    }
  }

  return '';
}

// ============================================================
// AUCTION FIELD VALIDATION
// ============================================================

function isRealParcelId(value) {
  const normalized =
    normalizeLabel(value);

  if (!normalized) {
    return false;
  }

  return !new Set([
    'property appraiser',
    'parcel id',
    'parcel number',
    'parcel',
    'account number',
    'account',
    'apn',
    'n/a',
    'na',
    'none',
    'null',
    'unknown',
    '-',
    '--',
  ]).has(normalized);
}

function isRealCaseNumber(value) {
  const normalized =
    normalizeLabel(value);

  if (!normalized) {
    return false;
  }

  return !new Set([
    'case',
    'case number',
    'case #',
    'cause number',
    'n/a',
    'none',
    'null',
    'unknown',
    '-',
    '--',
  ]).has(normalized);
}

// ============================================================
// AUCTION DATE EXTRACTION
// ============================================================

function extractAuctionDateFromUrl(url) {
  try {
    const parsedUrl = new URL(
      normalizeAmpersands(url)
    );

    for (
      const [key, value]
      of parsedUrl.searchParams.entries()
    ) {
      const normalizedKey = key
        .replace(/[^a-z]/gi, '')
        .toLowerCase();

      if (
        normalizedKey ===
        'auctiondate'
      ) {
        return clean(value);
      }
    }
  } catch {
    // Return blank below.
  }

  return '';
}

function extractAuctionDateFromText(
  blockText
) {
  const patterns = [
    /Date\/Time\s*:\s*([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4}(?:\s+[0-9]{1,2}:[0-9]{2}\s*(?:AM|PM)?\s*(?:ET|EST|EDT)?)?)/i,

    /Auction Date\s*:\s*([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4})/i,

    /Sale Date\s*:\s*([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4})/i,
  ];

  for (const pattern of patterns) {
    const match =
      blockText.match(pattern);

    if (match) {
      return clean(match[1]);
    }
  }

  return '';
}

// ============================================================
// ADDRESS PROCESSING
// ============================================================

function splitAddressAndCityZip(
  value
) {
  let propertyAddress =
    clean(value);

  let cityStateZip = '';

  const match =
    propertyAddress.match(
      /([A-Za-z .'-]+),?\s+([A-Za-z]{2})\s*-?\s*(\d{5}(?:-\d{4})?)\s*$/i
    );

  if (match) {
    cityStateZip =
      `${clean(match[1])}, ` +
      `${match[2].toUpperCase()} ` +
      `${match[3]}`;

    propertyAddress = clean(
      propertyAddress.slice(
        0,
        match.index
      )
    );
  }

  return {
    propertyAddress,
    cityStateZip,
  };
}

// ============================================================
// AUCTION STATUS DETECTION
// ============================================================

function determineAuctionStatus({
  status,
  blockText,
  itemClass,
}) {
  const statusLower =
    clean(status).toLowerCase();

  const blockLower =
    clean(blockText).toLowerCase();

  const classLower =
    clean(itemClass).toLowerCase();

  if (
    statusLower.includes(
      'redeemed'
    ) ||
    blockLower.includes(
      'redeemed'
    )
  ) {
    return 'Redeemed';
  }

  if (
    statusLower.includes(
      'cancel'
    ) ||
    blockLower.includes(
      'cancelled'
    ) ||
    blockLower.includes(
      'canceled'
    )
  ) {
    return 'Cancelled';
  }

  if (
    statusLower.includes('sold') ||
    statusLower.includes('paid') ||
    blockLower.includes(
      'auction sold'
    ) ||
    blockLower.includes(
      'sold to'
    ) ||
    classLower.includes('sold')
  ) {
    return 'Sold';
  }

  if (
    statusLower.includes(
      'active'
    ) ||
    blockLower.includes(
      'active auction'
    )
  ) {
    return 'Active';
  }

  if (
    classLower.includes(
      'preview'
    ) ||
    blockLower.includes(
      'preview'
    )
  ) {
    return 'Preview';
  }

  return 'Unknown';
}

// ============================================================
// STRICT SALE-PRICE EXTRACTION
// ============================================================

function extractConfirmedSalePrice(
  $,
  $item,
  blockText,
  auctionStatus
) {
  // Do not attempt to extract a sale price unless sold status
  // has been positively identified.
  if (auctionStatus !== 'Sold') {
    return '';
  }

  // Intentionally specific.
  // Do not add a generic "Amount" label because it can match
  // Final Judgment Amount or another unrelated value.
  const strictSalePriceLabels = [
    'Sale Price',
    'Sold Amount',
    'Winning Bid',
    'Final Bid',
    'Sale Amount',
    'Amount Sold',
    'High Bid',
  ];

  let salePrice =
    clean(
      $item
        .find('div.ASTAT_MSGD')
        .first()
        .text()
    ) ||
    clean(
      $item
        .find('.ASTAT_MSGD')
        .first()
        .text()
    );

  if (
    parseCurrency(
      salePrice
    ) === null
  ) {
    salePrice = getByLabel(
      $,
      $item,
      strictSalePriceLabels
    );
  }

  if (
    parseCurrency(
      salePrice
    ) === null
  ) {
    salePrice =
      extractCurrencyField(
        blockText,
        strictSalePriceLabels
      );
  }

  if (
    parseCurrency(
      salePrice
    ) === null
  ) {
    return '';
  }

  return clean(salePrice);
}

// ============================================================
// STAGE 2: PAGE AND PAGER CONTROLS
// ============================================================

async function getPageScopes(page) {
  await page.waitForSelector(
    '#BID_WINDOW_CONTAINER',
    {
      timeout:
        ELEMENT_WAIT_TIMEOUT,
    }
  );

  await page.waitForFunction(
    () => {
      const root =
        document.querySelector(
          '#BID_WINDOW_CONTAINER'
        );

      if (!root) {
        return false;
      }

      const auction =
        root.querySelector(
          'div[aid]'
        );

      const text =
        root.innerText ||
        root.textContent ||
        '';

      const looksEmpty =
        /no\s+(auction|record|result|item)s?\s+(found|available)/i.test(
          text
        );

      return Boolean(
        auction ||
        looksEmpty
      );
    },
    {
      timeout:
        ELEMENT_WAIT_TIMEOUT,
    }
  );

  return {
    async firstRowSignature() {
      return page.$eval(
        '#BID_WINDOW_CONTAINER',
        root => {
          const first =
            root.querySelector(
              'div[aid]'
            );

          if (!first) {
            return '__NONE__';
          }

          const auctionId =
            first.getAttribute(
              'aid'
            ) || '';

          const text =
            first.innerText ||
            first.textContent ||
            '';

          return (
            `${auctionId}|` +
            text
              .replace(/\s+/g, ' ')
              .trim()
          );
        }
      );
    },

    async waitForListChange(
      previousSignature,
      timeoutMilliseconds =
        PAGE_CHANGE_TIMEOUT
    ) {
      const started =
        Date.now();

      while (
        Date.now() - started <
        timeoutMilliseconds
      ) {
        try {
          const current =
            await this
              .firstRowSignature();

          if (
            current !==
            previousSignature
          ) {
            return true;
          }
        } catch {
          // Continue waiting.
        }

        await sleep(400);
      }

      return false;
    },

    async getPagerPieces() {
      const bar = await page.$(
        '#BID_WINDOW_CONTAINER ' +
        '.Head_C > div:nth-of-type(3)'
      );

      if (!bar) {
        return {
          bar: null,
          input: null,
          text: null,
          next: null,
        };
      }

      const input =
        await bar.$(
          "input[type='text'], " +
          "input[type='number'], " +
          "input:not([type])"
        );

      const text =
        await bar.$(
          'span.PageText'
        );

      const next =
        (await bar.$(
          'span.PageRight > img'
        )) ||
        (await bar.$(
          '.PageRight_HVR > img'
        )) ||
        (await bar.$(
          '.PageRight img'
        )) ||
        (await bar.$(
          'img[alt*="next" i], ' +
          'img[title*="next" i]'
        ));

      return {
        bar,
        input,
        text,
        next,
      };
    },

    async readIndicator(pieces) {
      if (
        !pieces ||
        !pieces.bar
      ) {
        return {
          current: null,
          total: null,
          raw: '',
        };
      }

      return page.evaluate(
        bar => {
          const input =
            bar.querySelector(
              'input'
            );

          const text =
            bar.querySelector(
              'span.PageText'
            );

          const inputValue = (
            input?.value || ''
          ).trim();

          const raw = (
            text?.innerText ||
            text?.textContent ||
            ''
          )
            .replace(/\s+/g, ' ')
            .trim();

          const totalMatch =
            raw.match(
              /\bof\s*([0-9]+)/i
            );

          const currentMatch =
            raw.match(
              /\b(?:page\s*)?([0-9]+)\s+of\s+[0-9]+/i
            );

          const currentFromInput =
            /^\d+$/.test(
              inputValue
            )
              ? Number(inputValue)
              : null;

          const currentFromText =
            currentMatch
              ? Number(
                  currentMatch[1]
                )
              : null;

          return {
            current:
              currentFromInput ||
              currentFromText ||
              null,

            total:
              totalMatch
                ? Number(
                    totalMatch[1]
                  )
                : null,

            raw,
          };
        },
        pieces.bar
      );
    },

    async setPageInputAndGo(
      pieces,
      nextPage
    ) {
      if (
        !pieces ||
        !pieces.input
      ) {
        return false;
      }

      try {
        await pieces.input.focus();

        await page.evaluate(
          (element, value) => {
            const descriptor =
              Object
                .getOwnPropertyDescriptor(
                  HTMLInputElement
                    .prototype,
                  'value'
                );

            const setter =
              descriptor?.set;

            if (setter) {
              setter.call(
                element,
                String(value)
              );
            } else {
              element.value =
                String(value);
            }

            element.dispatchEvent(
              new Event('input', {
                bubbles: true,
              })
            );

            element.dispatchEvent(
              new Event('change', {
                bubbles: true,
              })
            );
          },
          pieces.input,
          nextPage
        );

        await page.keyboard.press(
          'Enter'
        );

        return true;
      } catch {
        return false;
      }
    },

    async clickNextArrow(pieces) {
      if (
        !pieces ||
        !pieces.next
      ) {
        return false;
      }

      try {
        await page.evaluate(
          element => {
            element.scrollIntoView({
              block: 'center',
            });

            const clickable =
              element.closest(
                'a, button, ' +
                'input[type="submit"], ' +
                'input[type="button"]'
              );

            (
              clickable ||
              element
            ).click();
          },
          pieces.next
        );

        return true;
      } catch {
        try {
          await pieces.next.click({
            delay: 30,
          });

          return true;
        } catch {
          return false;
        }
      }
    },
  };
}

// ============================================================
// STAGE 2: AUCTION PARSER
// ============================================================

function parseAuctionsFromHtml(
  html,
  pageUrl
) {
  const $ = cheerio.load(html);

  const rows = [];
  const relevantElements = [];
  const rejectedRows = [];

  const openingBidLabels = [
    'Est. Min. Bid',
    'Estimated Minimum Bid',
    'Minimum Bid',
    'Opening Bid',
    'Final Judgment Amount',
  ];

  const assessedValueLabels = [
    'Adjudged Value',
    'Assessed Value',
    'Market Value',
    'Just Value',
    'Appraised Value',
  ];

  $('#BID_WINDOW_CONTAINER div[aid]')
    .each((_, item) => {
      const $item = $(item);

      const blockText =
        clean($item.text());

      const auctionId =
        clean(
          $item.attr('aid')
        );

      const itemClass =
        clean(
          $item.attr('class')
        );

      relevantElements.push({
        sourceUrl: pageUrl,
        tag: 'div',
        attrs:
          $item.attr() || {},
        text:
          blockText.slice(
            0,
            10000
          ),
      });

      // ------------------------------------------------------
      // CASE NUMBER
      // ------------------------------------------------------

      let caseNumber =
        getByLabel(
          $,
          $item,
          [
            'Cause Number',
            'Case Number',
            'Case #',
          ]
        );

      caseNumber ||=
        extractTextField(
          blockText,
          [
            'Case #',
            'Case Number',
            'Cause Number',
          ],
          [
            'Final Judgment Amount',
            'Est. Min. Bid',
            'Estimated Minimum Bid',
            'Minimum Bid',
            'Opening Bid',
            'Parcel ID',
            'Parcel Number',
            'Account Number',
            'Property Address',
            'Assessed Value',
            'Adjudged Value',
            'Plaintiff Max Bid',
          ]
        );

      // ------------------------------------------------------
      // OPENING OR MINIMUM BID
      // ------------------------------------------------------

      let openingBid =
        getByLabel(
          $,
          $item,
          openingBidLabels
        );

      if (
        parseCurrency(
          openingBid
        ) === null
      ) {
        openingBid =
          extractCurrencyField(
            blockText,
            openingBidLabels
          );
      }

      // ------------------------------------------------------
      // PARCEL ID
      // ------------------------------------------------------

      let parcelId =
        getByLabel(
          $,
          $item,
          [
            'Account Number',
            'Parcel Number',
            'Parcel ID',
            'Parcel #',
            'APN',
          ]
        );

      parcelId ||=
        extractTextField(
          blockText,
          [
            'Parcel ID',
            'Parcel Number',
            'Account Number',
            'Parcel #',
            'APN',
          ],
          [
            'Property Address',
            'Assessed Value',
            'Adjudged Value',
            'Plaintiff Max Bid',
            'Final Judgment Amount',
            'Opening Bid',
            'Est. Min. Bid',
            'Estimated Minimum Bid',
          ]
        );

      // ------------------------------------------------------
      // PROPERTY ADDRESS
      // ------------------------------------------------------

      let propertyAddress =
        getByLabel(
          $,
          $item,
          [
            'Property Address',
            'Address',
          ]
        );

      propertyAddress ||=
        extractTextField(
          blockText,
          ['Property Address'],
          [
            'City, State Zip',
            'City/State/Zip',
            'City State Zip',
            'Assessed Value',
            'Adjudged Value',
            'Plaintiff Max Bid',
            'Parcel ID',
            'Final Judgment Amount',
            'Opening Bid',
            'Est. Min. Bid',
          ]
        );

      // ------------------------------------------------------
      // ASSESSED VALUE
      // ------------------------------------------------------

      let assessedValue =
        getByLabel(
          $,
          $item,
          assessedValueLabels
        );

      if (
        parseCurrency(
          assessedValue
        ) === null
      ) {
        assessedValue =
          extractCurrencyField(
            blockText,
            assessedValueLabels
          );
      }

      // ------------------------------------------------------
      // CITY, STATE, ZIP
      // ------------------------------------------------------

      let cityStateZip =
        getByLabel(
          $,
          $item,
          [
            'City, State Zip',
            'City/State/Zip',
            'City State Zip',
            'City State ZIP',
          ]
        );

      if (
        !cityStateZip &&
        propertyAddress
      ) {
        const split =
          splitAddressAndCityZip(
            propertyAddress
          );

        propertyAddress =
          split.propertyAddress;

        cityStateZip =
          split.cityStateZip;
      }

      // ------------------------------------------------------
      // STATUS
      // ------------------------------------------------------

      const status =
        clean(
          $item
            .find(
              'div.ASTAT_MSGA'
            )
            .first()
            .text()
        ) ||
        clean(
          $item
            .find(
              '.status, ' +
              '.ASTAT_MSGA'
            )
            .first()
            .text()
        );

      const auctionStatus =
        determineAuctionStatus({
          status,
          blockText,
          itemClass,
        });

      // Sale price is only extracted for confirmed sold rows.
      const salePrice =
        extractConfirmedSalePrice(
          $,
          $item,
          blockText,
          auctionStatus
        );

      const auctionDate =
        extractAuctionDateFromUrl(
          pageUrl
        ) ||
        extractAuctionDateFromText(
          blockText
        );

      const openingBidNumber =
        parseCurrency(openingBid);

      const salePriceNumber =
        parseCurrency(salePrice);

      const assessedValueNumber =
        parseCurrency(
          assessedValue
        );

      // ------------------------------------------------------
      // REQUIRED VALIDATION
      // ------------------------------------------------------

      if (
        !isRealCaseNumber(
          caseNumber
        )
      ) {
        const rejection = {
          sourceUrl: pageUrl,
          auctionId,
          reason:
            'MissingOrInvalidCaseNumber',
          caseNumber:
            clean(caseNumber),
          openingBid:
            clean(openingBid),
          parcelId:
            clean(parcelId),
        };

        rejectedRows.push(
          rejection
        );

        relevantElements.push({
          sourceUrl: pageUrl,
          tag: 'parse-rejection',
          attrs: {
            aid: auctionId,
            reason:
              'MissingOrInvalidCaseNumber',
          },
          text:
            blockText.slice(
              0,
              10000
            ),
        });

        return;
      }

      if (
        EXCLUDE_ZERO_OR_MISSING_OPENING_BID &&
        (
          openingBidNumber === null ||
          openingBidNumber <= 0
        )
      ) {
        const rejection = {
          sourceUrl: pageUrl,
          auctionId,
          reason:
            'OpeningBidIsBlankInvalidOrZero',
          caseNumber:
            clean(caseNumber),
          openingBid:
            clean(openingBid),
          parcelId:
            clean(parcelId),
        };

        rejectedRows.push(
          rejection
        );

        relevantElements.push({
          sourceUrl: pageUrl,
          tag: 'parse-rejection',
          attrs: {
            aid: auctionId,
            reason:
              'OpeningBidIsBlankInvalidOrZero',
          },
          text:
            blockText.slice(
              0,
              10000
            ),
        });

        return;
      }

      // ------------------------------------------------------
      // CALCULATIONS
      // ------------------------------------------------------

      // Confirmed sold records only:
      // sale price minus opening/minimum bid.
      const saleSurplus =
        auctionStatus === 'Sold' &&
        salePriceNumber !== null &&
        openingBidNumber !== null
          ? Number(
              (
                salePriceNumber -
                openingBidNumber
              ).toFixed(2)
            )
          : null;

      // Assessed value minus confirmed sale price.
      const assessedVsSaleSpread =
        auctionStatus === 'Sold' &&
        assessedValueNumber !== null &&
        salePriceNumber !== null
          ? Number(
              (
                assessedValueNumber -
                salePriceNumber
              ).toFixed(2)
            )
          : null;

      // Assessed value minus opening bid.
      const assessedVsOpeningSpread =
        assessedValueNumber !== null &&
        openingBidNumber !== null
          ? Number(
              (
                assessedValueNumber -
                openingBidNumber
              ).toFixed(2)
            )
          : null;

      const meetsMinimumSurplus =
        saleSurplus === null
          ? ''
          : saleSurplus >=
              MIN_SURPLUS
            ? 'Yes'
            : 'No';

      const row = {
        sourceUrl: pageUrl,
        auctionId,
        auctionStatus,

        sale:
          auctionStatus === 'Sold'
            ? 'Yes'
            : 'No',

        auctionType:
          'Foreclosure',

        caseNumber:
          clean(caseNumber),

        parcelId:
          isRealParcelId(
            parcelId
          )
            ? clean(parcelId)
            : '',

        propertyAddress:
          clean(propertyAddress),

        cityStateZip:
          clean(cityStateZip),

        openingBid:
          clean(openingBid),

        // Blank for Active, Preview, Unknown, Redeemed,
        // and Cancelled auctions.
        salePrice:
          auctionStatus === 'Sold'
            ? clean(salePrice)
            : '',

        assessedValue:
          clean(assessedValue),

        auctionDate:
          clean(auctionDate),

        status:
          clean(status),

        rawClass:
          itemClass,

        surplus:
          saleSurplus,

        saleSurplus,

        assessedVsSaleSpread,

        assessedVsOpeningSpread,

        meetsMinimumSurplus,
      };

      if (
        FILTER_BY_MINIMUM_SURPLUS &&
        row.meetsMinimumSurplus !==
          'Yes'
      ) {
        rejectedRows.push({
          sourceUrl: pageUrl,
          auctionId,

          reason:
            row.saleSurplus === null
              ? 'SaleSurplusUnavailable'
              : 'BelowMinimumSurplusThreshold',

          caseNumber:
            row.caseNumber,

          parcelId:
            row.parcelId,

          openingBid:
            row.openingBid,

          salePrice:
            row.salePrice,

          saleSurplus:
            row.saleSurplus,
        });

        return;
      }

      rows.push(row);
    });

  return {
    rows,
    relevantElements,
    rejectedRows,
  };
}

// ============================================================
// STAGE 2: PROCESS ONE AUCTION URL
// ============================================================

async function inspectAndParse(
  browser,
  url
) {
  const page =
    await browser.newPage();

  const relevantElements = [];
  const parsedRows = [];
  const rejectedRows = [];

  const seenRows =
    new Set();

  const seenPageSignatures =
    new Set();

  try {
    await configurePage(page);

    const normalizedUrl =
      normalizeAmpersands(url);

    console.log(
      `Visiting auction URL: ` +
      `${normalizedUrl}`
    );

    const response =
      await page.goto(
        normalizedUrl,
        {
          waitUntil:
            'networkidle2',

          timeout:
            PAGE_LOAD_TIMEOUT,
        }
      );

    const statusCode =
      response
        ? response.status()
        : null;

    const initialHtml =
      await page.content();

    const blockedReason =
      detectBlockedPage(
        initialHtml,
        statusCode
      );

    if (blockedReason) {
      throw new Error(
        blockedReason
      );
    }

    const scope =
      await getPageScopes(page);

    let pagesProcessed = 0;

    while (
      pagesProcessed <
      MAX_PAGES
    ) {
      console.log(
        `Parsing result page ` +
        `${pagesProcessed + 1} ` +
        `of maximum ${MAX_PAGES}`
      );

      const html =
        await page.content();

      const currentBlockedReason =
        detectBlockedPage(
          html,
          null
        );

      if (
        currentBlockedReason
      ) {
        throw new Error(
          currentBlockedReason
        );
      }

      const currentPageSignature =
        await scope
          .firstRowSignature();

      if (
        seenPageSignatures.has(
          currentPageSignature
        )
      ) {
        console.log(
          'Repeated result page detected. ' +
          'Ending this URL.'
        );

        break;
      }

      seenPageSignatures.add(
        currentPageSignature
      );

      const {
        rows,
        relevantElements:
          currentElements,

        rejectedRows:
          currentRejections,
      } = parseAuctionsFromHtml(
        html,
        normalizedUrl
      );

      for (const row of rows) {
        const key = [
          row.sourceUrl,
          row.caseNumber,
          row.parcelId,
          row.auctionId,
        ].join('|');

        if (!seenRows.has(key)) {
          seenRows.add(key);
          parsedRows.push(row);
        }
      }

      relevantElements.push(
        ...currentElements
      );

      rejectedRows.push(
        ...currentRejections
      );

      console.log(
        `Unique parsed rows so far: ` +
        `${parsedRows.length}`
      );

      pagesProcessed += 1;

      const pieces =
        await scope
          .getPagerPieces();

      if (!pieces.bar) {
        console.log(
          'Pager bar not found. ' +
          'Ending this URL.'
        );

        break;
      }

      const indicator =
        await scope.readIndicator(
          pieces
        );

      const currentPage =
        indicator.current ||
        pagesProcessed;

      const totalPages =
        indicator.total;

      if (
        totalPages &&
        currentPage >= totalPages
      ) {
        console.log(
          `Reached last result page ` +
          `(${currentPage}/` +
          `${totalPages}).`
        );

        break;
      }

      const previousSignature =
        await scope
          .firstRowSignature();

      const nextPage =
        currentPage + 1;

      let changed = false;

      const inputTriggered =
        await scope
          .setPageInputAndGo(
            pieces,
            nextPage
          );

      if (inputTriggered) {
        changed =
          await scope
            .waitForListChange(
              previousSignature
            );
      }

      if (
        !changed &&
        pieces.next
      ) {
        const arrowTriggered =
          await scope
            .clickNextArrow(
              pieces
            );

        if (arrowTriggered) {
          changed =
            await scope
              .waitForListChange(
                previousSignature
              );
        }
      }

      if (!changed) {
        console.log(
          'No list change after pager actions. ' +
          'Ending this URL.'
        );

        break;
      }

      await sleep(800);
    }

    return {
      relevantElements,
      parsedRows,
      rejectedRows,
    };
  } catch (error) {
    const message =
      error?.message ||
      String(error);

    console.error(
      `Auction-page error on ` +
      `${url}: ${message}`
    );

    return {
      relevantElements,
      parsedRows,
      rejectedRows,

      error: {
        url,
        stage:
          'AuctionParsing',
        message,
      },
    };
  } finally {
    try {
      await page.close();
    } catch {
      // Ignore page-close errors.
    }
  }
}

// ============================================================
// GLOBAL AUCTION DEDUPLICATION
// ============================================================

function deduplicateRows(rows) {
  const unique = new Map();

  for (const row of rows) {
    const key = [
      row.sourceUrl,
      row.caseNumber,
      row.parcelId,
      row.auctionId,
    ].join('|');

    if (!unique.has(key)) {
      unique.set(key, row);
    }
  }

  return [...unique.values()];
}

// ============================================================
// MAIN
// ============================================================

(async () => {
  let browser;

  const startedAt =
    new Date();

  try {
    console.log(
      'Launching browser...'
    );

    browser =
      await puppeteer.launch({
        headless: true,

        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-blink-features=AutomationControlled',
          '--ignore-certificate-errors',
          '--disable-gpu',
          '--no-zygote',
          '--window-size=1366,768',
        ],
      });

    // ========================================================
    // STAGE 1
    // G2:G calendar URLs -> C2:C auction-date URLs
    // ========================================================

    const calendarResult =
      await refreshAuctionUrlsFromCalendars(
        browser
      );

    // Save discovery details locally for troubleshooting.
    writeJson(
      OUTPUT_CALENDAR_LINKS_FILE,
      {
        accepted:
          calendarResult
            .discoveryDetails,

        rejected:
          calendarResult
            .rejectedLinks,
      }
    );

    // ========================================================
    // STAGE 2
    // C2:C auction-date URLs -> parsed auction records
    // ========================================================

    console.log('');
    console.log(
      '================================================'
    );
    console.log(
      'STAGE 2: AUCTION PAGE PARSING'
    );
    console.log(
      '================================================'
    );

    const urls =
      await loadTargetUrls();

    console.log(
      `Loaded ${urls.length} unique ` +
      `auction URL(s) from ` +
      `${SHEET_NAME}!${AUCTION_URL_RANGE}`
    );

    if (!urls.length) {
      throw new Error(
        'Stage 1 did not produce valid ' +
        `auction URLs in ` +
        `${SHEET_NAME}!` +
        `${AUCTION_URL_RANGE}`
      );
    }

    const allElements = [];
    const allRows = [];
    const allRejectedRows = [];

    // Include Stage 1 errors in the common errors file.
    const errors = [
      ...calendarResult.errors,
    ];

    for (
      let index = 0;
      index < urls.length;
      index += 1
    ) {
      const url = urls[index];

      console.log('');
      console.log(
        `Processing auction URL ` +
        `${index + 1}/` +
        `${urls.length}: ${url}`
      );

      const result =
        await inspectAndParse(
          browser,
          url
        );

      allElements.push(
        ...result.relevantElements
      );

      allRows.push(
        ...result.parsedRows
      );

      allRejectedRows.push(
        ...result.rejectedRows
      );

      if (result.error) {
        errors.push(
          result.error
        );
      }

      await sleep(1000);
    }

    const finalRows =
      deduplicateRows(
        allRows
      );

    const completedAt =
      new Date();

    const durationSeconds =
      Number(
        (
          (
            completedAt.getTime() -
            startedAt.getTime()
          ) / 1000
        ).toFixed(2)
      );

    const summary = {
      generatedAt:
        completedAt.toISOString(),

      startedAt:
        startedAt.toISOString(),

      durationSeconds,

      calendarDiscovery: {
        sourceRange:
          `${SHEET_NAME}!` +
          `${CALENDAR_URL_RANGE}`,

        destinationRange:
          `${SHEET_NAME}!` +
          `${AUCTION_URL_RANGE}`,

        calendarUrlsProcessed:
          calendarResult
            .calendarUrls
            .length,

        auctionUrlsDiscovered:
          calendarResult
            .auctionUrls
            .length,

        acceptedLinkDetails:
          calendarResult
            .discoveryDetails
            .length,

        rejectedLinkCandidates:
          calendarResult
            .rejectedLinks
            .length,

        discoveryErrors:
          calendarResult
            .errors
            .length,
      },

      configuration: {
        minimumSurplus:
          MIN_SURPLUS,

        maximumPagesPerUrl:
          MAX_PAGES,

        excludeZeroOrMissingOpeningBid:
          EXCLUDE_ZERO_OR_MISSING_OPENING_BID,

        filterByMinimumSurplus:
          FILTER_BY_MINIMUM_SURPLUS,

        writeDiscoveredUrlsToSheet:
          WRITE_DISCOVERED_URLS_TO_SHEET,

        preserveColumnCIfAllCalendarsFail:
          PRESERVE_COLUMN_C_IF_ALL_CALENDARS_FAIL,
      },

      totalUrls:
        urls.length,

      totalElements:
        allElements.length,

      totalRowsRaw:
        allRows.length,

      totalRowsFinal:
        finalRows.length,

      duplicateRowsRemoved:
        allRows.length -
        finalRows.length,

      rejectedAuctionRows:
        allRejectedRows.length,

      errorsCount:
        errors.length,

      statusCounts: {
        sold:
          finalRows.filter(
            row =>
              row.auctionStatus ===
              'Sold'
          ).length,

        active:
          finalRows.filter(
            row =>
              row.auctionStatus ===
              'Active'
          ).length,

        preview:
          finalRows.filter(
            row =>
              row.auctionStatus ===
              'Preview'
          ).length,

        redeemed:
          finalRows.filter(
            row =>
              row.auctionStatus ===
              'Redeemed'
          ).length,

        cancelled:
          finalRows.filter(
            row =>
              row.auctionStatus ===
              'Cancelled'
          ).length,

        unknown:
          finalRows.filter(
            row =>
              row.auctionStatus ===
              'Unknown'
          ).length,
      },

      saleSurplus: {
        aboveOrEqualThreshold:
          finalRows.filter(
            row =>
              row.saleSurplus !==
                null &&
              row.saleSurplus >=
                MIN_SURPLUS
          ).length,

        belowThreshold:
          finalRows.filter(
            row =>
              row.saleSurplus !==
                null &&
              row.saleSurplus <
                MIN_SURPLUS
          ).length,

        unavailable:
          finalRows.filter(
            row =>
              row.saleSurplus ===
              null
          ).length,
      },

      blanks: {
        caseNumberBlank:
          finalRows.filter(
            row =>
              !row.caseNumber
          ).length,

        parcelIdBlank:
          finalRows.filter(
            row =>
              !row.parcelId
          ).length,

        propertyAddressBlank:
          finalRows.filter(
            row =>
              !row.propertyAddress
          ).length,

        openingBidBlank:
          finalRows.filter(
            row =>
              !row.openingBid
          ).length,

        salePriceBlank:
          finalRows.filter(
            row =>
              !row.salePrice
          ).length,

        assessedValueBlank:
          finalRows.filter(
            row =>
              !row.assessedValue
          ).length,

        auctionDateBlank:
          finalRows.filter(
            row =>
              !row.auctionDate
          ).length,

        saleSurplusBlank:
          finalRows.filter(
            row =>
              row.saleSurplus ===
              null
          ).length,
      },

      rejectionReasons:
        allRejectedRows.reduce(
          (counts, row) => {
            const reason =
              row.reason ||
              'Unknown';

            counts[reason] =
              (
                counts[reason] ||
                0
              ) + 1;

            return counts;
          },
          {}
        ),
    };

    // ========================================================
    // WRITE LOCAL OUTPUT FILES
    // ========================================================

    writeJson(
      OUTPUT_ELEMENTS_FILE,
      allElements
    );

    writeJson(
      OUTPUT_ROWS_FILE,
      finalRows
    );

    writeJson(
      OUTPUT_REJECTIONS_FILE,
      allRejectedRows
    );

    writeJson(
      OUTPUT_SUMMARY_FILE,
      summary
    );

    if (errors.length) {
      writeJson(
        OUTPUT_ERRORS_FILE,
        errors
      );
    } else {
      removeFileIfExists(
        OUTPUT_ERRORS_FILE
      );
    }

    console.log('');
    console.log(
      '================================================'
    );
    console.log(
      'PROCESS COMPLETED'
    );
    console.log(
      '================================================'
    );

    console.log(
      `Calendar URLs processed: ` +
      `${calendarResult.calendarUrls.length}`
    );

    console.log(
      `Auction URLs discovered: ` +
      `${calendarResult.auctionUrls.length}`
    );

    console.log(
      `Auction URLs processed: ` +
      `${urls.length}`
    );

    console.log(
      `Unique auctions saved: ` +
      `${finalRows.length}`
    );

    console.log(
      `Rejected auction rows: ` +
      `${allRejectedRows.length}`
    );

    console.log(
      `Errors: ${errors.length}`
    );

    console.log(
      `Saved calendar details -> ` +
      `${OUTPUT_CALENDAR_LINKS_FILE}`
    );

    console.log(
      `Saved raw elements -> ` +
      `${OUTPUT_ELEMENTS_FILE}`
    );

    console.log(
      `Saved parsed auctions -> ` +
      `${OUTPUT_ROWS_FILE}`
    );

    console.log(
      `Saved rejected auctions -> ` +
      `${OUTPUT_REJECTIONS_FILE}`
    );

    console.log(
      `Saved summary -> ` +
      `${OUTPUT_SUMMARY_FILE}`
    );

    if (errors.length) {
      console.log(
        `Saved errors -> ` +
        `${OUTPUT_ERRORS_FILE}`
      );
    }

    console.log(
      `Total duration: ` +
      `${durationSeconds} seconds`
    );
  } catch (error) {
    console.error(
      'Fatal inspectWebpage error:',
      error?.message ||
      error
    );

    process.exitCode = 1;
  } finally {
    if (browser) {
      try {
        await browser.close();
      } catch {
        // Ignore browser-close errors.
      }
    }
  }
})();
