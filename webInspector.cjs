// webInspector.cjs
//
// STAGE 1
// Reads calendar URLs from web_tda!G2:G.
// Finds populated Tax Deed or Foreclosure calendar dates.
// Extracts or constructs auction preview URLs.
// Writes unique preview URLs to web_tda!C2:C.
//
// STAGE 2
// Reads auction preview URLs from web_tda!C2:C.
// Waits for dynamically loaded RealForeclose auction records.
// Parses, validates, deduplicates, and exports auction data.
//
// Required packages:
// npm install puppeteer-extra puppeteer-extra-plugin-stealth cheerio googleapis
//
// Required credentials:
// ./service-account.json
//
// Important:
// Share the Google Sheet with the service-account email as Editor.

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

// Stage 1 source.
const CALENDAR_URL_RANGE = 'G2:G';

// Stage 1 destination and Stage 2 source.
const AUCTION_URL_RANGE = 'C2:C';

const OUTPUT_ELEMENTS_FILE = 'raw-elements.json';
const OUTPUT_ROWS_FILE = 'parsed-auctions.json';
const OUTPUT_ERRORS_FILE = 'errors.json';
const OUTPUT_SUMMARY_FILE = 'summary.json';
const OUTPUT_CALENDAR_LINKS_FILE = 'calendar-links.json';
const OUTPUT_REJECTIONS_FILE = 'rejected-auctions.json';

const MIN_SURPLUS = 25000;
const MAX_PAGES_PER_AREA = 50;

const PAGE_LOAD_TIMEOUT = 120000;
const ELEMENT_WAIT_TIMEOUT = 60000;
const AJAX_WAIT_TIMEOUT = 45000;
const PAGE_CHANGE_TIMEOUT = 30000;

// Exclude records with a blank, invalid, zero, or negative
// opening/minimum bid.
const EXCLUDE_ZERO_OR_MISSING_OPENING_BID = true;

// false:
// Keep all valid records and mark the threshold.
//
// true:
// Keep only records with a confirmed sale surplus of at least
// MIN_SURPLUS.
const FILTER_BY_MINIMUM_SURPLUS = false;

// Write discovered calendar URLs back to C2:C.
const WRITE_DISCOVERED_URLS_TO_SHEET = true;

// Never erase C2:C when Stage 1 finds zero URLs.
const PRESERVE_COLUMN_C_IF_NO_URLS_DISCOVERED = true;

// Preserve C2:C when all source calendars fail.
const PRESERVE_COLUMN_C_IF_ALL_CALENDARS_FAIL = true;

const VIEWPORT = {
  width: 1366,
  height: 768,
  deviceScaleFactor: 1,
};

const TIMEZONE = 'America/New_York';

const USER_AGENT =
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

// Full spreadsheets scope is required because the script writes
// discovered URLs to column C.
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
    .replace(/\\u0026/gi, '&')
    .replace(/\\x26/gi, '&')
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

  // Convert accounting notation:
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

  const parsed = Number.parseFloat(
    numericText
  );

  return Number.isFinite(parsed)
    ? parsed
    : null;
}

function roundCurrency(value) {
  if (!Number.isFinite(value)) {
    return null;
  }

  return Number(value.toFixed(2));
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

function createCanonicalUrlKey(value) {
  try {
    const parsedUrl = new URL(
      normalizeAmpersands(value)
    );

    parsedUrl.hash = '';

    const entries = [
      ...parsedUrl.searchParams.entries(),
    ].sort(
      ([keyA, valueA], [keyB, valueB]) => {
        const keyResult =
          keyA
            .toLowerCase()
            .localeCompare(
              keyB.toLowerCase()
            );

        if (keyResult !== 0) {
          return keyResult;
        }

        return valueA.localeCompare(valueB);
      }
    );

    parsedUrl.search = '';

    for (const [key, parameterValue] of entries) {
      parsedUrl.searchParams.append(
        key.toLowerCase(),
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
      // Preserve the original readable form for Sheets.
      unique.set(key, normalized);
    }
  }

  return [...unique.values()];
}

// ============================================================
// BROWSER PAGE CONFIGURATION
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

  await page.setUserAgent(USER_AGENT);

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
// BLOCKED PAGE DETECTION
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
      message: '403 Forbidden page detected',
    },
    {
      pattern: 'access denied',
      message: 'Access Denied page detected',
    },
    {
      pattern: 'request blocked',
      message: 'Request Blocked page detected',
    },
    {
      pattern: 'verify you are human',
      message: 'Human verification page detected',
    },
    {
      pattern: 'captcha',
      message: 'CAPTCHA page detected',
    },
    {
      pattern: 'cf-chl-',
      message: 'Cloudflare challenge detected',
    },
    {
      pattern: 'attention required',
      message: 'Security challenge page detected',
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
// GOOGLE SHEETS HELPERS
// ============================================================

async function loadUrlsFromRange(range) {
  const response =
    await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!${range}`,
    });

  const values = (
    response.data.values || []
  )
    .flat()
    .map(value =>
      normalizeAmpersands(value)
    );

  return deduplicateUrls(values);
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

async function writeAuctionUrlsToSheet(urls) {
  const uniqueUrls =
    deduplicateUrls(urls);

  if (
    !uniqueUrls.length &&
    PRESERVE_COLUMN_C_IF_NO_URLS_DISCOVERED
  ) {
    console.warn(
      `No auction URLs were discovered. ` +
      `${SHEET_NAME}!${AUCTION_URL_RANGE} ` +
      'was not cleared or modified.'
    );

    return {
      written: 0,
      preservedExistingValues: true,
    };
  }

  await sheets.spreadsheets.values.clear({
    spreadsheetId: SPREADSHEET_ID,
    range:
      `${SHEET_NAME}!${AUCTION_URL_RANGE}`,
  });

  if (!uniqueUrls.length) {
    console.log(
      `Cleared ${SHEET_NAME}!` +
      `${AUCTION_URL_RANGE}; ` +
      'no URLs were written.'
    );

    return {
      written: 0,
      preservedExistingValues: false,
    };
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

  return {
    written: uniqueUrls.length,
    preservedExistingValues: false,
  };
}

// ============================================================
// STAGE 1: AUCTION PREVIEW URL VALIDATION
// ============================================================

function isLikelyAuctionDateUrl(value) {
  try {
    const parsedUrl = new URL(
      normalizeAmpersands(value)
    );

    const parameters = {};

    for (
      const [key, parameterValue]
      of parsedUrl.searchParams.entries()
    ) {
      parameters[key.toLowerCase()] =
        clean(parameterValue);
    }

    const action = String(
      parameters.zaction || ''
    ).toUpperCase();

    const method = String(
      parameters.zmethod || ''
    ).toUpperCase();

    const auctionDate = String(
      parameters.auctiondate || ''
    );

    return (
      action === 'AUCTION' &&
      method === 'PREVIEW' &&
      /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(
        auctionDate
      )
    );
  } catch {
    return false;
  }
}

function buildAuctionPreviewUrl(
  calendarUrl,
  month,
  day,
  year
) {
  const numericMonth = Number(month);
  const numericDay = Number(day);
  const numericYear = Number(year);

  if (
    !Number.isInteger(numericMonth) ||
    !Number.isInteger(numericDay) ||
    !Number.isInteger(numericYear)
  ) {
    return '';
  }

  if (
    numericMonth < 1 ||
    numericMonth > 12 ||
    numericDay < 1 ||
    numericDay > 31 ||
    numericYear < 2000
  ) {
    return '';
  }

  try {
    const calendar = new URL(
      normalizeAmpersands(calendarUrl)
    );

    const auctionDate =
      `${String(numericMonth).padStart(2, '0')}/` +
      `${String(numericDay).padStart(2, '0')}/` +
      `${numericYear}`;

    return (
      `${calendar.origin}/index.cfm` +
      '?zaction=AUCTION' +
      '&zmethod=PREVIEW' +
      `&AuctionDate=${auctionDate}`
    );
  } catch {
    return '';
  }
}

// ============================================================
// STAGE 1: CALENDAR CELL EXTRACTION
// ============================================================

async function extractCalendarAuctionLinks(page) {
  return page.evaluate(() => {
    function cleanText(value) {
      return String(value || '')
        .replace(/\u00a0/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
    }

    function monthNameToNumber(value) {
      const months = {
        january: 1,
        february: 2,
        march: 3,
        april: 4,
        may: 5,
        june: 6,
        july: 7,
        august: 8,
        september: 9,
        october: 10,
        november: 11,
        december: 12,
      };

      return (
        months[
          String(value || '')
            .toLowerCase()
        ] || null
      );
    }

    function looksLikePopulatedAuctionCell(text) {
      const normalized =
        cleanText(text);

      return (
        /\btax\s*deed\b/i.test(normalized) ||
        /\bforeclosure\b/i.test(normalized) ||
        /\b\d+\s*\/\s*\d+\s*TD\b/i.test(
          normalized
        ) ||
        /\b\d+\s*\/\s*\d+\s*FC\b/i.test(
          normalized
        ) ||
        /\b\d+\s+TD\b/i.test(normalized) ||
        /\b\d+\s+FC\b/i.test(normalized)
      );
    }

    function findDisplayedMonthAndYear() {
      const bodyText = cleanText(
        document.body?.innerText ||
        document.body?.textContent ||
        ''
      );

      const patterns = [
        /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b/i,
      ];

      const preferredSelectors = [
        '[class*="month" i]',
        '[id*="month" i]',
        '[class*="calendar" i]',
        '[id*="calendar" i]',
        '#Content_Title',
        'h1',
        'h2',
        'h3',
        'table',
      ];

      for (
        const selector
        of preferredSelectors
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
          const text = cleanText(
            element.innerText ||
            element.textContent ||
            ''
          );

          for (const pattern of patterns) {
            const match =
              text.match(pattern);

            if (match) {
              return {
                monthName: match[1],
                month:
                  monthNameToNumber(
                    match[1]
                  ),
                year: Number(match[2]),
                sourceText: text,
              };
            }
          }
        }
      }

      for (const pattern of patterns) {
        const match =
          bodyText.match(pattern);

        if (match) {
          return {
            monthName: match[1],
            month:
              monthNameToNumber(
                match[1]
              ),
            year: Number(match[2]),
            sourceText: match[0],
          };
        }
      }

      return {
        monthName: '',
        month: null,
        year: null,
        sourceText: '',
      };
    }

    function extractDayNumber(cell) {
      // Prefer direct child text such as the number shown in
      // the corner of a calendar cell.
      const directTextParts = [
        ...cell.childNodes,
      ]
        .filter(node =>
          node.nodeType ===
          Node.TEXT_NODE
        )
        .map(node =>
          cleanText(node.textContent)
        )
        .filter(Boolean);

      for (const part of directTextParts) {
        const match = part.match(
          /^\s*(\d{1,2})\b/
        );

        if (match) {
          const day = Number(match[1]);

          if (day >= 1 && day <= 31) {
            return day;
          }
        }
      }

      const possibleDayElements = [
        ...cell.querySelectorAll(
          'a, span, div, strong, b'
        ),
      ];

      for (
        const element
        of possibleDayElements
      ) {
        const text = cleanText(
          element.innerText ||
          element.textContent ||
          ''
        );

        if (/^\d{1,2}$/.test(text)) {
          const day = Number(text);

          if (day >= 1 && day <= 31) {
            return day;
          }
        }
      }

      const cellText = cleanText(
        cell.innerText ||
        cell.textContent ||
        ''
      );

      const firstNumberMatch =
        cellText.match(
          /^\s*(\d{1,2})\b/
        );

      if (firstNumberMatch) {
        const day = Number(
          firstNumberMatch[1]
        );

        if (day >= 1 && day <= 31) {
          return day;
        }
      }

      return null;
    }

    function extractExistingPreviewUrls(cell) {
      const values = [];

      const elements = [
        cell,
        ...cell.querySelectorAll('*'),
      ];

      for (const element of elements) {
        if (
          element.tagName
            ?.toLowerCase() === 'a'
        ) {
          values.push(
            element.href || '',
            element.getAttribute(
              'href'
            ) || ''
          );
        }

        for (
          const attribute
          of element.attributes || []
        ) {
          values.push(
            attribute.value || ''
          );
        }

        values.push(
          element.outerHTML || ''
        );
      }

      const discovered = [];

      for (let value of values) {
        value = String(value || '')
          .replace(/&amp;/gi, '&')
          .replace(/\\u0026/gi, '&')
          .replace(/\\x26/gi, '&')
          .replace(/\\\//g, '/');

        const matches = value.match(
          /(?:https?:\/\/[^\s"'<>\\)]+)?\/?index\.cfm\?zaction=AUCTION&zmethod=PREVIEW&AuctionDate=\d{1,2}\/\d{1,2}\/\d{4}/gi
        ) || [];

        discovered.push(...matches);
      }

      return [...new Set(discovered)];
    }

    const displayedDate =
      findDisplayedMonthAndYear();

    const findings = [];

    // The displayed RealForeclose calendar is table based.
    const cells = [
      ...document.querySelectorAll('td'),
    ];

    for (const cell of cells) {
      const cellText = cleanText(
        cell.innerText ||
        cell.textContent ||
        ''
      );

      if (
        !looksLikePopulatedAuctionCell(
          cellText
        )
      ) {
        continue;
      }

      const existingUrls =
        extractExistingPreviewUrls(cell);

      const day =
        extractDayNumber(cell);

      findings.push({
        sourceType:
          existingUrls.length
            ? 'existing-url'
            : 'derived-date',

        urls: existingUrls,

        cellText,
        day,

        month:
          displayedDate.month,

        year:
          displayedDate.year,

        monthSource:
          displayedDate.sourceText,

        cellHtml: (
          cell.outerHTML || ''
        ).slice(0, 15000),
      });
    }

    return findings;
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

    await page.waitForFunction(
      () => {
        const bodyText =
          document.body?.innerText ||
          document.body?.textContent ||
          '';

        const hasWeekHeaders =
          /\bSunday\b/i.test(bodyText) &&
          /\bMonday\b/i.test(bodyText) &&
          /\bSaturday\b/i.test(bodyText);

        const hasDateHeading =
          /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b/i.test(
            bodyText
          );

        return (
          hasWeekHeaders &&
          hasDateHeading
        );
      },
      {
        timeout:
          ELEMENT_WAIT_TIMEOUT,
      }
    );

    await sleep(1200);

    const candidates =
      await extractCalendarAuctionLinks(
        page
      );

    console.log(
      `Populated calendar cells found: ` +
      `${candidates.length}`
    );

    const accepted = [];
    const rejected = [];

    for (const candidate of candidates) {
      const possibleUrls = [
        ...(candidate.urls || []),
      ];

      if (
        !possibleUrls.length &&
        candidate.sourceType ===
          'derived-date'
      ) {
        const generatedUrl =
          buildAuctionPreviewUrl(
            normalizedCalendarUrl,
            candidate.month,
            candidate.day,
            candidate.year
          );

        if (generatedUrl) {
          possibleUrls.push(
            generatedUrl
          );
        }
      }

      if (!possibleUrls.length) {
        rejected.push({
          calendarUrl:
            normalizedCalendarUrl,

          reason:
            'UnableToExtractOrDeriveAuctionUrl',

          sourceType:
            candidate.sourceType,

          cellText:
            candidate.cellText,

          day:
            candidate.day,

          month:
            candidate.month,

          year:
            candidate.year,

          monthSource:
            candidate.monthSource,

          cellHtml:
            candidate.cellHtml,
        });

        continue;
      }

      for (
        const discoveredValue
        of possibleUrls
      ) {
        let absoluteUrl = '';

        try {
          absoluteUrl = new URL(
            normalizeAmpersands(
              discoveredValue
            ),
            normalizedCalendarUrl
          ).toString();
        } catch {
          rejected.push({
            calendarUrl:
              normalizedCalendarUrl,

            reason:
              'InvalidDiscoveredUrl',

            discoveredValue,

            cellText:
              candidate.cellText,
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

            reason:
              'NotAnAuctionPreviewUrl',

            href:
              absoluteUrl,

            sourceType:
              candidate.sourceType,

            cellText:
              candidate.cellText,
          });

          continue;
        }

        accepted.push({
          calendarUrl:
            normalizedCalendarUrl,

          url:
            absoluteUrl,

          sourceType:
            candidate.sourceType,

          cellText:
            candidate.cellText,

          day:
            candidate.day,

          month:
            candidate.month,

          year:
            candidate.year,
        });
      }
    }

    const uniqueAccepted =
      new Map();

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

    for (const item of details) {
      console.log(
        `  ${item.sourceType}: ` +
        `${item.url}`
      );
    }

    if (!details.length) {
      console.warn(
        'No auction preview URLs were accepted.'
      );
    }

    return {
      urls: details.map(
        item => item.url
      ),

      details,
      rejected,

      candidatesFound:
        candidates.length,
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
      candidatesFound: 0,

      error: {
        url: calendarUrl,
        stage: 'CalendarDiscovery',
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
    `Found ${calendarUrls.length} unique ` +
    `calendar URL(s) in ` +
    `${SHEET_NAME}!${CALENDAR_URL_RANGE}`
  );

  if (!calendarUrls.length) {
    throw new Error(
      `No valid calendar URLs found in ` +
      `${SHEET_NAME}!${CALENDAR_URL_RANGE}`
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

  if (!uniqueAuctionUrls.length) {
    console.warn(
      'Stage 1 discovered zero auction preview URLs.'
    );

    console.warn(
      `${SHEET_NAME}!${AUCTION_URL_RANGE} ` +
      'will be preserved.'
    );
  }

  let sheetWriteResult = {
    written: 0,
    preservedExistingValues: true,
  };

  if (WRITE_DISCOVERED_URLS_TO_SHEET) {
    sheetWriteResult =
      await writeAuctionUrlsToSheet(
        uniqueAuctionUrls
      );
  }

  console.log('');
  console.log(
    `Stage 1 complete: ` +
    `${uniqueAuctionUrls.length} unique ` +
    'auction URL(s) discovered.'
  );

  return {
    calendarUrls,
    auctionUrls:
      uniqueAuctionUrls,
    discoveryDetails,
    rejectedLinks,
    errors,
    sheetWriteResult,
  };
}

// ============================================================
// STRUCTURED FIELD EXTRACTION
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
      parseCurrency(match[1]) !== null
    ) {
      return clean(match[1]);
    }
  }

  return '';
}

// ============================================================
// FIELD VALIDATION
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
      if (
        key
          .replace(/[^a-z]/gi, '')
          .toLowerCase() ===
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

function splitAddressAndCityZip(value) {
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
// STATUS AND SALE PRICE
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
    statusLower.includes('redeemed') ||
    blockLower.includes('redeemed')
  ) {
    return 'Redeemed';
  }

  if (
    statusLower.includes('cancel') ||
    blockLower.includes('cancelled') ||
    blockLower.includes('canceled')
  ) {
    return 'Cancelled';
  }

  if (
    statusLower.includes('sold') ||
    statusLower.includes('paid') ||
    blockLower.includes('auction sold') ||
    blockLower.includes('sold to') ||
    classLower.includes('sold')
  ) {
    return 'Sold';
  }

  if (
    statusLower.includes('active') ||
    blockLower.includes('active auction')
  ) {
    return 'Active';
  }

  if (
    classLower.includes('preview') ||
    blockLower.includes('preview')
  ) {
    return 'Preview';
  }

  return 'Unknown';
}

function extractConfirmedSalePrice(
  $,
  $item,
  blockText,
  auctionStatus
) {
  if (auctionStatus !== 'Sold') {
    return '';
  }

  // Do not add the generic label "Amount".
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
    parseCurrency(salePrice) === null
  ) {
    salePrice = getByLabel(
      $,
      $item,
      strictSalePriceLabels
    );
  }

  if (
    parseCurrency(salePrice) === null
  ) {
    salePrice =
      extractCurrencyField(
        blockText,
        strictSalePriceLabels
      );
  }

  if (
    parseCurrency(salePrice) === null
  ) {
    return '';
  }

  return clean(salePrice);
}

// ============================================================
// STAGE 2: WAIT FOR DYNAMIC AUCTION DATA
// ============================================================

async function waitForAuctionData(page) {
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

      const auctionRecord =
        root.querySelector(
          'div[aid]'
        );

      if (auctionRecord) {
        return true;
      }

      const rootText =
        root.innerText ||
        root.textContent ||
        '';

      const noCases =
        /there are no cases currently being auctioned/i.test(
          rootText
        ) ||
        /no\s+(auction|record|result|item)s?\s+(found|available)/i.test(
          rootText
        );

      if (noCases) {
        return true;
      }

      const auctionList = (
        document.querySelector(
          '#ALB'
        )?.textContent || ''
      ).trim();

      const possibleIds =
        auctionList
          .split(',')
          .map(value => value.trim())
          .filter(Boolean);

      const pageIndicators = [
        ...root.querySelectorAll(
          '.PageFrame input'
        ),
      ];

      const pageInputsReady =
        pageIndicators.some(input => {
          const value =
            String(input.value || '')
              .trim();

          const currentPage =
            String(
              input.getAttribute(
                'curPG'
              ) || ''
            ).trim();

          return Boolean(
            value ||
            currentPage
          );
        });

      const visibleCards = [
        ...root.querySelectorAll(
          '#Area_R > div, ' +
          '#Area_W > div, ' +
          '#Area_C > div'
        ),
      ].some(element =>
        !element.classList.contains(
          'Loading'
        )
      );

      return Boolean(
        visibleCards ||
        pageInputsReady ||
        possibleIds.length === 0
      );
    },
    {
      timeout:
        AJAX_WAIT_TIMEOUT,
    }
  );

  // Allow the last AJAX-render cycle to finish.
  await sleep(1500);
}

// ============================================================
// STAGE 2: AREA AND PAGINATION HELPERS
// ============================================================

async function getAreaState(
  page,
  areaCode
) {
  return page.evaluate(code => {
    const area =
      document.querySelector(
        `#Area_${code}`
      );

    const pageFrames = [
      ...document.querySelectorAll(
        `.PageFrame[area="${code}"]`
      ),
    ];

    const firstAuction =
      area?.querySelector(
        'div[aid]'
      );

    const firstSignature =
      firstAuction
        ? (
            `${
              firstAuction.getAttribute(
                'aid'
              ) || ''
            }|${
              (
                firstAuction.innerText ||
                firstAuction.textContent ||
                ''
              )
                .replace(/\s+/g, ' ')
                .trim()
            }`
          )
        : '__NONE__';

    let current = null;
    let total = null;

    for (const frame of pageFrames) {
      const input =
        frame.querySelector(
          'input'
        );

      const totalElement =
        frame.querySelector(
          '[id^="max"]'
        );

      const inputValue =
        String(
          input?.value ||
          input?.getAttribute(
            'curPG'
          ) ||
          ''
        ).trim();

      const totalValue =
        String(
          totalElement?.textContent ||
          ''
        ).trim();

      if (
        current === null &&
        /^\d+$/.test(inputValue)
      ) {
        current =
          Number(inputValue);
      }

      if (
        total === null &&
        /^\d+$/.test(totalValue)
      ) {
        total =
          Number(totalValue);
      }
    }

    return {
      exists: Boolean(area),
      firstSignature,
      recordCount:
        area
          ? area.querySelectorAll(
              'div[aid]'
            ).length
          : 0,
      current,
      total,
    };
  }, areaCode);
}

async function waitForAreaChange(
  page,
  areaCode,
  previousSignature,
  timeoutMilliseconds =
    PAGE_CHANGE_TIMEOUT
) {
  const started = Date.now();

  while (
    Date.now() - started <
    timeoutMilliseconds
  ) {
    const state =
      await getAreaState(
        page,
        areaCode
      );

    if (
      state.firstSignature !==
      previousSignature
    ) {
      await sleep(500);
      return true;
    }

    await sleep(400);
  }

  return false;
}

async function moveAreaToNextPage(
  page,
  areaCode,
  nextPage,
  previousSignature
) {
  const inputSelector =
    `.PageFrame[area="${areaCode}"] ` +
    'input';

  const nextSelector =
    `.PageFrame[area="${areaCode}"] ` +
    '.PageRight';

  const input =
    await page.$(inputSelector);

  if (input) {
    try {
      await input.focus();

      await page.evaluate(
        (element, value) => {
          const descriptor =
            Object
              .getOwnPropertyDescriptor(
                HTMLInputElement.prototype,
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
        input,
        nextPage
      );

      await page.keyboard.press('Enter');

      const changed =
        await waitForAreaChange(
          page,
          areaCode,
          previousSignature
        );

      if (changed) {
        return true;
      }
    } catch {
      // Fall back to next arrow.
    }
  }

  const nextArrow =
    await page.$(nextSelector);

  if (!nextArrow) {
    return false;
  }

  try {
    await page.evaluate(element => {
      element.scrollIntoView({
        block: 'center',
      });

      const clickable =
        element.closest(
          'a, button'
        );

      (
        clickable ||
        element
      ).click();
    }, nextArrow);

    return waitForAreaChange(
      page,
      areaCode,
      previousSignature
    );
  } catch {
    try {
      await nextArrow.click({
        delay: 30,
      });

      return waitForAreaChange(
        page,
        areaCode,
        previousSignature
      );
    } catch {
      return false;
    }
  }
}

// ============================================================
// STAGE 2: AUCTION HTML PARSER
// ============================================================

function parseAuctionsFromHtml(
  html,
  pageUrl,
  areaCode = ''
) {
  const $ = cheerio.load(html);

  const rows = [];
  const relevantElements = [];
  const rejectedRows = [];

  const selector = areaCode
    ? `#Area_${areaCode} div[aid]`
    : '#BID_WINDOW_CONTAINER div[aid]';

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

  $(selector).each((_, item) => {
    const $item = $(item);

    const blockText =
      clean($item.text());

    const auctionId =
      clean($item.attr('aid'));

    const itemClass =
      clean($item.attr('class'));

    relevantElements.push({
      sourceUrl: pageUrl,
      areaCode,
      tag: 'div',
      attrs:
        $item.attr() || {},
      text:
        blockText.slice(
          0,
          10000
        ),
    });

    // --------------------------------------------------------
    // Case number
    // --------------------------------------------------------

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

    // --------------------------------------------------------
    // Opening or minimum bid
    // --------------------------------------------------------

    let openingBid =
      getByLabel(
        $,
        $item,
        openingBidLabels
      );

    if (
      parseCurrency(openingBid) === null
    ) {
      openingBid =
        extractCurrencyField(
          blockText,
          openingBidLabels
        );
    }

    // --------------------------------------------------------
    // Parcel ID
    // --------------------------------------------------------

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

    // --------------------------------------------------------
    // Property address
    // --------------------------------------------------------

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

    // --------------------------------------------------------
    // Assessed value
    // --------------------------------------------------------

    let assessedValue =
      getByLabel(
        $,
        $item,
        assessedValueLabels
      );

    if (
      parseCurrency(assessedValue) === null
    ) {
      assessedValue =
        extractCurrencyField(
          blockText,
          assessedValueLabels
        );
    }

    // --------------------------------------------------------
    // City, state, and ZIP
    // --------------------------------------------------------

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

    // --------------------------------------------------------
    // Status
    // --------------------------------------------------------

    const status =
      clean(
        $item
          .find('div.ASTAT_MSGA')
          .first()
          .text()
      ) ||
      clean(
        $item
          .find('.status, .ASTAT_MSGA')
          .first()
          .text()
      );

    const auctionStatus =
      determineAuctionStatus({
        status,
        blockText,
        itemClass,
      });

    // Sale price remains blank unless sold status is confirmed.
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
      parseCurrency(assessedValue);

    // --------------------------------------------------------
    // Validation
    // --------------------------------------------------------

    if (
      !isRealCaseNumber(
        caseNumber
      )
    ) {
      const rejection = {
        sourceUrl: pageUrl,
        areaCode,
        auctionId,
        reason:
          'MissingOrInvalidCaseNumber',
        caseNumber:
          clean(caseNumber),
        parcelId:
          clean(parcelId),
        openingBid:
          clean(openingBid),
      };

      rejectedRows.push(rejection);

      relevantElements.push({
        sourceUrl: pageUrl,
        areaCode,
        tag: 'parse-rejection',
        attrs: {
          aid: auctionId,
          reason:
            rejection.reason,
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
        areaCode,
        auctionId,
        reason:
          'OpeningBidIsBlankInvalidOrZero',
        caseNumber:
          clean(caseNumber),
        parcelId:
          clean(parcelId),
        openingBid:
          clean(openingBid),
      };

      rejectedRows.push(rejection);

      relevantElements.push({
        sourceUrl: pageUrl,
        areaCode,
        tag: 'parse-rejection',
        attrs: {
          aid: auctionId,
          reason:
            rejection.reason,
        },
        text:
          blockText.slice(
            0,
            10000
          ),
      });

      return;
    }

    // --------------------------------------------------------
    // Calculations
    // --------------------------------------------------------

    const saleSurplus =
      auctionStatus === 'Sold' &&
      salePriceNumber !== null &&
      openingBidNumber !== null
        ? roundCurrency(
            salePriceNumber -
            openingBidNumber
          )
        : null;

    const assessedVsSaleSpread =
      auctionStatus === 'Sold' &&
      assessedValueNumber !== null &&
      salePriceNumber !== null
        ? roundCurrency(
            assessedValueNumber -
            salePriceNumber
          )
        : null;

    const assessedVsOpeningSpread =
      assessedValueNumber !== null &&
      openingBidNumber !== null
        ? roundCurrency(
            assessedValueNumber -
            openingBidNumber
          )
        : null;

    const meetsMinimumSurplus =
      saleSurplus === null
        ? ''
        : saleSurplus >= MIN_SURPLUS
          ? 'Yes'
          : 'No';

    const row = {
      sourceUrl: pageUrl,
      areaCode,
      auctionId,
      auctionStatus,

      sale:
        auctionStatus === 'Sold'
          ? 'Yes'
          : 'No',

      auctionType: 'Foreclosure',

      caseNumber:
        clean(caseNumber),

      parcelId:
        isRealParcelId(parcelId)
          ? clean(parcelId)
          : '',

      propertyAddress:
        clean(propertyAddress),

      cityStateZip:
        clean(cityStateZip),

      openingBid:
        clean(openingBid),

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
      row.meetsMinimumSurplus !== 'Yes'
    ) {
      rejectedRows.push({
        sourceUrl: pageUrl,
        areaCode,
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
          waitUntil: 'networkidle2',
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

    await waitForAuctionData(page);

    const areaCodes = [
      'R',
      'W',
      'C',
    ];

    for (const areaCode of areaCodes) {
      console.log(
        `Processing auction area: ` +
        `${areaCode}`
      );

      const seenAreaPages =
        new Set();

      let pagesProcessed = 0;

      while (
        pagesProcessed <
        MAX_PAGES_PER_AREA
      ) {
        const state =
          await getAreaState(
            page,
            areaCode
          );

        if (!state.exists) {
          console.log(
            `Area ${areaCode} was not found.`
          );

          break;
        }

        if (
          seenAreaPages.has(
            state.firstSignature
          )
        ) {
          console.log(
            `Repeated page detected in ` +
            `area ${areaCode}.`
          );

          break;
        }

        seenAreaPages.add(
          state.firstSignature
        );

        const html =
          await page.content();

        const {
          rows,
          relevantElements:
            currentElements,
          rejectedRows:
            currentRejections,
        } = parseAuctionsFromHtml(
          html,
          normalizedUrl,
          areaCode
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

        pagesProcessed += 1;

        console.log(
          `Area ${areaCode}, page ` +
          `${state.current || pagesProcessed}, ` +
          `records: ${state.recordCount}, ` +
          `unique total: ${parsedRows.length}`
        );

        if (
          state.total &&
          state.current &&
          state.current >= state.total
        ) {
          break;
        }

        if (
          !state.recordCount &&
          !state.total
        ) {
          break;
        }

        const currentPage =
          state.current ||
          pagesProcessed;

        const nextPage =
          currentPage + 1;

        const changed =
          await moveAreaToNextPage(
            page,
            areaCode,
            nextPage,
            state.firstSignature
          );

        if (!changed) {
          break;
        }

        await sleep(800);
      }
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
        stage: 'AuctionParsing',
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
    // G2:G calendars -> C2:C auction preview URLs
    // ========================================================

    const calendarResult =
      await refreshAuctionUrlsFromCalendars(
        browser
      );

    writeJson(
      OUTPUT_CALENDAR_LINKS_FILE,
      {
        accepted:
          calendarResult
            .discoveryDetails,

        rejected:
          calendarResult
            .rejectedLinks,

        sheetWriteResult:
          calendarResult
            .sheetWriteResult,
      }
    );

    // ========================================================
    // STAGE 2
    // C2:C auction preview URLs -> auction records
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
        'No auction URLs are available for Stage 2. ' +
        'Stage 1 discovered zero URLs and column C is empty. ' +
        `Inspect ${OUTPUT_CALENDAR_LINKS_FILE} for details.`
      );
    }

    const allElements = [];
    const allRows = [];
    const allRejectedRows = [];

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
        `${index + 1}/${urls.length}: ` +
        `${url}`
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
      roundCurrency(
        (
          completedAt.getTime() -
          startedAt.getTime()
        ) / 1000
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

        acceptedLinks:
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

        urlsWritten:
          calendarResult
            .sheetWriteResult
            .written,

        preservedExistingColumnC:
          calendarResult
            .sheetWriteResult
            .preservedExistingValues,
      },

      configuration: {
        minimumSurplus:
          MIN_SURPLUS,

        maximumPagesPerArea:
          MAX_PAGES_PER_AREA,

        excludeZeroOrMissingOpeningBid:
          EXCLUDE_ZERO_OR_MISSING_OPENING_BID,

        filterByMinimumSurplus:
          FILTER_BY_MINIMUM_SURPLUS,

        writeDiscoveredUrlsToSheet:
          WRITE_DISCOVERED_URLS_TO_SHEET,

        preserveColumnCIfNoUrlsDiscovered:
          PRESERVE_COLUMN_C_IF_NO_URLS_DISCOVERED,

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
              row.saleSurplus !== null &&
              row.saleSurplus >=
                MIN_SURPLUS
          ).length,

        belowThreshold:
          finalRows.filter(
            row =>
              row.saleSurplus !== null &&
              row.saleSurplus <
                MIN_SURPLUS
          ).length,

        unavailable:
          finalRows.filter(
            row =>
              row.saleSurplus === null
          ).length,
      },

      blanks: {
        caseNumberBlank:
          finalRows.filter(
            row => !row.caseNumber
          ).length,

        parcelIdBlank:
          finalRows.filter(
            row => !row.parcelId
          ).length,

        propertyAddressBlank:
          finalRows.filter(
            row => !row.propertyAddress
          ).length,

        openingBidBlank:
          finalRows.filter(
            row => !row.openingBid
          ).length,

        salePriceBlank:
          finalRows.filter(
            row => !row.salePrice
          ).length,

        assessedValueBlank:
          finalRows.filter(
            row => !row.assessedValue
          ).length,

        auctionDateBlank:
          finalRows.filter(
            row => !row.auctionDate
          ).length,

        saleSurplusBlank:
          finalRows.filter(
            row =>
              row.saleSurplus === null
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
    // WRITE OUTPUT FILES
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
      'Fatal webInspector error:',
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
