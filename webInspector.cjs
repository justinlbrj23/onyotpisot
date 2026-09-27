// inspectWebpage.cjs
// RealForeclose auction inspector, parser, evaluator, and validator
//
// Required packages:
// npm install puppeteer-extra puppeteer-extra-plugin-stealth cheerio googleapis
//
// Required local file:
// ./service-account.json
//
// Google Sheet source:
// web_tda!C2:C
//
// Important:
// Share the Google Sheet with the service account email address.

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
const URL_RANGE = 'C2:C';

const OUTPUT_ELEMENTS_FILE = 'raw-elements.json';
const OUTPUT_ROWS_FILE = 'parsed-auctions.json';
const OUTPUT_ERRORS_FILE = 'errors.json';
const OUTPUT_SUMMARY_FILE = 'summary.json';

const MIN_SURPLUS = 25000;
const MAX_PAGES = 50;

const PAGE_LOAD_TIMEOUT = 120000;
const ELEMENT_WAIT_TIMEOUT = 60000;
const PAGE_CHANGE_TIMEOUT = 30000;

// Filters
const EXCLUDE_ZERO_OR_MISSING_OPENING_BID = true;

// Set this to true only if you want to exclude records under $25,000.
// By default, records are retained and marked Yes or No.
const FILTER_BY_MINIMUM_SURPLUS = false;

// Browser settings
const VIEWPORT = {
  width: 1366,
  height: 768,
  deviceScaleFactor: 1,
};

const TIMEZONE = 'America/New_York';

const sleep = ms =>
  new Promise(resolve => setTimeout(resolve, ms));

// ============================================================
// GOOGLE SHEETS AUTHENTICATION
// ============================================================

const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
  ],
});

const sheets = google.sheets({
  version: 'v4',
  auth,
});

// ============================================================
// URL HELPERS
// ============================================================

function normalizeAmpersands(value) {
  return String(value || '')
    .replace(/&amp;amp;/gi, '&amp;')
    .replace(/&amp;/gi, '&')
    .replace(/%26amp%3B/gi, '&')
    .trim();
}

function isValidHttpUrl(value) {
  try {
    const parsed = new URL(normalizeAmpersands(value));
    return parsed.protocol === 'http:' || parsed.protocol === 'https:';
  } catch {
    return false;
  }
}

async function loadTargetUrls() {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${URL_RANGE}`,
  });

  const urls = (response.data.values || [])
    .flat()
    .map(value => normalizeAmpersands(value))
    .filter(isValidHttpUrl);

  // Remove duplicate URLs from the Google Sheet.
  return [...new Set(urls)];
}

// ============================================================
// GENERAL TEXT HELPERS
// ============================================================

function clean(value) {
  return String(value || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseCurrency(value) {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }

  const text = String(value).trim();

  if (!text) {
    return null;
  }

  const cleaned = text
    .replace(/\(([^)]+)\)/g, '-$1')
    .replace(/[^0-9.-]/g, '');

  if (
    !cleaned ||
    cleaned === '-' ||
    cleaned === '.' ||
    cleaned === '-.'
  ) {
    return null;
  }

  const parsed = Number.parseFloat(cleaned);

  return Number.isFinite(parsed) ? parsed : null;
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

function isPlaceholderValue(value) {
  const normalized = normalizeLabel(value);

  return new Set([
    '',
    'n/a',
    'na',
    'none',
    'null',
    'unknown',
    'not available',
    '-',
    '--',
  ]).has(normalized);
}

// ============================================================
// FIELD EXTRACTION
// ============================================================

function getByLabel($, $context, labels) {
  const targets = (
    Array.isArray(labels) ? labels : [labels]
  ).map(normalizeLabel);

  let result = '';

  $context.find('th, td').each((_, element) => {
    if (result) {
      return false;
    }

    const $element = $(element);

    const ownText = clean(
      $element
        .clone()
        .children()
        .remove()
        .end()
        .text()
    );

    const elementText = normalizeLabel(
      ownText || $element.text()
    );

    const matched = targets.some(
      target => elementText === target
    );

    if (!matched) {
      return;
    }

    const $next = $element.next('td, th');

    if ($next.length) {
      result = clean($next.text());

      if (result) {
        return false;
      }
    }

    const cells = $element
      .closest('tr')
      .find('th, td')
      .toArray();

    const index = cells.indexOf(element);

    if (index >= 0 && cells[index + 1]) {
      result = clean($(cells[index + 1]).text());

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
  const labelList = Array.isArray(labels)
    ? labels
    : [labels];

  for (const label of labelList) {
    const start = escapeRegex(label);

    const stops = stopLabels
      .map(escapeRegex)
      .join('|');

    const pattern = stops
      ? new RegExp(
          `${start}\\s*#?\\s*:?\\s*(.*?)(?=(?:${stops})\\s*#?\\s*:|$)`,
          'i'
        )
      : new RegExp(
          `${start}\\s*#?\\s*:?\\s*(.+)$`,
          'i'
        );

    const match = blockText.match(pattern);

    if (match && clean(match[1])) {
      return clean(match[1]);
    }
  }

  return '';
}

function extractCurrencyField(blockText, labels) {
  for (const label of labels) {
    const escaped = escapeRegex(label);

    const pattern = new RegExp(
      `${escaped}\\s*#?\\s*:?\\s*(\\(?\\s*\\$?\\s*-?[0-9][0-9,]*(?:\\.[0-9]{1,2})?\\s*\\)?)`,
      'i'
    );

    const match = blockText.match(pattern);

    if (match && parseCurrency(match[1]) !== null) {
      return clean(match[1]);
    }
  }

  return '';
}

// ============================================================
// FIELD VALIDATION
// ============================================================

function isRealParcelId(value) {
  const normalized = normalizeLabel(value);

  if (!normalized) {
    return false;
  }

  const invalidValues = new Set([
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
  ]);

  return !invalidValues.has(normalized);
}

function isRealCaseNumber(value) {
  const normalized = normalizeLabel(value);

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
    'unknown',
    '-',
  ]).has(normalized);
}

function extractAuctionDateFromUrl(url) {
  try {
    const parsed = new URL(normalizeAmpersands(url));

    for (const [key, value] of parsed.searchParams.entries()) {
      const normalizedKey = key
        .replace(/[^a-z]/gi, '')
        .toLowerCase();

      if (normalizedKey === 'auctiondate') {
        return clean(value);
      }
    }
  } catch {
    // Return blank below.
  }

  return '';
}

function extractAuctionDateFromText(blockText) {
  const patterns = [
    /Date\/Time\s*:\s*([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4}(?:\s+[0-9]{1,2}:[0-9]{2}\s*(?:AM|PM)?\s*(?:ET|EST|EDT)?)?)/i,

    /Auction Date\s*:\s*([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4})/i,

    /Sale Date\s*:\s*([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4})/i,
  ];

  for (const pattern of patterns) {
    const match = blockText.match(pattern);

    if (match) {
      return clean(match[1]);
    }
  }

  return '';
}

function splitAddressAndCityZip(value) {
  let propertyAddress = clean(value);
  let cityStateZip = '';

  const match = propertyAddress.match(
    /([A-Za-z .'-]+),?\s+([A-Za-z]{2})\s*-?\s*(\d{5}(?:-\d{4})?)\s*$/i
  );

  if (match) {
    cityStateZip =
      `${clean(match[1])}, ` +
      `${match[2].toUpperCase()} ${match[3]}`;

    propertyAddress = clean(
      propertyAddress.slice(0, match.index)
    );
  }

  return {
    propertyAddress,
    cityStateZip,
  };
}

// ============================================================
// BLOCKED PAGE DETECTION
// ============================================================

function detectBlockedPage(html, statusCode) {
  const lower = String(html || '').toLowerCase();

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
// PAGE AND PAGER HELPERS
// ============================================================

async function getPageScopes(page) {
  await page.waitForSelector(
    '#BID_WINDOW_CONTAINER',
    {
      timeout: ELEMENT_WAIT_TIMEOUT,
    }
  );

  // Wait for either an auction record or a recognizable empty page.
  await page.waitForFunction(
    () => {
      const root = document.querySelector(
        '#BID_WINDOW_CONTAINER'
      );

      if (!root) {
        return false;
      }

      const auction = root.querySelector('div[aid]');

      const text =
        root.innerText ||
        root.textContent ||
        '';

      const looksEmpty =
        /no\s+(auction|record|result|item)s?\s+(found|available)/i.test(
          text
        );

      return Boolean(auction || looksEmpty);
    },
    {
      timeout: ELEMENT_WAIT_TIMEOUT,
    }
  );

  return {
    async firstRowSignature() {
      return page.$eval(
        '#BID_WINDOW_CONTAINER',
        root => {
          const first = root.querySelector('div[aid]');

          if (!first) {
            return '__NONE__';
          }

          const aid =
            first.getAttribute('aid') || '';

          const text =
            first.innerText ||
            first.textContent ||
            '';

          return `${aid}|${text
            .replace(/\s+/g, ' ')
            .trim()}`;
        }
      );
    },

    async waitForListChange(
      previousSignature,
      timeoutMs = PAGE_CHANGE_TIMEOUT
    ) {
      const started = Date.now();

      while (Date.now() - started < timeoutMs) {
        try {
          const current =
            await this.firstRowSignature();

          if (current !== previousSignature) {
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
        '#BID_WINDOW_CONTAINER .Head_C > div:nth-of-type(3)'
      );

      if (!bar) {
        return {
          bar: null,
          input: null,
          text: null,
          next: null,
        };
      }

      const input = await bar.$(
        "input[type='text'], " +
        "input[type='number'], " +
        "input:not([type])"
      );

      const text = await bar.$('span.PageText');

      const next =
        (await bar.$('span.PageRight > img')) ||
        (await bar.$('.PageRight_HVR > img')) ||
        (await bar.$('.PageRight img')) ||
        (await bar.$(
          'img[alt*="next" i], img[title*="next" i]'
        ));

      return {
        bar,
        input,
        text,
        next,
      };
    },

    async readIndicator(pieces) {
      if (!pieces || !pieces.bar) {
        return {
          current: null,
          total: null,
          raw: '',
        };
      }

      return page.evaluate(bar => {
        const input = bar.querySelector('input');
        const text = bar.querySelector(
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

        const totalMatch = raw.match(
          /\bof\s*([0-9]+)/i
        );

        const currentMatch = raw.match(
          /\b(?:page\s*)?([0-9]+)\s+of\s+[0-9]+/i
        );

        const currentFromInput =
          /^\d+$/.test(inputValue)
            ? Number(inputValue)
            : null;

        const currentFromText =
          currentMatch
            ? Number(currentMatch[1])
            : null;

        return {
          current:
            currentFromInput ||
            currentFromText ||
            null,

          total:
            totalMatch
              ? Number(totalMatch[1])
              : null,

          raw,
        };
      }, pieces.bar);
    },

    async setPageInputAndGo(
      pieces,
      nextPage
    ) {
      if (!pieces || !pieces.input) {
        return false;
      }

      try {
        await pieces.input.focus();

        await page.evaluate(
          (element, value) => {
            const descriptor =
              Object.getOwnPropertyDescriptor(
                HTMLInputElement.prototype,
                'value'
              );

            const setter = descriptor?.set;

            if (setter) {
              setter.call(
                element,
                String(value)
              );
            } else {
              element.value = String(value);
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

        await page.keyboard.press('Enter');

        return true;
      } catch {
        return false;
      }
    },

    async clickNextArrow(pieces) {
      if (!pieces || !pieces.next) {
        return false;
      }

      try {
        await page.evaluate(element => {
          element.scrollIntoView({
            block: 'center',
          });

          const clickable = element.closest(
            'a, button, ' +
            'input[type="submit"], ' +
            'input[type="button"]'
          );

          (clickable || element).click();
        }, pieces.next);

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
// AUCTION PARSER
// ============================================================

function parseAuctionsFromHtml(html, pageUrl) {
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

  const salePriceLabels = [
    'Sale Price',
    'Sold Amount',
    'Winning Bid',
    'Final Bid',
    'Sale Amount',
    'Amount Sold',
    'High Bid',
    'Amount',
  ];

  const assessedValueLabels = [
    'Adjudged Value',
    'Assessed Value',
    'Market Value',
    'Just Value',
    'Appraised Value',
  ];

  $('#BID_WINDOW_CONTAINER div[aid]').each(
    (_, item) => {
      const $item = $(item);

      const blockText = clean($item.text());
      const auctionId = clean($item.attr('aid'));
      const itemClass = clean($item.attr('class'));

      // Store only actual auction blocks rather than every
      // parent and child element on the page.
      relevantElements.push({
        sourceUrl: pageUrl,
        tag: 'div',
        attrs: $item.attr() || {},
        text: blockText.slice(0, 10000),
      });

      // ------------------------------------------------------
      // Case number
      // ------------------------------------------------------

      let caseNumber = getByLabel(
        $,
        $item,
        [
          'Cause Number',
          'Case Number',
          'Case #',
        ]
      );

      caseNumber ||= extractTextField(
        blockText,
        [
          'Case #',
          'Case Number',
          'Cause Number',
        ],
        [
          'Final Judgment Amount',
          'Est. Min. Bid',
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
      // Opening or minimum bid
      // ------------------------------------------------------

      let openingBid = getByLabel(
        $,
        $item,
        openingBidLabels
      );

      if (
        parseCurrency(openingBid) === null
      ) {
        openingBid = extractCurrencyField(
          blockText,
          openingBidLabels
        );
      }

      // ------------------------------------------------------
      // Parcel ID
      // ------------------------------------------------------

      let parcelId = getByLabel(
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

      parcelId ||= extractTextField(
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
        ]
      );

      // ------------------------------------------------------
      // Property address
      // ------------------------------------------------------

      let propertyAddress = getByLabel(
        $,
        $item,
        [
          'Property Address',
          'Address',
        ]
      );

      propertyAddress ||= extractTextField(
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
        ]
      );

      // ------------------------------------------------------
      // Assessed or adjudged value
      // ------------------------------------------------------

      let assessedValue = getByLabel(
        $,
        $item,
        assessedValueLabels
      );

      if (
        parseCurrency(assessedValue) === null
      ) {
        assessedValue = extractCurrencyField(
          blockText,
          assessedValueLabels
        );
      }

      // ------------------------------------------------------
      // City, state, and ZIP
      // ------------------------------------------------------

      let cityStateZip = getByLabel(
        $,
        $item,
        [
          'City, State Zip',
          'City/State/Zip',
          'City State Zip',
          'City State ZIP',
        ]
      );

      if (!cityStateZip && propertyAddress) {
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
      // Status and sold amount
      // ------------------------------------------------------

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

      if (parseCurrency(salePrice) === null) {
        salePrice = getByLabel(
          $,
          $item,
          salePriceLabels
        );
      }

      if (parseCurrency(salePrice) === null) {
        salePrice = extractCurrencyField(
          blockText,
          salePriceLabels
        );
      }

      const statusLower =
        status.toLowerCase();

      const blockLower =
        blockText.toLowerCase();

      const classLower =
        itemClass.toLowerCase();

      let auctionStatus = 'Unknown';

      if (
        statusLower.includes('redeemed') ||
        blockLower.includes('redeemed')
      ) {
        auctionStatus = 'Redeemed';
      } else if (
        statusLower.includes('cancel') ||
        blockLower.includes('cancelled') ||
        blockLower.includes('canceled')
      ) {
        auctionStatus = 'Cancelled';
      } else if (
        statusLower.includes('sold') ||
        statusLower.includes('paid') ||
        blockLower.includes('auction sold') ||
        classLower.includes('sold') ||
        parseCurrency(salePrice) !== null
      ) {
        auctionStatus = 'Sold';
      } else if (
        statusLower.includes('active') ||
        blockLower.includes('active auction')
      ) {
        auctionStatus = 'Active';
      } else if (
        classLower.includes('preview') ||
        blockLower.includes('preview')
      ) {
        auctionStatus = 'Preview';
      } else if (status) {
        auctionStatus = status;
      }

      // ------------------------------------------------------
      // Auction date
      // ------------------------------------------------------

      const auctionDate =
        extractAuctionDateFromUrl(pageUrl) ||
        extractAuctionDateFromText(blockText);

      // ------------------------------------------------------
      // Validate required values
      // ------------------------------------------------------

      if (!isRealCaseNumber(caseNumber)) {
        rejectedRows.push({
          sourceUrl: pageUrl,
          auctionId,
          reason: 'MissingOrInvalidCaseNumber',
          caseNumber: clean(caseNumber),
          openingBid: clean(openingBid),
          parcelId: clean(parcelId),
        });

        relevantElements.push({
          sourceUrl: pageUrl,
          tag: 'parse-rejection',
          attrs: {
            aid: auctionId,
            reason:
              'MissingOrInvalidCaseNumber',
          },
          text: blockText.slice(0, 10000),
        });

        return;
      }

      const openingBidNum =
        parseCurrency(openingBid);

      if (
        EXCLUDE_ZERO_OR_MISSING_OPENING_BID &&
        (
          openingBidNum === null ||
          openingBidNum <= 0
        )
      ) {
        rejectedRows.push({
          sourceUrl: pageUrl,
          auctionId,
          reason:
            'OpeningBidIsBlankInvalidOrZero',
          caseNumber: clean(caseNumber),
          openingBid: clean(openingBid),
          parcelId: clean(parcelId),
        });

        relevantElements.push({
          sourceUrl: pageUrl,
          tag: 'parse-rejection',
          attrs: {
            aid: auctionId,
            reason:
              'OpeningBidIsBlankInvalidOrZero',
          },
          text: blockText.slice(0, 10000),
        });

        return;
      }

      const salePriceNum =
        parseCurrency(salePrice);

      const assessedValueNum =
        parseCurrency(assessedValue);

      // ------------------------------------------------------
      // Separate calculations
      // ------------------------------------------------------

      // Sale proceeds difference:
      // Sale price minus opening/minimum bid.
      const saleSurplus =
        salePriceNum !== null &&
        openingBidNum !== null
          ? salePriceNum - openingBidNum
          : null;

      // Estimated value spread for sold auctions:
      // Assessed value minus sale price.
      const assessedVsSaleSpread =
        assessedValueNum !== null &&
        salePriceNum !== null
          ? assessedValueNum - salePriceNum
          : null;

      // Estimated value spread before or without a sale:
      // Assessed value minus opening bid.
      const assessedVsOpeningSpread =
        assessedValueNum !== null &&
        openingBidNum !== null
          ? assessedValueNum - openingBidNum
          : null;

      const meetsMinimumSurplus =
        saleSurplus === null
          ? ''
          : saleSurplus >= MIN_SURPLUS
            ? 'Yes'
            : 'No';

      const row = {
        sourceUrl: pageUrl,
        auctionId,
        auctionStatus,
        auctionType: 'Foreclosure',

        caseNumber: clean(caseNumber),

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
          clean(salePrice),

        assessedValue:
          clean(assessedValue),

        auctionDate:
          clean(auctionDate),

        status:
          clean(status),

        rawClass:
          itemClass,

        // Main auction-sale calculation.
        surplus: saleSurplus,
        saleSurplus,

        // Additional property value comparisons.
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
          auctionId,
          reason:
            'BelowMinimumSurplusThreshold',
          caseNumber: row.caseNumber,
          openingBid: row.openingBid,
          parcelId: row.parcelId,
          surplus: row.surplus,
        });

        return;
      }

      rows.push(row);
    }
  );

  return {
    rows,
    relevantElements,
    rejectedRows,
  };
}

// ============================================================
// PROCESS ONE URL
// ============================================================

async function inspectAndParse(browser, url) {
  const page = await browser.newPage();

  const relevantElements = [];
  const parsedRows = [];
  const rejectedRows = [];
  const seen = new Set();
  const seenPageSignatures = new Set();

  page.setDefaultNavigationTimeout(
    PAGE_LOAD_TIMEOUT
  );

  page.setDefaultTimeout(
    ELEMENT_WAIT_TIMEOUT
  );

  try {
    await page.setViewport(VIEWPORT);

    try {
      await page.emulateTimezone(TIMEZONE);
    } catch (error) {
      console.warn(
        `Timezone emulation warning: ${error.message}`
      );
    }

    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
      'AppleWebKit/537.36 (KHTML, like Gecko) ' +
      'Chrome/121.0.0.0 Safari/537.36'
    );

    await page.setExtraHTTPHeaders({
      'Accept-Language': 'en-US,en;q=0.9',
      Accept:
        'text/html,application/xhtml+xml,' +
        'application/xml;q=0.9,' +
        'image/avif,image/webp,*/*;q=0.8',
    });

    const normalizedUrl =
      normalizeAmpersands(url);

    console.log(`Visiting: ${normalizedUrl}`);

    const response = await page.goto(
      normalizedUrl,
      {
        waitUntil: 'networkidle2',
        timeout: PAGE_LOAD_TIMEOUT,
      }
    );

    const statusCode =
      response ? response.status() : null;

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

    const scope =
      await getPageScopes(page);

    let pagesProcessed = 0;

    while (pagesProcessed < MAX_PAGES) {
      const pageNumber =
        pagesProcessed + 1;

      console.log(
        `Parsing page ${pageNumber} of maximum ${MAX_PAGES}`
      );

      const html =
        await page.content();

      const currentBlockedReason =
        detectBlockedPage(html, null);

      if (currentBlockedReason) {
        throw new Error(
          currentBlockedReason
        );
      }

      const firstSignature =
        await scope.firstRowSignature();

      if (
        seenPageSignatures.has(
          firstSignature
        )
      ) {
        console.log(
          'Repeated page content detected. Ending this URL.'
        );

        break;
      }

      seenPageSignatures.add(
        firstSignature
      );

      const {
        rows,
        relevantElements: relevant,
        rejectedRows: rejected,
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

        if (!seen.has(key)) {
          seen.add(key);
          parsedRows.push(row);
        }
      }

      relevantElements.push(
        ...relevant
      );

      rejectedRows.push(
        ...rejected
      );

      console.log(
        `Parsed unique rows so far: ${parsedRows.length}`
      );

      pagesProcessed += 1;

      const pieces =
        await scope.getPagerPieces();

      if (!pieces.bar) {
        console.log(
          'Pager bar not found. Ending this URL.'
        );

        break;
      }

      const indicator =
        await scope.readIndicator(pieces);

      const current =
        indicator.current ||
        pagesProcessed;

      const total =
        indicator.total;

      if (
        total &&
        current >= total
      ) {
        console.log(
          `Reached last page (${current}/${total}).`
        );

        break;
      }

      const previousSignature =
        await scope.firstRowSignature();

      const nextPage =
        current + 1;

      let changed = false;

      const inputTriggered =
        await scope.setPageInputAndGo(
          pieces,
          nextPage
        );

      if (inputTriggered) {
        changed =
          await scope.waitForListChange(
            previousSignature
          );
      }

      if (!changed && pieces.next) {
        const arrowTriggered =
          await scope.clickNextArrow(
            pieces
          );

        if (arrowTriggered) {
          changed =
            await scope.waitForListChange(
              previousSignature
            );
        }
      }

      if (!changed) {
        console.log(
          'No list change after pager actions. Ending this URL.'
        );

        break;
      }

      // Small courtesy delay between result pages.
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
      `Error on ${url}: ${message}`
    );

    return {
      relevantElements,
      parsedRows,
      rejectedRows,
      error: {
        url,
        message,
      },
    };
  } finally {
    try {
      await page.close();
    } catch {
      // Ignore page-close failures.
    }
  }
}

// ============================================================
// GLOBAL DEDUPLICATION
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
// FILE HELPERS
// ============================================================

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
// MAIN
// ============================================================

(async () => {
  let browser;

  const startedAt =
    new Date();

  try {
    console.log('Loading URLs from Google Sheets...');

    const urls =
      await loadTargetUrls();

    console.log(
      `Got ${urls.length} unique URL(s) to process.`
    );

    if (!urls.length) {
      throw new Error(
        `No valid URLs found in ${SHEET_NAME}!${URL_RANGE}`
      );
    }

    browser = await puppeteer.launch({
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

    const allElements = [];
    const allRows = [];
    const allRejectedRows = [];
    const errors = [];

    for (
      let index = 0;
      index < urls.length;
      index += 1
    ) {
      const url = urls[index];

      console.log('');
      console.log(
        `Processing URL ${index + 1}/${urls.length}: ${url}`
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
        errors.push(result.error);
      }

      // Courtesy delay before opening the next source URL.
      await sleep(1000);
    }

    const finalRows =
      deduplicateRows(allRows);

    const completedAt =
      new Date();

    const summary = {
      generatedAt:
        completedAt.toISOString(),

      startedAt:
        startedAt.toISOString(),

      durationSeconds:
        Number(
          (
            (
              completedAt.getTime() -
              startedAt.getTime()
            ) / 1000
          ).toFixed(2)
        ),

      configuration: {
        sheet:
          `${SHEET_NAME}!${URL_RANGE}`,

        minimumSurplus:
          MIN_SURPLUS,

        maximumPagesPerUrl:
          MAX_PAGES,

        excludeZeroOrMissingOpeningBid:
          EXCLUDE_ZERO_OR_MISSING_OPENING_BID,

        filterByMinimumSurplus:
          FILTER_BY_MINIMUM_SURPLUS,
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

      rejectedRows:
        allRejectedRows.length,

      errorsCount:
        errors.length,

      statusCounts: {
        sold:
          finalRows.filter(
            row =>
              row.auctionStatus === 'Sold'
          ).length,

        active:
          finalRows.filter(
            row =>
              row.auctionStatus === 'Active'
          ).length,

        preview:
          finalRows.filter(
            row =>
              row.auctionStatus === 'Preview'
          ).length,

        redeemed:
          finalRows.filter(
            row =>
              row.auctionStatus === 'Redeemed'
          ).length,

        cancelled:
          finalRows.filter(
            row =>
              row.auctionStatus === 'Cancelled'
          ).length,

        unknown:
          finalRows.filter(
            row =>
              row.auctionStatus === 'Unknown'
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
              row.reason || 'Unknown';

            counts[reason] =
              (counts[reason] || 0) + 1;

            return counts;
          },
          {}
        ),
    };

    writeJson(
      OUTPUT_ELEMENTS_FILE,
      allElements
    );

    writeJson(
      OUTPUT_ROWS_FILE,
      finalRows
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
      `Saved ${allElements.length} elements -> ${OUTPUT_ELEMENTS_FILE}`
    );

    console.log(
      `Saved ${finalRows.length} unique auctions -> ${OUTPUT_ROWS_FILE}`
    );

    console.log(
      `Rejected ${allRejectedRows.length} invalid or filtered records`
    );

    console.log(
      `Saved summary -> ${OUTPUT_SUMMARY_FILE}`
    );

    if (errors.length) {
      console.log(
        `Saved ${errors.length} errors -> ${OUTPUT_ERRORS_FILE}`
      );
    }

    console.log('Done.');
  } catch (error) {
    console.error(
      'Fatal inspectWebpage error:',
      error?.message || error
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
