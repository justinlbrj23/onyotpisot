// inspectWebpage.cjs
//
// RealForeclose auction inspector, parser, evaluator, and validator.
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
// Share the source Google Sheet with the service account email address.
//
// Output files:
// raw-elements.json
// parsed-auctions.json
// errors.json
// summary.json

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

// Exclude rows where opening/minimum bid is:
// - blank
// - invalid
// - zero
// - negative
const EXCLUDE_ZERO_OR_MISSING_OPENING_BID = true;

// When false, rows below $25,000 remain in parsed-auctions.json.
// They are only marked Yes or No.
//
// When true, only rows with saleSurplus >= MIN_SURPLUS remain.
const FILTER_BY_MINIMUM_SURPLUS = false;

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

    return (
      parsed.protocol === 'http:' ||
      parsed.protocol === 'https:'
    );
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

  // Remove duplicate source URLs.
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

  // Convert accounting format:
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

  const parsed = Number.parseFloat(numericText);

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

function hasMeaningfulValue(value) {
  const normalized = normalizeLabel(value);

  return !new Set([
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
// STRUCTURED FIELD EXTRACTION
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

// ============================================================
// TEXT FALLBACK EXTRACTION
// ============================================================

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
    const escapedLabel = escapeRegex(label);

    const pattern = new RegExp(
      `${escapedLabel}\\s*#?\\s*:?\\s*` +
      `(\\(?\\s*\\$?\\s*-?[0-9][0-9,]*` +
      `(?:\\.[0-9]{1,2})?\\s*\\)?)`,
      'i'
    );

    const match = blockText.match(pattern);

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

  const invalidValues = new Set([
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
  ]);

  return !invalidValues.has(normalized);
}

// ============================================================
// AUCTION DATE EXTRACTION
// ============================================================

function extractAuctionDateFromUrl(url) {
  try {
    const parsed = new URL(
      normalizeAmpersands(url)
    );

    for (
      const [key, value]
      of parsed.searchParams.entries()
    ) {
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

// ============================================================
// ADDRESS PROCESSING
// ============================================================

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
// PAGE AND PAGER CONTROLS
// ============================================================

async function getPageScopes(page) {
  await page.waitForSelector(
    '#BID_WINDOW_CONTAINER',
    {
      timeout: ELEMENT_WAIT_TIMEOUT,
    }
  );

  // Wait for either an auction record or a recognizable
  // no-results message.
  await page.waitForFunction(
    () => {
      const root = document.querySelector(
        '#BID_WINDOW_CONTAINER'
      );

      if (!root) {
        return false;
      }

      const auction = root.querySelector(
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
          const first = root.querySelector(
            'div[aid]'
          );

          if (!first) {
            return '__NONE__';
          }

          const auctionId =
            first.getAttribute('aid') || '';

          const text =
            first.innerText ||
            first.textContent ||
            '';

          return (
            `${auctionId}|` +
            text.replace(/\s+/g, ' ').trim()
          );
        }
      );
    },

    async waitForListChange(
      previousSignature,
      timeoutMs = PAGE_CHANGE_TIMEOUT
    ) {
      const started = Date.now();

      while (
        Date.now() - started < timeoutMs
      ) {
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

      const input = await bar.$(
        "input[type='text'], " +
        "input[type='number'], " +
        "input:not([type])"
      );

      const text = await bar.$(
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
      if (!pieces || !pieces.bar) {
        return {
          current: null,
          total: null,
          raw: '',
        };
      }

      return page.evaluate(bar => {
        const input =
          bar.querySelector('input');

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

// ============================================================
// STRICT SALE PRICE EXTRACTION
// ============================================================

function extractConfirmedSalePrice(
  $,
  $item,
  blockText,
  auctionStatus
) {
  // Never assign a sale price to auctions that are not
  // positively identified as sold.
  if (auctionStatus !== 'Sold') {
    return '';
  }

  // These labels are intentionally specific.
  //
  // Do not add the generic label "Amount" because it can
  // match Final Judgment Amount or another unrelated amount.
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

  if (parseCurrency(salePrice) === null) {
    salePrice = getByLabel(
      $,
      $item,
      strictSalePriceLabels
    );
  }

  if (parseCurrency(salePrice) === null) {
    salePrice = extractCurrencyField(
      blockText,
      strictSalePriceLabels
    );
  }

  // Do not preserve nonnumeric status text as sale price.
  if (parseCurrency(salePrice) === null) {
    return '';
  }

  return clean(salePrice);
}

// ============================================================
// AUCTION PARSER
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

  $('#BID_WINDOW_CONTAINER div[aid]').each(
    (_, item) => {
      const $item = $(item);

      const blockText = clean(
        $item.text()
      );

      const auctionId = clean(
        $item.attr('aid')
      );

      const itemClass = clean(
        $item.attr('class')
      );

      // Save the actual auction container for debugging.
      relevantElements.push({
        sourceUrl: pageUrl,
        tag: 'div',
        attrs: $item.attr() || {},
        text: blockText.slice(0, 10000),
      });

      // ------------------------------------------------------
      // CASE NUMBER
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
      // PARCEL ID
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
          'Estimated Minimum Bid',
        ]
      );

      // ------------------------------------------------------
      // PROPERTY ADDRESS
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
          'Est. Min. Bid',
        ]
      );

      // ------------------------------------------------------
      // ASSESSED OR ADJUDGED VALUE
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
      // CITY, STATE, AND ZIP
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
      // RAW STATUS
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

      // ------------------------------------------------------
      // DETERMINE STATUS BEFORE EXTRACTING SALE PRICE
      // ------------------------------------------------------

      const auctionStatus =
        determineAuctionStatus({
          status,
          blockText,
          itemClass,
        });

      // ------------------------------------------------------
      // STRICT SALE PRICE EXTRACTION
      // ------------------------------------------------------

      const salePrice =
        extractConfirmedSalePrice(
          $,
          $item,
          blockText,
          auctionStatus
        );

      // ------------------------------------------------------
      // AUCTION DATE
      // ------------------------------------------------------

      const auctionDate =
        extractAuctionDateFromUrl(
          pageUrl
        ) ||
        extractAuctionDateFromText(
          blockText
        );

      // ------------------------------------------------------
      // NUMERIC VALUES
      // ------------------------------------------------------

      const openingBidNum =
        parseCurrency(openingBid);

      const salePriceNum =
        parseCurrency(salePrice);

      const assessedValueNum =
        parseCurrency(assessedValue);

      // ------------------------------------------------------
      // REQUIRED CASE NUMBER VALIDATION
      // ------------------------------------------------------

      if (!isRealCaseNumber(caseNumber)) {
        const rejection = {
          sourceUrl: pageUrl,
          auctionId,
          reason:
            'MissingOrInvalidCaseNumber',
          caseNumber: clean(caseNumber),
          openingBid: clean(openingBid),
          parcelId: clean(parcelId),
        };

        rejectedRows.push(rejection);

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

      // ------------------------------------------------------
      // OPENING BID FILTER
      // ------------------------------------------------------

      if (
        EXCLUDE_ZERO_OR_MISSING_OPENING_BID &&
        (
          openingBidNum === null ||
          openingBidNum <= 0
        )
      ) {
        const rejection = {
          sourceUrl: pageUrl,
          auctionId,
          reason:
            'OpeningBidIsBlankInvalidOrZero',
          caseNumber: clean(caseNumber),
          openingBid: clean(openingBid),
          parcelId: clean(parcelId),
        };

        rejectedRows.push(rejection);

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

      // ------------------------------------------------------
      // CALCULATIONS
      // ------------------------------------------------------

      // Actual sale surplus:
      // final sale price minus opening/minimum bid.
      //
      // This remains null unless the auction is confirmed sold.
      const saleSurplus =
        auctionStatus === 'Sold' &&
        salePriceNum !== null &&
        openingBidNum !== null
          ? salePriceNum - openingBidNum
          : null;

      // Assessed value minus final sale price.
      //
      // This measures a property value spread and is not the
      // same as auction excess proceeds.
      const assessedVsSaleSpread =
        auctionStatus === 'Sold' &&
        assessedValueNum !== null &&
        salePriceNum !== null
          ? assessedValueNum - salePriceNum
          : null;

      // Assessed value minus opening/minimum bid.
      //
      // This may be available before the auction is sold.
      const assessedVsOpeningSpread =
        assessedValueNum !== null &&
        openingBidNum !== null
          ? assessedValueNum - openingBidNum
          : null;

      // Preserve a blank value when surplus cannot be calculated.
      const meetsMinimumSurplus =
        saleSurplus === null
          ? ''
          : saleSurplus >= MIN_SURPLUS
            ? 'Yes'
            : 'No';

      // ------------------------------------------------------
      // OUTPUT RECORD
      // ------------------------------------------------------

      const row = {
        sourceUrl: pageUrl,
        auctionId,

        auctionStatus,

        // Convenience field for downstream spreadsheet mapping.
        sale:
          auctionStatus === 'Sold'
            ? 'Yes'
            : 'No',

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

        // This is blank unless the auction is confirmed sold.
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

        // Main auction sale calculation.
        surplus: saleSurplus,
        saleSurplus,

        // Property value comparisons.
        assessedVsSaleSpread,
        assessedVsOpeningSpread,

        meetsMinimumSurplus,
      };

      // ------------------------------------------------------
      // OPTIONAL MINIMUM SURPLUS FILTER
      // ------------------------------------------------------

      if (
        FILTER_BY_MINIMUM_SURPLUS &&
        row.meetsMinimumSurplus !== 'Yes'
      ) {
        rejectedRows.push({
          sourceUrl: pageUrl,
          auctionId,
          reason:
            row.saleSurplus === null
              ? 'SaleSurplusUnavailable'
              : 'BelowMinimumSurplusThreshold',

          caseNumber: row.caseNumber,
          openingBid: row.openingBid,
          salePrice: row.salePrice,
          parcelId: row.parcelId,
          surplus: row.saleSurplus,
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
// PROCESS ONE SOURCE URL
// ============================================================

async function inspectAndParse(
  browser,
  url
) {
  const page = await browser.newPage();

  const relevantElements = [];
  const parsedRows = [];
  const rejectedRows = [];

  const seenRows = new Set();
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
      await page.emulateTimezone(
        TIMEZONE
      );
    } catch (error) {
      console.warn(
        `Timezone warning: ${error.message}`
      );
    }

    await page.setUserAgent(
      'Mozilla/5.0 ' +
      '(Windows NT 10.0; Win64; x64) ' +
      'AppleWebKit/537.36 ' +
      '(KHTML, like Gecko) ' +
      'Chrome/121.0.0.0 ' +
      'Safari/537.36'
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

    const normalizedUrl =
      normalizeAmpersands(url);

    console.log(
      `Visiting: ${normalizedUrl}`
    );

    const response = await page.goto(
      normalizedUrl,
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

    const scope =
      await getPageScopes(page);

    let pagesProcessed = 0;

    while (
      pagesProcessed < MAX_PAGES
    ) {
      const displayedPageNumber =
        pagesProcessed + 1;

      console.log(
        `Parsing page ${displayedPageNumber} ` +
        `of maximum ${MAX_PAGES}`
      );

      const html =
        await page.content();

      const currentBlockedReason =
        detectBlockedPage(
          html,
          null
        );

      if (currentBlockedReason) {
        throw new Error(
          currentBlockedReason
        );
      }

      const currentPageSignature =
        await scope.firstRowSignature();

      if (
        seenPageSignatures.has(
          currentPageSignature
        )
      ) {
        console.log(
          'Repeated page content detected. ' +
          'Ending this URL.'
        );

        break;
      }

      seenPageSignatures.add(
        currentPageSignature
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

        if (!seenRows.has(key)) {
          seenRows.add(key);
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
        `Unique parsed rows so far: ` +
        `${parsedRows.length}`
      );

      pagesProcessed += 1;

      const pieces =
        await scope.getPagerPieces();

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
          `Reached last page ` +
          `(${currentPage}/${totalPages}).`
        );

        break;
      }

      const previousSignature =
        await scope.firstRowSignature();

      const nextPage =
        currentPage + 1;

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

      if (
        !changed &&
        pieces.next
      ) {
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
          'No list change after pager actions. ' +
          'Ending this URL.'
        );

        break;
      }

      // Courtesy delay before parsing the next result page.
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

  const startedAt = new Date();

  try {
    console.log(
      'Loading URLs from Google Sheets...'
    );

    const urls =
      await loadTargetUrls();

    console.log(
      `Got ${urls.length} unique URL(s) ` +
      'to process.'
    );

    if (!urls.length) {
      throw new Error(
        `No valid URLs found in ` +
        `${SHEET_NAME}!${URL_RANGE}`
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
        `Processing URL ${index + 1}/` +
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

      // Courtesy delay before the next source URL.
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
              row.saleSurplus >= MIN_SURPLUS
          ).length,

        belowThreshold:
          finalRows.filter(
            row =>
              row.saleSurplus !== null &&
              row.saleSurplus < MIN_SURPLUS
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
      `Saved ${allElements.length} elements ` +
      `-> ${OUTPUT_ELEMENTS_FILE}`
    );

    console.log(
      `Saved ${finalRows.length} unique auctions ` +
      `-> ${OUTPUT_ROWS_FILE}`
    );

    console.log(
      `Rejected ${allRejectedRows.length} ` +
      'invalid or filtered records'
    );

    console.log(
      `Saved summary ` +
      `-> ${OUTPUT_SUMMARY_FILE}`
    );

    if (errors.length) {
      console.log(
        `Saved ${errors.length} errors ` +
        `-> ${OUTPUT_ERRORS_FILE}`
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
        // Ignore browser-close failures.
      }
    }
  }
})();
