import fs from 'fs';
import { Builder, By, Key, until } from 'selenium-webdriver';
import chrome from 'selenium-webdriver/chrome.js';
import { google } from 'googleapis';

// -----------------------------
// CONFIG
// -----------------------------
const SHEET_ID = process.env.SHEET_ID;
const RANGE_INPUT = 'Sheet1!AG2:AI';
const RANGE_OUTPUT_OWNER = 'Sheet1!CM2:CM';
const RANGE_OUTPUT_DATE = 'Sheet1!CN2:CN';
const RANGE_OUTPUT_SIZE = 'Sheet1!CO2:CO';
const RANGE_OUTPUT_ROAD = 'Sheet1!CP2:CP';

const PAGE_LOAD_TIMEOUT_MS = 60000;
const ELEMENT_TIMEOUT_MS = 20000;
const HEADLESS = true;
const CHROME_PATH = process.env.CHROME_PATH || null;

// -----------------------------
// GOOGLE SHEETS AUTH
// -----------------------------
async function getSheetsClient() {
  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(process.env.SERVICE_ACCOUNT_JSON),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  return google.sheets({ version: 'v4', auth });
}

// -----------------------------
// Selenium launcher
// -----------------------------
async function launchDriver() {
  console.log('[Browser] Launching Chrome driver, headless:', HEADLESS);

  const options = new chrome.Options();

  if (HEADLESS) {
    options.addArguments('--headless=new', '--disable-gpu', '--window-size=1200,900');
  } else {
    options.addArguments('--start-maximized');
  }

  options.addArguments(
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-dev-shm-usage',
    '--disable-blink-features=AutomationControlled'
  );

  let chromeBinary = CHROME_PATH;

  if (!chromeBinary) {
    switch (process.platform) {
      case 'win32':
        chromeBinary =
          fs.existsSync('C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe')
            ? 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe'
            : 'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe';
        break;
      case 'darwin':
        chromeBinary = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
        break;
      default:
        chromeBinary = '/usr/bin/google-chrome';
    }
  }

  if (chromeBinary && fs.existsSync(chromeBinary)) {
    options.setChromeBinaryPath(chromeBinary);
    console.log(`[Browser] Using Chrome binary: ${chromeBinary}`);
  }

  const driver = await new Builder()
    .forBrowser('chrome')
    .setChromeOptions(options)
    .build();

  await driver.manage().setTimeouts({
    implicit: 0,
    pageLoad: PAGE_LOAD_TIMEOUT_MS,
    script: 60000,
  });

  return driver;
}

// -----------------------------
// DOM helpers
// -----------------------------
async function exists(driver, locator, timeout = ELEMENT_TIMEOUT_MS) {
  try {
    await driver.wait(until.elementLocated(locator), timeout);
    return true;
  } catch {
    return false;
  }
}

async function getTextSafe(driver, locator, timeout = ELEMENT_TIMEOUT_MS) {
  try {
    const el = await driver.wait(until.elementLocated(locator), timeout);
    await driver.wait(until.elementIsVisible(el), timeout);
    return (await el.getText()).trim();
  } catch {
    return '';
  }
}

// -----------------------------
// MODAL HANDLER
// -----------------------------
async function dismissModalIfPresent(driver) {
  const modal = By.css('#appBody > div.modal.in > div > div');
  const btn = By.css(
    '#appBody > div.modal.in > div > div > div.modal-focus-target > div.modal-footer > a.btn.btn-primary.button-1'
  );

  try {
    const isModal = await exists(driver, modal, 5000);
    if (isModal) {
      console.log('[Modal] Detected popup, dismissing...');
      const button = await driver.findElement(btn);
      await driver.executeScript('arguments[0].click();', button);
      await driver.sleep(1500);
    }
  } catch {
    console.log('[Modal] No modal or already dismissed');
  }
}

// -----------------------------
// MAIN SCRAPER FLOW
// -----------------------------
async function run() {
  const sheets = await getSheetsClient();
  const driver = await launchDriver();

  try {
    // -----------------------------
    // FETCH GOOGLE SHEETS DATA
    // -----------------------------
    const input = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: RANGE_INPUT,
    });

    const rows = input.data.values || [];

    const outputOwner = [];
    const outputDate = [];
    const outputSize = [];
    const outputRoad = [];

    for (let i = 0; i < rows.length; i++) {
      const [ag = '', ah = '', ai = ''] = rows[i];
      const x = `${ag} ${ah} ${ai}`.trim();

      if (!x) {
        outputOwner.push(['']);
        outputDate.push(['']);
        outputSize.push(['']);
        outputRoad.push(['']);
        continue;
      }

      console.log(`[Row ${i + 2}] Searching: ${x}`);

      // NAVIGATE
      await driver.get(
        'https://qpublic.schneidercorp.com/Application.aspx?AppID=694&LayerID=11394&PageTypeID=2&PageID=4832'
      );

      await dismissModalIfPresent(driver);

      // INPUT SEARCH
      const inputBox = await driver.wait(
        until.elementLocated(By.css('#ctlBodyPane_ctl01_ctl01_txtAddress')),
        ELEMENT_TIMEOUT_MS
      );

      await inputBox.clear();
      await inputBox.sendKeys(x, Key.RETURN);

      // WAIT FOR RESULT PAGE
      let hasResults = false;

      try {
        await driver.wait(
          until.elementLocated(
            By.css('#Form1 > div.container.page-container > div > div.col-md-10.page-center-pane')
          ),
          15000
        );
        hasResults = true;
      } catch {
        hasResults = false;
      }

      if (!hasResults) {
        console.log(`[Row ${i + 2}] No results found`);
        outputOwner.push(['']);
        outputDate.push(['']);
        outputSize.push(['']);
        outputRoad.push(['']);
        continue;
      }

      // SCRAPE DATA
      const owner = await getTextSafe(
        driver,
        By.css('#ctlBodyPane_ctl02_ctl01_lnkOwnerName_lnkSearch')
      );

      const datePurchased = await getTextSafe(
        driver,
        By.css('#ctlBodyPane_ctl12_ctl01_gvwSales > tbody > tr:nth-child(1) > th')
      );

      const bldgSize = await getTextSafe(
        driver,
        By.css('#ctlBodyPane_ctl06_ctl01_rptResidential_ctl00_lblSqFt')
      );

      const roadAccess = await getTextSafe(
        driver,
        By.css('#ctlBodyPane_ctl01_ctl01_lblParcelRoadAccess')
      );

      outputOwner.push([owner]);
      outputDate.push([datePurchased]);
      outputSize.push([bldgSize]);
      outputRoad.push([roadAccess]);

      console.log(`[Row ${i + 2}] Done`);
    }

    // WRITE BACK
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: RANGE_OUTPUT_OWNER,
      valueInputOption: 'RAW',
      requestBody: { values: outputOwner },
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: RANGE_OUTPUT_DATE,
      valueInputOption: 'RAW',
      requestBody: { values: outputDate },
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: RANGE_OUTPUT_SIZE,
      valueInputOption: 'RAW',
      requestBody: { values: outputSize },
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: RANGE_OUTPUT_ROAD,
      valueInputOption: 'RAW',
      requestBody: { values: outputRoad },
    });

    console.log('✅ DONE');
  } catch (err) {
    console.error(err);
  } finally {
    await driver.quit();
  }
}

run();