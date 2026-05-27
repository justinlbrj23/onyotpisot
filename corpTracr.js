import { Builder, By, until, Key } from "selenium-webdriver";
import { google } from "googleapis";
import fs from "fs";
import path from "path";
import readline from "readline";
import chrome from "selenium-webdriver/chrome.js";

// ==========================
// Google Sheets Configuration
// ==========================
const SHEET_ID = "1n1daep0zpdeC4ITPoRTYeW7-ayx_rcEh2nGYAeavCL0";
const SHEET_NAME = "Sheet4";
const SCOPES = ["https://www.googleapis.com/auth/spreadsheets"];

// ==========================
// Authenticate Google Sheets (Service Account)
// ==========================
async function authenticateGoogleSheets() {
  // Point to your downloaded service account JSON key
  const SERVICE_ACCOUNT_PATH = path.join(process.cwd(), "service-account.json");

  // Create a GoogleAuth client using the service account
  const auth = new google.auth.GoogleAuth({
    keyFile: SERVICE_ACCOUNT_PATH,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  // Get an authenticated client
  const client = await auth.getClient();

  return client;
}

// ==========================
// Update Google Sheet Row (Officer names L→R)
// ==========================
async function updateSheet(auth, officerNames, rowIndex) {
  const sheets = google.sheets({ version: "v4", auth });

  const maxCols = 7; //
  const trimmed = officerNames.slice(0, maxCols);
  while (trimmed.length < maxCols) trimmed.push("");

  const range = `${SHEET_NAME}!L${rowIndex}:R${rowIndex}`;

  try {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range,
      valueInputOption: "RAW",
      resource: { values: [trimmed] },
    });
  } catch (error) {
    console.error(`❌ Error updating sheet at row ${rowIndex}:`, error.message);
  }
}

// ==========================
// Update Google Sheet Row (Company details P→S)
// ==========================
async function updateCompanyDetails(auth, details, rowIndex) {
  const sheets = google.sheets({ version: "v4", auth });

  const range = `${SHEET_NAME}!S${rowIndex}:V${rowIndex}`;
  const values = [[details.registered_name, details.status, details.mail, details.agent]];

  try {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range,
      valueInputOption: "RAW",
      resource: { values },
    });
  } catch (error) {
    console.error(`❌ Error updating details at row ${rowIndex}:`, error.message);
  }
}

// ==========================
// Update Google Sheet Row (Common Street + ZIP W→X)
// ==========================
async function updateCommonAddress(auth, street, zip, rowIndex) {
  const sheets = google.sheets({ version: "v4", auth });

  const range = `${SHEET_NAME}!W${rowIndex}:X${rowIndex}`;
  const values = [[street, zip]];

  try {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range,
      valueInputOption: "RAW",
      resource: { values },
    });
  } catch (error) {
    console.error(`❌ Error updating common address at row ${rowIndex}:`, error.message);
  }
}

// ==========================
// Normalize Company Names (ignore commas)
// ==========================
function normalizeName(name) {
  return (name || "")
    .replace(/,/g, "") // remove commas
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

// ==========================
// Fuzzy String Similarity (uses normalized input)
// ==========================
function stringSimilarity(a, b) {
  a = normalizeName(a);
  b = normalizeName(b);

  const longer = a.length > b.length ? a : b;
  const shorter = a.length > b.length ? b : a;
  const longerLength = longer.length;
  if (longerLength === 0) return 1.0;

  const costs = [];
  for (let i = 0; i <= shorter.length; i++) {
    let lastValue = i;
    for (let j = 0; j <= longer.length; j++) {
      if (i === 0) costs[j] = j;
      else if (j > 0) {
        let newValue = costs[j - 1];
        if (shorter.charAt(i - 1) !== longer.charAt(j - 1)) {
          newValue = Math.min(Math.min(newValue, lastValue), costs[j]) + 1;
        }
        costs[j - 1] = lastValue;
        lastValue = newValue;
      }
    }
    if (i > 0) costs[longer.length] = lastValue;
  }
  return (longerLength - costs[longer.length]) / parseFloat(longerLength);
}

// ==========================
// Company Name Matching (explicitly ignores commas)
// ==========================
function namesMatch(foundName, searchName) {
  const fn = normalizeName(foundName);
  const sn = normalizeName(searchName);
  return fn === sn || fn.includes(sn) || stringSimilarity(foundName, searchName) >= 0.85;
}

// ==========================
// Extract Officer Names + Common Address/Zip
// ==========================
async function scrapeOfficerData(driver) {
  try {
    const section = await driver.findElement(By.xpath('//*[@id="maincontent"]/div[2]/div[6]'));
    const text = await section.getText();

    const lines = text
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean);

    const badStarts = [/^title\b/i, /^authorized person/i, /^name & address/i];

    // Dictionary: Street Name → Abbreviation
    const streetTerms = {
      "Alley": "ALY", "Apartment": "APT", "Arcade": "ARC", "Avenue": "AVE",
      "Bay": "BA", "Beach": "BCH", "Boulevard": "BLVD", "Building": "BLDG",
      "Bypass": "BYPS", "Camp": "CMP", "Canyon": "CYN", "Cape": "CPE",
      "Causeway": "CSWY", "Center": "CTR", "Circle": "CIR", "Cliff": "CLF",
      "Close": "CL", "Common": "CMN", "Court": "CT", "Cove": "CV", "Creek": "CRK",
      "Crescent": "CRES", "Crossing": "XING", "Drive": "DR", "Expressway": "EXPY",
      "Extension": "EXT", "Fall": "FALL", "Field": "FLD", "Flat": "FLT",
      "Forest": "FRST", "Fort": "FT", "Freeway": "FWY", "Grove": "GRV",
      "Heights": "HTS", "Highway": "HWY", "Hollow": "HOLW", "Hospital": "HOSP",
      "Island": "IS", "Junction": "JCT", "Key": "KEY", "Lake": "LK",
      "Lane": "LN", "Library": "LBRY", "Light": "LGT", "Manor": "MR",
      "Mount": "MT", "Overpass": "OPAS", "Park": "PK", "Parkway": "PKWY",
      "Passage": "PS", "Path": "PH", "Place": "PL", "Plain": "PLAIN",
      "Plaza": "PLZ", "Point": "PT", "Post Office": "PO", "Pond": "PND",
      "Range": "RNG", "Road": "RD", "Rural Route": "RR", "Ridge": "RDGE",
      "River": "RV", "Run": "RN", "School": "SCH", "Shore": "SH",
      "Spring": "SPG", "Square": "SQ", "Station": "STA", "Street": "ST",
      "Summit": "SUMMIT", "Terrace": "TER", "Throughway": "THRWAY", "Tower": "TWR",
      "Trail": "TRL", "Tunnel": "TUNL", "Turnpike": "TPKE", "Union": "UN",
      "Valley": "VLY", "Village": "VLG", "Way": "WAY", "Wharf": "WHF",
      "Woods": "WDS", "Work": "WRK"
    };

    // Build regex that matches either full term or abbreviation
    const streetPattern = Object.entries(streetTerms)
      .map(([full, abbr]) => `${full}\\b|${abbr}\\b`)
      .join("|");
    const streetRegex = new RegExp(`\\b(${streetPattern})\\b`, "i");

    const looksLikeAddress = (s) => /\d/.test(s) || streetRegex.test(s);
    const looksLikeStreet = (s) => /^\d+\s+/.test(s) && streetRegex.test(s);

    const officerNames = [];
    const addresses = [];
    const zipCounts = {};
    const streetCounts = {};

    for (const line of lines) {
      if (badStarts.some((re) => re.test(line))) continue;
      if (/^[-–—]+$/.test(line)) continue;

      // Detect officer names
      if (/^[A-Za-z ,.'-]+$/.test(line) && line.replace(/[^A-Za-z]/g, "").length >= 3 && !looksLikeAddress(line)) {
        const cleanName = line.replace(/[.,'"]/g, "").trim();
        if (!officerNames.includes(cleanName)) officerNames.push(cleanName);
      }

      // Detect addresses
      if (looksLikeAddress(line)) {
        addresses.push(line);

        // Count zip
        const zipMatch = line.match(/\b\d{5}(?:-\d{4})?\b/);
        if (zipMatch) {
          const zip = zipMatch[0];
          zipCounts[zip] = (zipCounts[zip] || 0) + 1;
        }

        // Count streets
        if (looksLikeStreet(line)) {
          const street = line.trim();
          streetCounts[street] = (streetCounts[street] || 0) + 1;
        }
      }
    }

    let commonStreet = "No Street Found";
    let commonZip = "No Zip Found";

    if (Object.keys(streetCounts).length > 0) {
      commonStreet = Object.entries(streetCounts).sort((a, b) => b[1] - a[1])[0][0];
    }

    if (Object.keys(zipCounts).length > 0) {
      commonZip = Object.entries(zipCounts).sort((a, b) => b[1] - a[1])[0][0];
    }

    return {
      officerNames: officerNames.length ? officerNames : ["No Officers Found"],
      commonStreet,
      commonZip,
    };
  } catch (err) {
    return {
      officerNames: ["No Officers Found"],
      commonStreet: "No Street Found",
      commonZip: "No Zip Found",
    };
  }
}

// ==========================
// Scrape Extra Company Details
// ==========================
async function scrapeCompanyDetails(driver) {
  try {
    const registered_name = await driver
      .findElement(By.css('#maincontent > div.searchResultDetail > div.detailSection.corporationName > p:nth-child(2)'))
      .getText()
      .catch(() => "No Data");

    const status = await driver
      .findElement(By.css('#maincontent .filingInformation span:nth-child(10)'))
      .getText()
      .catch(() => "No Data");

    const mail = await driver
      .findElement(By.css('#maincontent .detailSection:nth-child(5) div'))
      .getText()
      .catch(() => "No Data");

    const agent = await driver
      .findElement(By.css('#maincontent .detailSection:nth-child(6) span:nth-child(2)'))
      .getText()
      .catch(() => "No Data");

    return { registered_name, status, mail, agent };
  } catch (err) {
    console.error("❌ Error scraping company details:", err.message);
    return { registered_name: "No Data", status: "No Data", mail: "No Data", agent: "No Data" };
  }
}

// ==========================
// Get Company Names
// ==========================
async function getCompanyNames(auth) {
  const sheets = google.sheets({ version: "v4", auth });
  const range = `${SHEET_NAME}!K2:K`;

  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range,
    });

    const values = response.data.values || [];

    return values
      .map((val, index) => ({
        name: val[0]?.trim() || null,
        rowIndex: index + 2,
        isBusiness: isBusinessEntity(val[0]?.trim() || ""),
      }))
      .filter((entry) => entry.name);
  } catch (error) {
    console.error("❌ Error fetching company names:", error.message);
    throw error;
  }
}

// ==========================
// Detect Business Entities
// ==========================
function isBusinessEntity(name) {
  const businessKeywords = [
    "LLC",
    "CORP",
    "INC",
    "LTD",
    "CO ",
    "COMPANY",
    "ENTERPRISES",
    "ASSOCIATES",
    "GROUP",
  ];
  return businessKeywords.some((kw) => (name || "").toLowerCase().includes(kw.toLowerCase()));
}

// ==========================
// Delay Helper
// ==========================
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ==========================
// Retry Helper
// ==========================
async function retryWait(driver, locator, retries = 3, timeout = 30000) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await driver.wait(until.elementLocated(locator), timeout);
    } catch (err) {
      if (attempt === retries) throw err;
      console.log(`🔁 Retry ${attempt}/${retries} waiting for element ${locator}...`);
    }
  }
}

// ==========================
// Resolve businesses recursively to individuals
// ==========================
async function resolveToIndividuals(driver, auth, companyName, visited = new Set()) {
  if (visited.has(normalizeName(companyName))) {
    console.log(`🔄 Skipping circular lookup for ${companyName}`);
    return [];
  }
  visited.add(normalizeName(companyName));

  console.log(`🔍 Resolving officers for: ${companyName}`);

  await driver.get("https://search.sunbiz.org/Inquiry/CorporationSearch/ByName");
  const search = await driver.wait(until.elementLocated(By.id("SearchTerm")), 20000);
  await search.clear();
  await search.sendKeys(companyName, Key.RETURN);

  // Bail out if no results
  try {
    await driver.wait(until.elementLocated(By.css("#search-results table tbody tr")), 15000);
  } catch {
    console.log(`⚠️ No results for ${companyName}`);
    return [];
  }

  // Find first matching row
  const rows = await driver.findElements(By.xpath('//*[@id="search-results"]/table/tbody/tr'));
  let found = false;
  for (const row of rows) {
    const foundName = await row.findElement(By.xpath("./td[1]/a")).getText();
    if (namesMatch(foundName, companyName)) {
      await row.findElement(By.xpath("./td[1]/a")).click();
      found = true;
      break;
    }
  }
  if (!found) return [];

  await retryWait(driver, By.id("main"), 3, 20000);

  // Scrape officers
  const { officerNames } = await scrapeOfficerData(driver);
  const cleanNames = officerNames.map(n => n.replace(/[.,'"]/g, "").trim());

  let resolved = [];
  for (const officer of cleanNames) {
    if (isBusinessEntity(officer)) {
      const subNames = await resolveToIndividuals(driver, auth, officer, visited);
      resolved.push(...subNames);
    } else {
      resolved.push(officer);
    }
  }
  return resolved;
}

// ==========================
// Process a single company (recursive to individuals)
// ==========================
async function processCompany(auth, driver, companyName, rowIndex = null, visited = new Set()) {
  const normName = normalizeName(companyName);

  if (visited.has(normName)) {
    console.log(`🔄 Skipping circular/duplicate lookup for ${companyName}`);
    return [];
  }
  visited.add(normName);

  console.log(`🔍 Processing: ${companyName} (Row ${rowIndex || "N/A"})`);
  await driver.get("https://search.sunbiz.org/Inquiry/CorporationSearch/ByName");

  // --- Perform search
  const search = await driver.wait(until.elementLocated(By.id("SearchTerm")), 20000);
  await search.clear();
  await search.sendKeys(companyName, Key.RETURN);

  // --- Handle search results
  try {
    await driver.wait(until.elementLocated(By.css("#search-results table tbody tr")), 20000);
  } catch {
    console.log(`⚠️ No search results for ${companyName}`);
    if (rowIndex) {
      await updateSheet(auth, ["No Data"], rowIndex);
      await updateCompanyDetails(auth, {
        registered_name: "No Data",
        status: "No Data",
        mail: "No Data",
        agent: "No Data"
      }, rowIndex);
      await updateCommonAddress(auth, "No Address Found", "No Zip Found", rowIndex);
    }
    return [];
  }

  // --- Try to match rows
  const rows = await driver.findElements(By.xpath('//*[@id="search-results"]/table/tbody/tr'));
  let companyFound = false;

  for (const row of rows) {
    const foundName = await row.findElement(By.xpath("./td[1]/a")).getText();
    if (namesMatch(foundName, companyName)) {
      await row.findElement(By.xpath("./td[1]/a")).click();
      companyFound = true;
      break;
    }
  }

  if (!companyFound) {
    console.log(`⚠️ No close match for ${companyName}`);
    if (rowIndex) {
      await updateSheet(auth, ["No Data"], rowIndex);
      await updateCompanyDetails(auth, {
        registered_name: "No Data",
        status: "No Data",
        mail: "No Data",
        agent: "No Data"
      }, rowIndex);
      await updateCommonAddress(auth, "No Address Found", "No Zip Found", rowIndex);
    }
    return [];
  }

  // --- Wait for detail page
  const pageReady = await retryWait(driver, By.id("main"), 3, 30000);
  if (!pageReady) {
    console.log(`❌ Failed to load details page for ${companyName}`);
    if (rowIndex) {
      await updateSheet(auth, ["No Data"], rowIndex);
      await updateCompanyDetails(auth, {
        registered_name: "No Data",
        status: "No Data",
        mail: "No Data",
        agent: "No Data"
      }, rowIndex);
      await updateCommonAddress(auth, "No Address Found", "No Zip Found", rowIndex);
    }
    return [];
  }

  // --- Scrape details + officers
  const details = await scrapeCompanyDetails(driver);
  const { officerNames, commonStreet, commonZip } = await scrapeOfficerData(driver);
  const cleanNames = officerNames.map(n => n.replace(/[.,'"]/g, "").trim());

  // --- Resolve businesses → individuals
  const resolvedNames = [];
  for (const officer of cleanNames) {
    if (isBusinessEntity(officer)) {
      // recurse into sub-company
      const subNames = await processCompany(auth, driver, officer, null, visited);
      if (subNames?.length) resolvedNames.push(...subNames);
    } else {
      resolvedNames.push(officer);
    }
  }

  // --- Recurse into agent if it’s a business
  if (details.agent && isBusinessEntity(details.agent)) {
    console.log(`↪️ Recursing into agent entity: ${details.agent}`);
    await wait(1500);
    const agentNames = await processCompany(auth, driver, details.agent, null, visited);
    if (agentNames?.length) resolvedNames.push(...agentNames);
  }

  // --- Update Google Sheets
  if (rowIndex) {
    await updateSheet(auth, resolvedNames.length ? resolvedNames : ["No Data"], rowIndex);
    await updateCommonAddress(auth, commonStreet || "No Address Found", commonZip || "No Zip Found", rowIndex);
    await updateCompanyDetails(auth, details, rowIndex);
  }

  console.log(`✅ Logged individuals for ${companyName}:`, resolvedNames);

  // Return officers for recursive calls (only individuals)
  return resolvedNames;
}

// ==========================
// Main Execution
// ==========================
(async function main() {
  const auth = await authenticateGoogleSheets();
  const companies = await getCompanyNames(auth);
  console.log(`✅ Retrieved ${companies.length} companies`);

  const options = new chrome.Options();
  options.addArguments(
    "--headless=new",
    "--disable-gpu",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-logging",
    "--log-level=3",
    "--silent"
  );

  const driver = await new Builder().forBrowser("chrome").setChromeOptions(options).build();

  try {
    const visited = new Set();

    for (const { name: companyName, rowIndex, isBusiness } of companies) {
      if (!isBusiness) {
        console.log(`⏩ Skipping personal name: ${companyName} (Row ${rowIndex})`);
        continue;
      }

      await processCompany(auth, driver, companyName, rowIndex, visited);
      await wait(1500);
    }
  } catch (error) {
    console.error("❌ Error during execution:", error.message);
  } finally {
    await driver.quit();
  }
})();
