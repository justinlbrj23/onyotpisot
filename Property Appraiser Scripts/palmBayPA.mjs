import { Builder, By, until, Key } from 'selenium-webdriver';
import { google } from 'googleapis';
import fs from 'fs';
import path from 'path';
import readline from 'readline';
import edge from 'selenium-webdriver/edge.js';

// Google Sheets setup
const SHEET_ID = '1VUB2NdGSY0l3tuQAfkz8QV2XZpOj2khCB69r5zU1E5A';
const SHEET_NAME = 'Palm Bay - Taxdeed';
const SCOPES = ['https://www.googleapis.com/auth/spreadsheets'];
const CREDENTIALS_PATH = path.join(process.cwd(), 'credentials.json');
const TOKEN_PATH = path.join(process.cwd(), 'token.json');

async function authenticateGoogleSheets() {
const credentials = JSON.parse(fs.readFileSync(CREDENTIALS_PATH));
const { client_id, client_secret, redirect_uris } = credentials.installed;
const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, redirect_uris[0]);

if (fs.existsSync(TOKEN_PATH)) {
const token = JSON.parse(fs.readFileSync(TOKEN_PATH));
oAuth2Client.setCredentials(token);
} else {
const authUrl = oAuth2Client.generateAuthUrl({
access_type: 'offline',
scope: SCOPES,
});
console.log('Authorize this app by visiting this URL:', authUrl);

const rl = readline.createInterface({
input: process.stdin,
output: process.stdout,
});

const code = await new Promise((resolve) =>
rl.question('Enter the code from the page here: ', (code) => {
rl.close();
resolve(code);
})
);

const { tokens } = await oAuth2Client.getToken(code);
oAuth2Client.setCredentials(tokens);
fs.writeFileSync(TOKEN_PATH, JSON.stringify(tokens));
console.log('Token stored to', TOKEN_PATH);
}

return google.sheets({ version: 'v4', auth: oAuth2Client });
}

async function fetchDataAndUpdateSheet() {
const sheets = await authenticateGoogleSheets();
const sheet = await sheets.spreadsheets.values.get({
spreadsheetId: SHEET_ID,
range: `${SHEET_NAME}!D2:D`,
});

const sheetData = sheet.data.values;

const url = 'https://www.bcpao.us/propertysearch/#/nav/Search';
const options = new edge.Options();

for (let i = 0; i < sheetData.length; i++) {
let driver = await new Builder().forBrowser('MicrosoftEdge').setEdgeOptions(options).build();

await driver.get(url);
const Site = sheetData[i][0];
console.log(`Processing Name: ${Site}`);

if (!Site || Site.trim() === '') {
console.log(`Skipping empty or blank cell at row ${i + 2}`);
continue;
}

try {
const siteInput = await driver.wait(
until.elementLocated(By.xpath('//*[@id="txtPropertySearch_Account"]')),
60000
);
await siteInput.sendKeys(Site, Key.RETURN);

await driver.sleep(4000)

const ownershipText = await driver.findElement(By.xpath('//*[@id="cssDetails_Top_Outer"]/div[2]/div/div[1]/div[2]/div[1]')).getText();
await sheets.spreadsheets.values.update({
spreadsheetId: SHEET_ID,
range: `${SHEET_NAME}!J${i + 2}`,
valueInputOption: 'RAW',
resource: { values: [[ownershipText]] },
});

const additionalText = await driver.findElement(By.xpath('//*[@id="cssDetails_Top_Outer"]/div[2]/div/div[2]/div[2]/div')).getText();
await sheets.spreadsheets.values.update({
spreadsheetId: SHEET_ID,
range: `${SHEET_NAME}!I${i + 2}`,
valueInputOption: 'RAW',
resource: { values: [[additionalText]] },
});


const propertyValue = await driver.wait(until.elementLocated(By.xpath('//*[@id="tSalesTransfers"]/tbody/tr[1]/td[2]')),60000);
const propertyValueText = await propertyValue.getText();
await sheets.spreadsheets.values.update({
spreadsheetId: SHEET_ID,
range: `${SHEET_NAME}!K${i + 2}`,
valueInputOption: 'RAW',
resource: { values: [[propertyValueText]] },
});

const BldgInfo = await driver.findElement(By.xpath('//*[@id="cssDetails_Top_Outer"]/div[2]/div/div[7]/div[2]')).getText();
await sheets.spreadsheets.values.update({
spreadsheetId: SHEET_ID,
range: `${SHEET_NAME}!L${i + 2}`,
valueInputOption: 'RAW',
resource: { values: [[BldgInfo]] },
});

} catch (error) {
console.log(`Error processing row ${i + 2}:`, error);
} finally {
await driver.quit();
}
}
}

(async () => {
await fetchDataAndUpdateSheet();
})();