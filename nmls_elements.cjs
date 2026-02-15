const fetch = require("node-fetch");
const { google } = require("googleapis");

// CONFIG
const SERVICE_ACCOUNT_FILE = "./service-account.json";

const SPREADSHEET_ID =
  "1CAEdjXisPmgAHmv3qo3y1LBYktQftLKHk-LK04_oKes";

const SHEET_RANGE = "Sheet1!A:E";


// GOOGLE AUTH

const auth = new google.auth.GoogleAuth({

  keyFile: SERVICE_ACCOUNT_FILE,

  scopes: [

    "https://www.googleapis.com/auth/spreadsheets",

  ],

});

const sheets = google.sheets({

  version: "v4",

  auth,

});


// FETCH NMLS DATA

async function fetchNMLS() {

  const url =
    "https://www.nmlsconsumeraccess.org/Home.aspx/SubSearch";

  const res = await fetch(url, {

    method: "POST",

    headers: {

      "Content-Type":
        "application/json; charset=UTF-8",

      "X-Requested-With":
        "XMLHttpRequest",

    },

    body: JSON.stringify({

      searchText: "",

      entityType: "INDIVIDUAL",

      pageIndex: 1,

      pageSize: 100,

    }),

  });

  const json = await res.json();

  return json.Results || [];

}


// WRITE TO SHEETS

async function writeSheet(data) {

  if (!data.length) {

    console.log("No results");

    return;

  }

  const timestamp =
    new Date().toISOString();

  const values = data.map(r => [

    timestamp,

    r.Name,

    r.NMLSNumber,

    r.City,

    r.State,

  ]);

  values.unshift([

    "Timestamp",

    "Name",

    "NMLS",

    "City",

    "State",

  ]);

  await sheets.spreadsheets.values.append({

    spreadsheetId:
      SPREADSHEET_ID,

    range: SHEET_RANGE,

    valueInputOption: "RAW",

    requestBody: {

      values,

    },

  });

  console.log(
    `✅ Wrote ${data.length} records`
  );

}


// MAIN

(async () => {

  console.log("Fetching NMLS...");

  const data =
    await fetchNMLS();

  console.log(
    "Records:",
    data.length
  );

  await writeSheet(data);

})();