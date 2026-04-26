import fs from "fs";

const BASE_URL =
  "https://services.arcgis.com/Tbke9ca9DhtF4VIx/arcgis/rest/services/Parcel_Polygons_working/FeatureServer/0/query";

const MAX_RECORDS = 2000;

const WHERE = process.env.WHERE || "1=1";
const OUT_FIELDS = process.env.OUT_FIELDS || "*";
const OUTPUT_FILE =
  process.env.OUTPUT_FILE ||
  `parcels_${new Date().toISOString().split("T")[0]}.csv`;

async function fetchBatch(offset, retries = 3) {
  const url = `${BASE_URL}?where=${encodeURIComponent(
    WHERE
  )}&outFields=${OUT_FIELDS}&returnGeometry=false&f=json&resultOffset=${offset}&resultRecordCount=${MAX_RECORDS}`;

  for (let i = 0; i < retries; i++) {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      return data.features || [];
    } catch (err) {
      console.log(`Retry ${i + 1} for offset ${offset}...`);
      await new Promise(r => setTimeout(r, 1000));
    }
  }

  throw new Error(`Failed at offset ${offset}`);
}

function escapeCSV(value) {
  if (value == null) return "";
  const str = String(value);
  if (/[",\n]/.test(str)) return `"${str.replace(/"/g, '""')}"`;
  return str;
}

async function main() {
  const writeStream = fs.createWriteStream(OUTPUT_FILE);

  let offset = 0;
  let headers = null;
  let total = 0;

  try {
    while (true) {
      console.log(`Fetching offset: ${offset}`);

      const batch = await fetchBatch(offset);
      console.log(`Fetched: ${batch.length}`);

      if (!batch.length) break;

      const records = batch.map(f => f.attributes);

      if (!headers) {
        headers = [...new Set(records.flatMap(r => Object.keys(r)))];
        writeStream.write(headers.join(",") + "\n");
      }

      for (const row of records) {
        const line = headers.map(h => escapeCSV(row[h])).join(",");
        writeStream.write(line + "\n");
        total++;
      }

      if (batch.length < MAX_RECORDS) break;
      offset += MAX_RECORDS;
    }

    writeStream.end();

    console.log(`✅ Done`);
    console.log(`File: ${OUTPUT_FILE}`);
    console.log(`Rows: ${total}`);
  } catch (err) {
    console.error("❌ Error:", err);
    writeStream.end();
    process.exit(1);
  }
}

main();
