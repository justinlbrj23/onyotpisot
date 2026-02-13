import fs from "fs";
import xlsx from "xlsx";

// --------------------------------------
// HELPERS
// --------------------------------------
function cleanValues(values) {
  return values
    .flat(Infinity)                // flatten nested(v => (v || "").toString().trim())
    .filter(v => v.length > 0);    // remove blanks
}

function uniqueCount(values) {
  return new Set(values).size;
}

// --------------------------------------
// EXCEL HANDLER
// --------------------------------------
function countExcel(filePath) {
  if (!fs.existsSync(filePath)) {
    console.warn(`⚠️ Skipping missing file: ${filePath}`);
    return { file: filePath, raw: 0, unique: 0 };
  }

  const wb = xlsx.readFile(filePath);
  let allValues = [];
  wb.SheetNames.forEach(name => {
    const ws = wb.Sheets[name];
    const data = xlsx.utils.sheet_to_json(ws, { header: 1 });
    allValues.push(...cleanValues(data));
  });

  const rawCount = allValues.length;
  const unique = uniqueCount(allValues);

  console.log(
    `📊 Excel ${filePath} → Raw: ${rawCount}, Unique: ${unique}, Duplicates removed: ${rawCount - unique}`
  );

  return { file: filePath, raw: rawCount, unique };
}

// --------------------------------------
// MAIN
// --------------------------------------
async function run() {
  // ✅ Only include the Excel files you actually have
  const excelFiles = ["./file1.xlsx"];

  let grandTotal = 0;

  for (const file of excelFiles) {
    const { unique } = countExcel(file);
    grandTotal += unique;
  }

  console.log(`🎯 Grand Total Unique Count: ${grandTotal}`);
}

run().catch(err => console.error("❌ Fatal Error:", err));