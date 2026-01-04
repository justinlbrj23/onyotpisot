const vision = require("@google-cloud/vision");
const fs = require("fs");

const client = new vision.ImageAnnotatorClient({
  keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS
});

const filePath = process.argv[2];

if (!filePath) {
  console.error("❌ Usage: node ocr_google_vision.cjs <file>");
  process.exit(1);
}

(async () => {
  const [result] = await client.textDetection(filePath);
  const detections = result.textAnnotations;

  if (!detections.length) {
    console.log("No text detected.");
    return;
  }

  fs.writeFileSync("ocr_output.txt", detections[0].description);
  console.log("✅ OCR complete (Google Vision)");
})();