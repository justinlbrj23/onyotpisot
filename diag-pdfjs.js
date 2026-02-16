// diag-pdfjs.js
const fs = require('fs');
const paths = [
  'node_modules/pdfjs-dist/legacy/build/pdf.js',
  'node_modules/pdfjs-dist/build/pdf.js',
  'node_modules/pdfjs-dist/legacy/build/pdf.node.js',
  'node_modules/pdfjs-dist/build/pdf'
];

console.log('Checking file existence under node_modules/pdfjs-dist:');
paths.forEach(p => {
  try {
    console.log(p, fs.existsSync(p) ? 'FOUND' : 'MISSING');
  } catch (e) {
    console.log(p, 'ERROR', e.message);
  }
});

console.log('\nTrying require.resolve for common entries:');
['pdfjs-dist/legacy/build/pdf.js','pdfjs-dist/build/pdf.js','pdfjs-dist/build/pdf'].forEach(p => {
  try {
    console.log(p, '->', require.resolve(p));
  } catch (e) {
    console.log(p, '-> not resolvable:', e.message);
  }
});