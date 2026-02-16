function inspectPage() {
  const elements = [];
  document.querySelectorAll('*').forEach(el => {
    const text = el.innerText.trim();
    if (text) {
      const attrs = {};
      for (const attr of el.attributes) {
        attrs[attr.name] = attr.value;
      }
      elements.push({ tag: el.tagName, text, attrs });
    }
  });
  browser.runtime.sendMessage({ type: 'INSPECT_RESULTS', data: elements });
}

function searchPage(zipcode) {
  const input = document.querySelector('input[type="text"], input[type="search"]');
  if (!input) {
    browser.runtime.sendMessage({ type: 'SEARCH_ERROR', message: 'No input found' });
    return;
  }
  input.value = zipcode;
  input.dispatchEvent(new Event('input', { bubbles: true }));
  input.form?.submit();

  setTimeout(() => {
    const results = [];
    document.querySelectorAll('.resultRow, li, tr').forEach(r => {
      results.push({ name: r.innerText.split('\n')[0], details: r.innerText });
    });
    browser.runtime.sendMessage({ type: 'SEARCH_RESULTS', data: results });
  }, 3000);
}

browser.runtime.onMessage.addListener((msg) => {
  if (msg.type === 'INSPECT') inspectPage();
  if (msg.type === 'SEARCH') searchPage(msg.zip);
});