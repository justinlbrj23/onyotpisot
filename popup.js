document.getElementById('inspect').addEventListener('click', () => {
  browser.tabs.query({ active: true, currentWindow: true }).then(tabs => {
    browser.tabs.sendMessage(tabs[0].id, { type: 'INSPECT' });
  });
});

document.getElementById('search').addEventListener('click', () => {
  const zip = document.getElementById('zip').value;
  browser.tabs.query({ active: true, currentWindow: true }).then(tabs => {
    browser.tabs.sendMessage(tabs[0].id, { type: 'SEARCH', zip });
  });
});