browser.runtime.onMessage.addListener((msg, sender) => {
  if (msg.type === 'INSPECT_RESULTS' || msg.type === 'SEARCH_RESULTS') {
    console.log("Received data:", msg.data);

    // Store results locally
    browser.storage.local.set({ lastResults: msg.data });

    // Placeholder: Google Sheets integration
    // In Firefox, you’ll need to implement OAuth2 manually or send data to a backend.
    // Example:
    // fetch("https://sheets.googleapis.com/v4/spreadsheets/.../values/Sheet1!A:D:append", {
    //   method: "POST",
    //   headers: { "Authorization": "Bearer <token>", "Content-Type": "application/json" },
    //   body: JSON.stringify({ values: msg.data })
    // });
  }
});