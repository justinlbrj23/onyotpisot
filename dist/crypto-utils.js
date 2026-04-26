// crypto-utils.js

/**
 * Converts a Uint8Array of bytes into a hex‐encoded string.
 *
 * @param {Uint8Array} bytes
 * @returns {string}
 */
export function bytesToHex(bytes) {
  return Array.from(bytes)
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
}

/**
 * Converts a hex‐encoded string into a Uint8Array.
 *
 * @param {string} hex
 * @returns {Uint8Array}
 */
export function hexToBytes(hex) {
  const clean = hex.replace(/[^0-9a-f]/gi, '');
  if (clean.length % 2 !== 0) {
    throw new Error('hexToBytes: invalid hex string');
  }
  const bytes = new Uint8Array(clean.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

/**
 * Decode a PEM-formatted key string to ArrayBuffer bytes.
 * Supports PEM headers like "-----BEGIN PRIVATE KEY-----"
 *
 * @param {string} pem
 * @returns {ArrayBuffer}
 */
export function decodePem(pem) {
  if (!/^-----BEGIN [\w\s]+ KEY-----/.test(pem.trim())) {
    throw new Error('decodePem: unsupported or missing PEM header');
  }
  const b64 = pem
    .replace(/-----(BEGIN|END)[\w\s]+-----/g, '')
    .replace(/\s+/g, '');
  const binaryStr = atob(b64);
  const bytes = new Uint8Array(binaryStr.length);
  for (let i = 0; i < binaryStr.length; i++) {
    bytes[i] = binaryStr.charCodeAt(i);
  }
  return bytes.buffer;
}

/**
 * Imports a PEM-formatted RSA or ECDSA private key for signing.
 *
 * @param {string} pem - PEM-formatted key string
 * @param {'RSA'|'ECDSA'} type - Key type
 * @returns {Promise<CryptoKey>}
 */
export async function importKeyFromPem(pem, type) {
  const keyBuffer = decodePem(pem);
  if (type === 'RSA') {
    return crypto.subtle.importKey(
      'pkcs8',
      keyBuffer,
      { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
      false,
      ['sign']
    );
  } else if (type === 'ECDSA') {
    return crypto.subtle.importKey(
      'pkcs8',
      keyBuffer,
      { name: 'ECDSA', namedCurve: 'P-256' },
      false,
      ['sign']
    );
  } else {
    throw new Error(`importKeyFromPem: unsupported key type '${type}'`);
  }
}

/**
 * Export a CryptoKey to JWK (JSON Web Key) format.
 *
 * @param {CryptoKey} key
 * @returns {Promise<JsonWebKey>}
 */
export async function exportKeyToJwk(key) {
  return crypto.subtle.exportKey('jwk', key);
}

/**
 * Computes the SHA‐256 digest of a UTF-8 string and returns it as a hex string.
 *
 * @param {string} text
 * @returns {Promise<string>}
 */
export async function sha256Hex(text) {
  const data = new TextEncoder().encode(text);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  return bytesToHex(new Uint8Array(hashBuffer));
}

/**
 * Generates a P-256 ECDSA key pair for signing & verification.
 *
 * @returns {Promise<CryptoKeyPair>}
 */
export async function genSigningKey() {
  return crypto.subtle.generateKey(
    { name: 'ECDSA', namedCurve: 'P-256' },
    true,               // extractable
    ['sign', 'verify']  // usages
  );
}

/**
 * Signs a UTF-8 message with a private ECDSA key,
 * returning the raw ArrayBuffer of the signature.
 *
 * @param {CryptoKey} privateKey
 * @param {string} message
 * @returns {Promise<ArrayBuffer>}
 */
export async function signData(privateKey, message) {
  const data = new TextEncoder().encode(message);
  return crypto.subtle.sign(
    { name: 'ECDSA', hash: 'SHA-256' },
    privateKey,
    data
  );
}

/**
 * Signs a UTF-8 message with a private ECDSA key,
 * returning a hex-encoded signature string.
 *
 * @param {CryptoKey} privateKey
 * @param {string} message
 * @returns {Promise<string>}
 */
export async function signDataHex(privateKey, message) {
  const sigBuf = await signData(privateKey, message);
  return bytesToHex(new Uint8Array(sigBuf));
}

/**
 * Verifies an ECDSA signature against a UTF-8 message.
 *
 * @param {CryptoKey} publicKey
 * @param {string} message
 * @param {string|Uint8Array|ArrayBuffer|number[]} signature
 * @returns {Promise<boolean>}
 */
export async function verifyData(publicKey, message, signature) {
  let sigBytes;

  if (typeof signature === 'string') {
    sigBytes = hexToBytes(signature);
  } else if (signature instanceof ArrayBuffer) {
    sigBytes = new Uint8Array(signature);
  } else if (signature instanceof Uint8Array) {
    sigBytes = signature;
  } else if (Array.isArray(signature)) {
    sigBytes = new Uint8Array(signature);
  } else {
    throw new Error('verifyData: unsupported signature format');
  }

  const data = new TextEncoder().encode(message);
  return crypto.subtle.verify(
    { name: 'ECDSA', hash: 'SHA-256' },
    publicKey,
    sigBytes,
    data
  );
}

/**
 * Signs a UTF-8 message with a private RSA key (RSASSA-PKCS1-v1_5),
 * returning the raw ArrayBuffer of the signature.
 *
 * @param {CryptoKey} privateKey
 * @param {string} message
 * @returns {Promise<ArrayBuffer>}
 */
export async function signRS256(privateKey, message) {
  const data = new TextEncoder().encode(message);
  return crypto.subtle.sign(
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
    privateKey,
    data
  );
}

/* -----------------------
 * Config loader & logging
 * -----------------------
 */

let _configCache = null;

/**
 * Loads config.json dynamically at runtime.
 * @returns {Promise<Object>}
 */
async function loadConfig() {
  if (_configCache) return _configCache;
  try {
    const resp = await fetch(chrome.runtime.getURL('config.json'));
    if (!resp.ok) throw new Error(`Failed to load config.json: ${resp.statusText}`);
    _configCache = await resp.json();
    return _configCache;
  } catch (err) {
    console.error('[crypto-utils] Failed to load config.json:', err);
    return {};
  }
}

/**
 * Logs a message if logging is enabled in config.
 */
export async function log(...args) {
  const config = await loadConfig();
  if (config.logging?.enabled) console.log('[LOG]', ...args);
}

/**
 * Logs a debug message if logging is enabled and level is 'debug'.
 */
export async function debug(...args) {
  const config = await loadConfig();
  if (config.logging?.enabled && config.logging.level === 'debug') {
    console.debug('[DEBUG]', ...args);
  }
}

/**
 * Logs a warning message if logging is enabled.
 */
export async function warn(...args) {
  const config = await loadConfig();
  if (config.logging?.enabled) console.warn('[WARN]', ...args);
}

/**
 * Logs an error message if logging is enabled.
 */
export async function error(...args) {
  const config = await loadConfig();
  if (config.logging?.enabled) console.error('[ERROR]', ...args);
}