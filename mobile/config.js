/**
 * Defaults only. Prefer editing VPS IP + token inside the app Settings tab.
 * Values save on the phone (AsyncStorage) — no rebuild needed after first install.
 *
 * Futures nginx is on host port 8080 (not 80):
 *   http://YOUR_VPS_IP:8080
 * NOT http://YOUR_VPS_IP:8000 or :8010 (API is localhost-only behind nginx)
 */

export const DEFAULT_API_HOST = 'http://YOUR_VPS_IP:8080';
export const DEFAULT_API_TOKEN = 'YOUR_RUBAIH_API_TOKEN';

export function normalizeHost(host) {
  let h = (host || '').trim().replace(/\/+$/, '');
  if (!h) return '';
  if (!/^https?:\/\//i.test(h)) h = `http://${h}`;
  // Mistaken direct-API ports → public nginx
  h = h.replace(/:(8000|8010)(?=\/|$)/, ':8080');
  try {
    const u = new URL(h);
    // Compose publishes nginx on 8080; bare http://IP hits nothing useful on :80
    if (u.protocol === 'http:' && !u.port) {
      u.port = '8080';
    }
    h = `${u.protocol}//${u.host}`;
  } catch (_) {
    /* keep h */
  }
  return h;
}

export function buildUrls(host, token) {
  const apiHost = normalizeHost(host);
  const apiToken = (token || '').trim();
  return {
    apiHost,
    apiToken,
    apiUrl: `${apiHost}/api`,
    wsUrl: `${apiHost.replace(/^http/i, 'ws')}/ws?token=${encodeURIComponent(apiToken)}`,
    authHeaders: {
      'Content-Type': 'application/json',
      'X-API-Token': apiToken,
    },
    configured: Boolean(
      apiHost &&
      !apiHost.includes('YOUR_VPS_IP') &&
      apiToken &&
      !apiToken.includes('YOUR_RUBAIH')
    ),
  };
}
