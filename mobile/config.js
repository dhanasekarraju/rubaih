/**
 * Defaults only. Prefer editing VPS IP + token inside the app Settings tab.
 * Values save on the phone (AsyncStorage) — no rebuild needed after first install.
 *
 * Phone must use nginx port 80:
 *   http://YOUR_VPS_IP
 * NOT http://YOUR_VPS_IP:8000  (API is localhost-only behind nginx)
 */

export const DEFAULT_API_HOST = 'http://YOUR_VPS_IP';
export const DEFAULT_API_TOKEN = 'YOUR_RUBAIH_API_TOKEN';

export function normalizeHost(host) {
  let h = (host || '').trim().replace(/\/+$/, '');
  if (!h) return '';
  if (!/^https?:\/\//i.test(h)) h = `http://${h}`;
  // strip accidental :8000 — public traffic is nginx :80
  h = h.replace(/:8000$/, '');
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
