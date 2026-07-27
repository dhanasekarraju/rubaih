/**
 * Rubaih mobile runtime config — PRODUCTION
 *
 * Before building the APK:
 * 1. Set API_HOST to your VPS IP or domain (no trailing slash)
 * 2. Set API_TOKEN to the same RUBAIH_API_TOKEN as in server .env
 *
 * setup-vps.sh can patch API_HOST automatically.
 * Prefer nginx :80 (no :8000) so traffic goes through the reverse proxy.
 */

export const API_HOST = 'http://YOUR_VPS_IP';
export const API_TOKEN = 'YOUR_RUBAIH_API_TOKEN';

export const API_URL = `${API_HOST}/api`;
export const WS_URL = `${API_HOST.replace(/^http/, 'ws')}/ws?token=${encodeURIComponent(API_TOKEN)}`;

export const AUTH_HEADERS = {
  'Content-Type': 'application/json',
  'X-API-Token': API_TOKEN,
};
