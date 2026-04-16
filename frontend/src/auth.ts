import { CONFIG } from './config';

export function getLoginUrl() {
  return `https://${CONFIG.cognitoDomain}/login?client_id=${CONFIG.clientId}&response_type=code&scope=openid+email+profile&redirect_uri=${encodeURIComponent(CONFIG.redirectUri)}`;
}

export function getLogoutUrl() {
  return `https://${CONFIG.cognitoDomain}/logout?client_id=${CONFIG.clientId}&logout_uri=${encodeURIComponent(CONFIG.redirectUri)}`;
}

export function getIdToken(): string | null {
  return sessionStorage.getItem('id_token');
}

export function getAccessToken(): string | null {
  return sessionStorage.getItem('access_token');
}

export function decodeJwt(token: string) {
  try {
    const payload = token.split('.')[1];
    return JSON.parse(atob(payload.replace(/-/g, '+').replace(/_/g, '/')));
  } catch { return null; }
}

export async function exchangeCode(code: string) {
  const tokenUrl = `https://${CONFIG.cognitoDomain}/oauth2/token`;
  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    client_id: CONFIG.clientId,
    code,
    redirect_uri: CONFIG.redirectUri,
  });
  const res = await fetch(tokenUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  });
  if (!res.ok) return null;
  return res.json();
}

export async function handleAuthCallback(): Promise<boolean> {
  const params = new URLSearchParams(window.location.search);
  const code = params.get('code');
  if (!code) return !!getIdToken();

  const tokens = await exchangeCode(code);
  if (tokens?.id_token) {
    sessionStorage.setItem('id_token', tokens.id_token);
    if (tokens.access_token) sessionStorage.setItem('access_token', tokens.access_token);
    window.history.replaceState({}, '', '/');
    return true;
  }
  return false;
}

export function logout() {
  sessionStorage.removeItem('id_token');
  sessionStorage.removeItem('access_token');
  window.location.href = getLogoutUrl();
}
