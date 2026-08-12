import axios from 'axios';
import authStore from '../store/authStore';

if (!import.meta.env.VITE_API_URL) {
  throw new Error('Missing required environment variable: VITE_API_URL');
}

const configuredApiBase = import.meta.env.VITE_API_URL.replace(/\/$/, '');
let activeApiBase = configuredApiBase;

// Global interceptor: auto-logout on 401/403 (expired or invalid token)
axios.interceptors.response.use(
  (response) => response,
  (error) => {
    const status = error.response?.status;
    if (status === 401 || status === 403) {
      // Clear stale credentials
      authStore.logout();
      // Redirect to login page
      if (typeof window !== 'undefined' && !window.location.pathname.startsWith('/login')) {
        window.location.href = '/login';
      }
    }
    return Promise.reject(error);
  }
);

export async function apiRequest(config) {
  const candidates = [activeApiBase];
  let lastError;

  for (const base of candidates) {
    try {
      const response = await axios({
        ...config,
        url: `${base}${config.url}`,
      });
      activeApiBase = base;
      return response;
    } catch (error) {
      const status = error.response?.status;
      // Don't retry on auth errors — interceptor already handled them
      const shouldTryNext = !error.response || (status !== 401 && status !== 403 && (status === 404 || status >= 500));

      if (!shouldTryNext || base === candidates[candidates.length - 1]) {
        throw error;
      }

      lastError = error;
    }
  }

  throw lastError;
}
