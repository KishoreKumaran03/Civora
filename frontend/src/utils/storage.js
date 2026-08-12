export const FAVORITE_PROJECTS_STORAGE_KEY = 'favoriteProjectIds';

export function getStoredFavoriteProjectIds() {
  try {
    const parsed = JSON.parse(localStorage.getItem(FAVORITE_PROJECTS_STORAGE_KEY) || '[]');
    return Array.isArray(parsed) ? parsed.map((value) => String(value)) : [];
  } catch {
    return [];
  }
}

export function setStoredFavoriteProjectIds(projectIds) {
  localStorage.setItem(FAVORITE_PROJECTS_STORAGE_KEY, JSON.stringify(projectIds.map((value) => String(value))));
}
