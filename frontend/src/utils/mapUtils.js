export const MAP_STATE_NAME_ALIASES = {
  Delhi: 'NCT of Delhi',
  'New Delhi': 'NCT of Delhi',
  Pondicherry: 'Puducherry',
  Kerla: 'Kerala',
  Tamilnadu: 'Tamil Nadu',
  'Tamil Nadu ': 'Tamil Nadu',
  Chhatisgarh: 'Chhattisgarh',
  Chattisgarh: 'Chhattisgarh',
  UP: 'Uttar Pradesh',
  'U.P.': 'Uttar Pradesh',
  'Andra Pradesh': 'Andhra Pradesh',
};

export function normalizeMapStateName(value) {
  const text = String(value || '').trim();
  return MAP_STATE_NAME_ALIASES[text] || text;
}
