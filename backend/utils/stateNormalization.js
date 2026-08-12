const stateAliases = require('../state_aliases.json');

const stateLookup = Object.entries(stateAliases).reduce((lookup, [canonicalName, aliases]) => {
  [canonicalName, ...aliases].forEach((alias) => {
    const normalizedAlias = String(alias).toLowerCase().replace(/[^a-z0-9]/g, '');
    lookup[normalizedAlias] = canonicalName;
  });
  return lookup;
}, {});

function normalizeIndianState(value) {
  const text = String(value || '').trim();
  if (!text) {
    return null;
  }

  const normalizedText = text.toLowerCase().replace(/[^a-z0-9]/g, '');
  if (stateLookup[normalizedText]) {
    return stateLookup[normalizedText];
  }

  const matchedEntry = Object.entries(stateLookup).find(
    ([aliasKey]) =>
      normalizedText === aliasKey || normalizedText.includes(aliasKey) || aliasKey.includes(normalizedText)
  );

  return matchedEntry ? matchedEntry[1] : text;
}

function normalizeStateRevenueMap(rawData) {
  return Object.entries(rawData || {}).reduce((normalizedData, [region, value]) => {
    const normalizedState = normalizeIndianState(region);
    if (!normalizedState) {
      return normalizedData;
    }

    normalizedData[normalizedState] = (normalizedData[normalizedState] || 0) + Number(value || 0);
    return normalizedData;
  }, {});
}

module.exports = { normalizeIndianState, normalizeStateRevenueMap };
