function isLikelyList(text) {
  return /^\s*([-*]|\d+\.)\s+/m.test(String(text || ''));
}

function toIndianCompactNumber(value) {
  const amount = Number(value);
  if (!Number.isFinite(amount)) {
    return null;
  }

  const absValue = Math.abs(amount);
  const sign = amount < 0 ? '-' : '';

  if (absValue >= 10000000) {
    const crores = (absValue / 10000000).toFixed(absValue >= 100000000 ? 1 : 2);
    return `${sign}${crores} crore`;
  }

  if (absValue >= 100000) {
    const lakhs = (absValue / 100000).toFixed(absValue >= 1000000 ? 1 : 2);
    return `${sign}${lakhs} lakh`;
  }

  return `${sign}${absValue.toLocaleString('en-IN', { maximumFractionDigits: 0 })}`;
}

function formatNumericToken(token) {
  const normalized = String(token || '').replace(/[₹, ]/g, '');
  if (!normalized || !/^-?\d+(?:\.\d+)?$/.test(normalized)) {
    return token;
  }

  const value = Number(normalized);
  if (!Number.isFinite(value) || Math.abs(value) < 100000) {
    return token;
  }

  const compact = toIndianCompactNumber(value);
  if (!compact) {
    return token;
  }

  const hasRupee = String(token || '').includes('₹');
  return hasRupee ? `₹${compact}` : compact;
}

function normalizeIndianNumerals(text) {
  return String(text || '').replace(/₹?\s?-?\d[\d,]*(?:\.\d+)?/g, (match) => {
    const cleaned = String(match).replace(/\s+/g, '');
    const formatted = formatNumericToken(cleaned);
    return formatted === cleaned ? match : formatted;
  });
}

function normalizeResponseLayout(text) {
  return String(text || '')
    .replace(/\r\n?/g, '\n')
    .replace(/\*\*|__/g, '')
    .replace(/`/g, '')
    // Convert Markdown headings and horizontal rules into ordinary line breaks.
    .replace(/\s*---+\s*/g, '\n')
    .replace(/(^|\s)#{1,6}\s*/g, '$1\n')
    // Models sometimes put an entire report on one line. Restore each list item
    // to its own line before the response is turned into a readable list.
    .replace(/\s+(?=(?:[-*•])\s+(?=[A-Zâ‚¹0-9]))/g, '\n')
    .replace(/\s+(?=\d+\.\s*(?:[-*•]\s*)?(?=[A-Zâ‚¹]))/g, '\n')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function splitIntoPoints(text) {
  const normalized = normalizeResponseLayout(text);
  if (!normalized) {
    return [];
  }

  if (isLikelyList(normalized)) {
    return normalized
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => line.replace(/^\s*([-*]|\d+\.)\s+/, '').trim())
      .filter(Boolean);
  }

  const sentenceParts = normalized
    .split(/\n+/)
    .flatMap((line) => line.split(/(?<=[.!?])\s+(?=[A-Z₹0-9])/g))
    .map((part) => part.trim())
    .filter(Boolean);

  if (sentenceParts.length <= 1) {
    return [normalized];
  }

  return sentenceParts;
}

function formatAsPoints(reply) {
  const points = splitIntoPoints(normalizeIndianNumerals(reply));
  if (points.length <= 1) {
    return points[0] || '';
  }

  return points.map((point) => `- ${point}`).join('\n');
}

function formatYuaReply(reply, intent = {}) {
  const text = normalizeResponseLayout(reply);
  if (!text) {
    return '';
  }

  if (intent?.needsLiveData || intent?.mode === 'both') {
    return formatAsPoints(text);
  }

  return normalizeIndianNumerals(text);
}

module.exports = {
  formatAsPoints,
  formatIndianCompactNumber: toIndianCompactNumber,
  formatYuaReply,
  isLikelyList,
  normalizeIndianNumerals,
};
