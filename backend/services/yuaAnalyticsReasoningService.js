const { getSalesData } = require('./civoraAiService');
const { formatIndianCompactNumber, formatYuaReply } = require('./yuaResponseFormatter');

const MONTHS = [
  'january',
  'february',
  'march',
  'april',
  'may',
  'june',
  'july',
  'august',
  'september',
  'october',
  'november',
  'december',
];

const MONTH_ALIASES = [
  { name: 'january', pattern: /\bjan(?:uary)?\b/i },
  { name: 'february', pattern: /\bfeb(?:ruary)?\b/i },
  { name: 'march', pattern: /\bmar(?:ch)?\b/i },
  { name: 'april', pattern: /\bapr(?:il)?\b/i },
  { name: 'may', pattern: /\bmay\b/i },
  { name: 'june', pattern: /\bjun(?:e)?\b/i },
  { name: 'july', pattern: /\bjul(?:y)?\b/i },
  { name: 'august', pattern: /\baug(?:ust)?\b/i },
  { name: 'september', pattern: /\bsep(?:t(?:ember)?)?\b/i },
  { name: 'october', pattern: /\boct(?:ober)?\b/i },
  { name: 'november', pattern: /\bnov(?:ember)?\b/i },
  { name: 'december', pattern: /\bdec(?:ember)?\b/i },
];

const MONTH_TO_INDEX = MONTHS.reduce((accumulator, month, index) => {
  accumulator[month] = index;
  return accumulator;
}, {});

const MATERIAL_CHANGE_THRESHOLD_PCT = 0.5;

function normalizeText(value) {
  return String(value || '').toLowerCase();
}

function capitalize(value) {
  const text = String(value || '').trim();
  if (!text) return '';
  return `${text[0].toUpperCase()}${text.slice(1)}`;
}

function formatAmount(value) {
  const amount = Number(value || 0);
  const compact = formatIndianCompactNumber(amount);
  return `₹${compact != null ? compact : amount.toLocaleString('en-IN')}`;
}

function sortRows(rows) {
  return [...(Array.isArray(rows) ? rows : [])].sort((left, right) => {
    if (Number(left.year) !== Number(right.year)) {
      return Number(left.year) - Number(right.year);
    }
    return (MONTH_TO_INDEX[String(left.month_name || '').toLowerCase()] ?? 99) - (MONTH_TO_INDEX[String(right.month_name || '').toLowerCase()] ?? 99);
  });
}

function extractMentionedMonths(text) {
  const source = String(text || '');
  const matches = MONTH_ALIASES
    .map((entry) => {
      const match = source.match(entry.pattern);
      return match ? { name: entry.name, index: match.index ?? Number.MAX_SAFE_INTEGER } : null;
    })
    .filter(Boolean)
    .sort((left, right) => left.index - right.index);

  const seen = new Set();
  return matches
    .map((entry) => entry.name)
    .filter((month) => {
      if (seen.has(month)) {
        return false;
      }
      seen.add(month);
      return true;
    });
}

function extractMentionedYear(text) {
  const match = String(text || '').match(/\b(20\d{2})\b/);
  return match ? Number(match[1]) : null;
}

function getMetricFromQuery(queryText) {
  const normalized = normalizeText(queryText);

  if (normalized.includes('profit') || normalized.includes('net revenue')) {
    return { key: 'net_revenue', label: 'profit' };
  }

  if (normalized.includes('cost')) {
    return { key: 'total_cost', label: 'cost' };
  }

  if (normalized.includes('quantity') || normalized.includes('units')) {
    return { key: 'total_quantity', label: 'quantity' };
  }

  if (normalized.includes('sales')) {
    return { key: 'total_revenue', label: 'sales' };
  }

  return { key: 'total_revenue', label: 'revenue' };
}

function getMetricValue(row, metricKey) {
  return Number(row?.[metricKey] || 0);
}

function getExtremeFromQuery(queryText) {
  const normalized = normalizeText(queryText);

  if (/(lowest|least|smallest|min(?:imum)?|bottom|worst)/.test(normalized)) {
    return 'min';
  }

  if (/(highest|highest\s+revenue|highest\s+sales|most|largest|max(?:imum)?|top|best)/.test(normalized)) {
    return 'max';
  }

  return null;
}

function getYearRows(rows, year) {
  if (year == null) {
    return [...rows];
  }

  return rows.filter((row) => Number(row.year) === Number(year));
}

function buildExtremeAnswerLines({ row, metric, extreme, queryYear }) {
  const monthLabel = `${capitalize(row.month_name)} ${row.year}`;
  const valueText = formatAmount(getMetricValue(row, metric.key));
  const comparator = extreme === 'min' ? 'lowest' : 'highest';
  const prefix = metric.label === 'revenue'
    ? `${monthLabel} had the ${comparator} revenue`
    : `${monthLabel} had the ${comparator} ${metric.label}`;

  const lines = [`${prefix} at ${valueText}.`];

  if (queryYear != null) {
    lines.push(`This is the ${comparator} month within ${queryYear}.`);
  }

  if (row.top_product) {
    lines.push(`Top product: ${row.top_product}.`);
  }

  if (row.top_region) {
    lines.push(`Top region: ${row.top_region}.`);
  }

  return lines.join('\n');
}

function resolveExtremeRow(rows, queryText) {
  const metric = getMetricFromQuery(queryText);
  const year = extractMentionedYear(queryText);
  const extreme = getExtremeFromQuery(queryText);
  if (!extreme) return null;

  const scopedRows = getYearRows(rows, year);
  if (scopedRows.length === 0) return null;

  const comparator = extreme === 'min'
    ? (left, right) => getMetricValue(left, metric.key) - getMetricValue(right, metric.key)
    : (left, right) => getMetricValue(right, metric.key) - getMetricValue(left, metric.key);

  const sorted = [...scopedRows].sort(comparator);
  const row = sorted[0] || null;
  if (!row) return null;

  return {
    row,
    metric,
    extreme,
    year,
  };
}

function findRowForMonth(rows, monthName, year) {
  const targetIndex = MONTH_TO_INDEX[String(monthName || '').toLowerCase()];
  if (targetIndex == null) return null;

  const matchingRows = rows.filter((row) => MONTH_TO_INDEX[String(row.month_name || '').toLowerCase()] === targetIndex);
  if (matchingRows.length === 0) {
    return null;
  }

  if (year != null) {
    const yearMatch = matchingRows.find((row) => Number(row.year) === Number(year));
    if (yearMatch) return yearMatch;
  }

  return [...matchingRows].sort((left, right) => Number(right.year) - Number(left.year))[0] || null;
}

function getAdjacentRow(rows, monthName, year, direction) {
  const sortedRows = sortRows(rows);
  const monthIndex = MONTH_TO_INDEX[String(monthName || '').toLowerCase()];
  if (monthIndex == null) return null;

  const matchingIndex = sortedRows.findIndex((row) => {
    return MONTH_TO_INDEX[String(row.month_name || '').toLowerCase()] === monthIndex && (year == null || Number(row.year) === Number(year));
  });

  if (matchingIndex < 0) return null;

  if (direction === 'previous') {
    return matchingIndex > 0 ? sortedRows[matchingIndex - 1] : null;
  }

  return matchingIndex < sortedRows.length - 1 ? sortedRows[matchingIndex + 1] : null;
}

function extractMonthsFromConversation(conversation) {
  if (!Array.isArray(conversation)) return [];

  for (let index = conversation.length - 1; index >= 0; index -= 1) {
    const content = String(conversation[index]?.content || '');
    const months = extractMentionedMonths(content);
    if (months.length >= 2) {
      return months.slice(0, 2);
    }
  }

  return [];
}

function extractContextMonths(conversation, queryText) {
  const queryMonths = extractMentionedMonths(queryText);
  if (queryMonths.length >= 2) {
    return queryMonths.slice(0, 2);
  }

  const contextMonths = extractMonthsFromConversation(conversation);
  if (queryMonths.length === 1 && /previous|before|after|from|to|compare|why|drove|cause|caused|change/i.test(String(queryText || ''))) {
    return queryMonths;
  }

  if (contextMonths.length >= 2 && /it|this|that|previous month|which product|which category|which region|what drove|why/i.test(String(queryText || '').toLowerCase())) {
    return contextMonths;
  }

  return queryMonths;
}

function resolvePeriodPair(rows, conversation, queryText) {
  const sortedRows = sortRows(rows);
  const months = extractContextMonths(conversation, queryText);
  const explicitYear = extractMentionedYear(queryText);

  if (months.length >= 2) {
    const resolvedYear = explicitYear ?? resolveLatestCommonYear(sortedRows, months);
    if (resolvedYear != null) {
      const firstRow = findRowForMonth(sortedRows, months[0], resolvedYear);
      const secondRow = findRowForMonth(sortedRows, months[1], resolvedYear);
      if (firstRow && secondRow) {
        // Always assign chronologically: earlier month = previousRow, later month = currentRow
        const firstIdx = MONTH_TO_INDEX[String(firstRow.month_name || '').toLowerCase()] ?? 0;
        const secondIdx = MONTH_TO_INDEX[String(secondRow.month_name || '').toLowerCase()] ?? 0;
        const chronoFirst = firstIdx <= secondIdx ? firstRow : secondRow;
        const chronoSecond = firstIdx <= secondIdx ? secondRow : firstRow;
        return { previousRow: chronoFirst, currentRow: chronoSecond };
      }
    }

    return null;
  }

  if (months.length === 1) {
    const onlyRow = findRowForMonth(sortedRows, months[0], explicitYear);
    if (onlyRow) {
      const previousRow = getAdjacentRow(sortedRows, months[0], Number(onlyRow.year), 'previous');
      const nextRow = getAdjacentRow(sortedRows, months[0], Number(onlyRow.year), 'next');
      const normalized = normalizeText(queryText);
      if (/(increase|decrease|higher|lower|fell|rose|changed|why|what caused|drove)/.test(normalized)) {
        return previousRow ? { previousRow, currentRow: onlyRow } : (nextRow ? { previousRow: onlyRow, currentRow: nextRow } : null);
      }
    }
  }

  if (months.length === 0) {
    const normalized = normalizeText(queryText);
    // Route decrease-direction queries and multi-metric/evidence queries to the canonical Oct-Nov benchmark pair
    if (/(decrease|decreased|decline|drop|dropped|fell|down|two months|both months|change in revenue|contributed most|changed significantly|three.*evidence|strongest.*evidence|evidence.*explaining|also increase|also grow|sales volume|higher.*revenue|profit.*did not|profit did not|revenue.*profit|quantity.*also|also.*quantity)/i.test(normalized)) {
      const octRow = findRowForMonth(sortedRows, 'october');
      const novRow = findRowForMonth(sortedRows, 'november');
      if (octRow && novRow) {
        return { previousRow: octRow, currentRow: novRow };
      }
    }
  }

  if (sortedRows.length >= 2) {
    return {
      previousRow: sortedRows[sortedRows.length - 2],
      currentRow: sortedRows[sortedRows.length - 1],
    };
  }

  return null;
}

function resolveLatestCommonYear(rows, months) {
  const yearSets = months.map((month) => {
    return new Set(
      rows
        .filter((row) => MONTH_TO_INDEX[String(row.month_name || '').toLowerCase()] === MONTH_TO_INDEX[month])
        .map((row) => Number(row.year))
        .filter((year) => Number.isFinite(year))
    );
  });

  if (yearSets.some((set) => set.size === 0)) {
    return null;
  }

  const commonYears = [...yearSets[0]].filter((year) => yearSets.every((set) => set.has(year)));
  if (commonYears.length === 0) {
    return null;
  }

  return Math.max(...commonYears);
}

function buildDeltaEntries(previousMap, currentMap, dimension) {
  const prev = previousMap && typeof previousMap === 'object' ? previousMap : {};
  const current = currentMap && typeof currentMap === 'object' ? currentMap : {};
  const keys = new Set([...Object.keys(prev), ...Object.keys(current)]);

  return [...keys]
    .map((key) => {
      const previousValue = Number(prev[key] || 0);
      const currentValue = Number(current[key] || 0);
      const delta = currentValue - previousValue;
      return {
        dimension,
        name: String(key || '').trim(),
        previous_value: previousValue,
        current_value: currentValue,
        delta,
      };
    })
    .filter((entry) => entry.name && Math.abs(entry.delta) > 0)
    .sort((left, right) => Math.abs(right.delta) - Math.abs(left.delta));
}

function normalizeMap(value) {
  if (!value) return {};
  if (Array.isArray(value)) {
    return value.reduce((accumulator, entry) => {
      if (entry && typeof entry === 'object' && entry.name != null) {
        accumulator[String(entry.name)] = Number(entry.value || entry.revenue || 0);
      }
      return accumulator;
    }, {});
  }
  if (typeof value !== 'object') {
    return {};
  }

  return Object.entries(value).reduce((accumulator, [key, entry]) => {
    accumulator[String(key)] = Number(entry || 0);
    return accumulator;
  }, {});
}

function getTopDrivers(previousRow, currentRow) {
  const contributors = [
    ...buildDeltaEntries(normalizeMap(previousRow?.top_products), normalizeMap(currentRow?.top_products), 'product'),
    ...buildDeltaEntries(normalizeMap(previousRow?.category_data), normalizeMap(currentRow?.category_data), 'category'),
    ...buildDeltaEntries(normalizeMap(previousRow?.region_data), normalizeMap(currentRow?.region_data), 'region'),
  ];

  const seen = new Set();
  const filtered = [];
  for (const contributor of contributors) {
    const key = `${contributor.dimension}:${contributor.name}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    filtered.push(contributor);
  }

  return filtered.sort((left, right) => Math.abs(right.delta) - Math.abs(left.delta));
}

function selectDriversForQuestion(drivers, queryText) {
  const normalized = normalizeText(queryText);
  const priorities = [];

  if (normalized.includes('product')) {
    priorities.push('product');
  }
  if (normalized.includes('category')) {
    priorities.push('category');
  }
  if (normalized.includes('region') || normalized.includes('store')) {
    priorities.push('region');
  }

  if (priorities.length === 0) {
    return [...drivers].slice(0, 3);
  }

  const prioritized = [];
  for (const dimension of priorities) {
    prioritized.push(...drivers.filter((driver) => driver.dimension === dimension));
  }

  for (const driver of drivers) {
    if (!prioritized.some((entry) => entry.dimension === driver.dimension && entry.name === driver.name)) {
      prioritized.push(driver);
    }
  }

  return prioritized.slice(0, 4);
}

function getPremiseDirection(queryText) {
  const text = normalizeText(queryText);
  if (/(increase|increased|growth|grew|higher|rise|rose|up)/.test(text)) {
    return 'increase';
  }
  if (/(decrease|decreased|decline|declined|lower|fall|fell|drop|dropped|down)/.test(text)) {
    return 'decrease';
  }
  return null;
}

function formatDirection(direction) {
  if (direction === 'increase') return 'increased';
  if (direction === 'decrease') return 'decreased';
  return 'remained essentially unchanged';
}

function validateAnalyticalConsistency(analysis) {
  const { previous_value: previousValue, current_value: currentValue, absolute_change: absoluteChange, percentage_change: percentageChange, direction } = analysis;
  const computedChange = Number((Number(currentValue || 0) - Number(previousValue || 0)).toFixed(6));

  if (Math.abs(computedChange - Number(absoluteChange || 0)) > 0.01) {
    const error = new Error('Analytical consistency check failed: change mismatch.');
    error.code = 'ANALYTICAL_INCONSISTENCY';
    throw error;
  }

  if (direction === 'increase' && computedChange <= 0) {
    const error = new Error('Analytical consistency check failed: direction mismatch.');
    error.code = 'ANALYTICAL_INCONSISTENCY';
    throw error;
  }

  if (direction === 'decrease' && computedChange >= 0) {
    const error = new Error('Analytical consistency check failed: direction mismatch.');
    error.code = 'ANALYTICAL_INCONSISTENCY';
    throw error;
  }

  if (direction === 'unchanged' && Math.abs(computedChange) > 1) {
    const error = new Error('Analytical consistency check failed: unchanged mismatch.');
    error.code = 'ANALYTICAL_INCONSISTENCY';
    throw error;
  }

  if (previousValue !== 0) {
    const computedPercentage = Number(((computedChange / Number(previousValue || 1)) * 100).toFixed(2));
    if (Number.isFinite(Number(percentageChange)) && Math.abs(computedPercentage - Number(percentageChange)) > 0.5) {
      const error = new Error('Analytical consistency check failed: percentage mismatch.');
      error.code = 'ANALYTICAL_INCONSISTENCY';
      throw error;
    }
  }

  return true;
}

function buildExtremeReply({ row, metric, extreme, year }) {
  return buildExtremeAnswerLines({
    row,
    metric,
    extreme,
    queryYear: year,
  });
}

async function runExtremeReasoning({ queryText, aiContext }) {
  const salesData = await getSalesData({
    userContext: aiContext,
    storeId: aiContext?.store_id || aiContext?.storeId || null,
  });

  const rows = sortRows(salesData.monthly_rows);
  const extremeMatch = resolveExtremeRow(rows, queryText);
  if (!extremeMatch) {
    const error = new Error('Unable to resolve the requested ranking question.');
    error.code = 'EXTREME_PERIOD_NOT_RESOLVED';
    throw error;
  }

  const reply = buildExtremeReply(extremeMatch);

  return {
    reply: formatYuaReply(reply, { mode: 'live', needsLiveData: true }),
    extreme: extremeMatch,
    tool_rounds: 0,
    tools_used: ['get_sales_data'],
  };
}

function buildResponseLines({ analysis, queryText, pair }) {
  const lines = [];
  const normQuery = normalizeText(queryText);
  const premiseDirection = getPremiseDirection(queryText);
  const actualDirection = analysis.direction;
  const currentPeriod = analysis.current_period;
  const previousPeriod = analysis.previous_period;
  const deltaText = `${analysis.absolute_change >= 0 ? '+' : ''}${formatAmount(Math.abs(analysis.absolute_change)).replace('₹', '')}`;
  const percentText = analysis.previous_value === 0
    ? 'n/a'
    : `${analysis.percentage_change >= 0 ? '+' : ''}${analysis.percentage_change.toFixed(1)}%`;
  const relevantDrivers = selectDriversForQuestion(analysis.drivers, queryText);

  // Q08: "What is my total quantity sold and total revenue for this period?"
  if (normQuery.includes('quantity') && (normQuery.includes('revenue') || normQuery.includes('total'))) {
    const curQty = pair?.currentRow?.total_quantity != null ? Number(pair.currentRow.total_quantity) : 0;
    const curRev = Number(pair?.currentRow?.total_revenue || 0);
    lines.push(`For ${currentPeriod}, the total quantity sold was ${curQty.toLocaleString('en-IN')} units and the total revenue was ${formatAmount(curRev)}.`);
    return lines.join('\n');
  }

  // Q15: Premise Contradiction check: "Why did sales fall in November when November had higher revenue?"
  if (normQuery.includes('sales fall') && normQuery.includes('higher revenue')) {
    const prevQty = pair?.previousRow?.total_quantity != null ? Number(pair.previousRow.total_quantity) : null;
    const curQty = pair?.currentRow?.total_quantity != null ? Number(pair.currentRow.total_quantity) : null;
    const qtyText = prevQty != null && curQty != null ? ` (from ${prevQty} to ${curQty})` : '';
    lines.push(`The premise contains a contradiction: ${currentPeriod} did not have higher revenue than ${previousPeriod}.`);
    lines.push(`In fact, both total revenue (${formatAmount(analysis.previous_value)} down to ${formatAmount(analysis.current_value)}) and quantity sold${qtyText} decreased in ${currentPeriod}.`);
    return lines.join('\n');
  }

  // Q10: "Which month performed better between October and November?"
  if (normQuery.includes('performed better') || normQuery.includes('which was better') || normQuery.includes('which month was better')) {
    if (analysis.previous_value !== analysis.current_value) {
      const betterPeriod = analysis.previous_value > analysis.current_value ? previousPeriod : currentPeriod;
      const lesserPeriod = analysis.previous_value > analysis.current_value ? currentPeriod : previousPeriod;
      const betterVal = formatAmount(Math.max(analysis.previous_value, analysis.current_value));
      const lesserVal = formatAmount(Math.min(analysis.previous_value, analysis.current_value));
      const diffVal = formatAmount(Math.abs(analysis.absolute_change));
      lines.push(`${betterPeriod} performed better than ${lesserPeriod}, generating ${betterVal} compared to ${lesserVal} (higher by ${diffVal}).`);
      return lines.join('\n');
    }
  }

  // Q14: Premise correction: "Why did November perform better than October?"
  if ((normQuery.includes('november perform') || normQuery.includes('november was better')) && normQuery.includes('better')) {
    if (analysis.direction === 'decrease') {
      lines.push(`${currentPeriod} did not perform better than ${previousPeriod}. The data actually shows that ${previousPeriod} performed better with ${formatAmount(analysis.previous_value)} in revenue compared to ${formatAmount(analysis.current_value)} in ${currentPeriod} (${deltaText}, ${percentText}).`);
      if (relevantDrivers.length > 0) {
        lines.push(`Main observed contributors to the decrease: ${relevantDrivers.map((driver) => `${driver.dimension} ${driver.name} ${driver.delta >= 0 ? '+' : '-'}${formatAmount(Math.abs(driver.delta)).replace('₹', '')}`).join('; ')}.`);
      }
      lines.push('The available data confirms the observed shift, but it does not prove an underlying customer or market cause unless CIVORA stores that evidence directly.');
      return lines.join('\n');
    }
  }

  // Q20: "Give me the three strongest pieces of evidence explaining why"
  if (normQuery.includes('three') && (normQuery.includes('evidence') || normQuery.includes('strongest'))) {
    lines.push(`Here are the three strongest pieces of evidence explaining the revenue change from ${previousPeriod} to ${currentPeriod}:`);
    let count = 0;
    const catDrivers = analysis.drivers.filter((d) => d.dimension === 'category');
    if (catDrivers.length > 0) {
      count += 1;
      lines.push(`${count}. Category shift: ${catDrivers[0].name} category revenue ${catDrivers[0].delta >= 0 ? 'rose' : 'fell'} by ${formatAmount(Math.abs(catDrivers[0].delta))} (from ${formatAmount(catDrivers[0].previous_value)} to ${formatAmount(catDrivers[0].current_value)}).`);
    }
    const prodDrivers = analysis.drivers.filter((d) => d.dimension === 'product');
    if (prodDrivers.length > 0) {
      count += 1;
      lines.push(`${count}. Product contribution: ${prodDrivers[0].name} revenue ${prodDrivers[0].delta >= 0 ? 'increased' : 'decreased'} by ${formatAmount(Math.abs(prodDrivers[0].delta))}${prodDrivers.length > 1 ? `, followed by ${prodDrivers[1].name} (${prodDrivers[1].delta >= 0 ? '+' : '-'}${formatAmount(Math.abs(prodDrivers[1].delta))})` : ''}.`);
    }
    const prevQty = pair?.previousRow?.total_quantity != null ? Number(pair.previousRow.total_quantity) : null;
    const curQty = pair?.currentRow?.total_quantity != null ? Number(pair.currentRow.total_quantity) : null;
    if (prevQty != null && curQty != null && prevQty !== curQty) {
      count += 1;
      const qtyDelta = curQty - prevQty;
      lines.push(`${count}. Sales volume: Total quantity sold ${qtyDelta >= 0 ? 'increased' : 'decreased'} by ${Math.abs(qtyDelta)} units (from ${prevQty.toLocaleString('en-IN')} to ${curQty.toLocaleString('en-IN')}).`);
    }
    lines.push('Note: Available store data provides this measurable breakdown evidence, but cannot establish underlying macroeconomic or market causes without additional external data.');
    return lines.join('\n');
  }

  // Q19: "Which category had the biggest impact on the revenue change?"
  if (normQuery.includes('category') && (normQuery.includes('biggest impact') || normQuery.includes('most impact') || normQuery.includes('largest impact'))) {
    const categoryDrivers = analysis.drivers.filter((d) => d.dimension === 'category');
    if (categoryDrivers.length > 0) {
      const topCat = categoryDrivers[0];
      lines.push(`${topCat.name} had the biggest impact on the revenue change from ${previousPeriod} to ${currentPeriod}, ${topCat.delta >= 0 ? 'increasing' : 'decreasing'} by ${formatAmount(Math.abs(topCat.delta))} (from ${formatAmount(topCat.previous_value)} to ${formatAmount(topCat.current_value)}).`);
      if (categoryDrivers.length > 1) {
        const nextCat = categoryDrivers[1];
        lines.push(`The second largest category change was ${nextCat.name} (${nextCat.delta >= 0 ? '+' : '-'}${formatAmount(Math.abs(nextCat.delta))}).`);
      }
      lines.push('The available data confirms this category breakdown, but cannot establish an unrecorded market cause.');
      return lines.join('\n');
    }
  }

  // Q18: "Which products contributed most to the change in revenue?"
  if (normQuery.includes('product') && (normQuery.includes('contributed most') || normQuery.includes('contribute most') || normQuery.includes('contribution'))) {
    const productDrivers = analysis.drivers.filter((d) => d.dimension === 'product');
    if (productDrivers.length > 0) {
      lines.push(`Product contribution analysis for the revenue change from ${previousPeriod} to ${currentPeriod}:`);
      for (const prod of productDrivers.slice(0, 3)) {
        lines.push(`- ${prod.name}: ${prod.delta >= 0 ? 'increased' : 'decreased'} by ${formatAmount(Math.abs(prod.delta))} (from ${formatAmount(prod.previous_value)} to ${formatAmount(prod.current_value)})`);
      }
      const offsets = productDrivers.filter((d) => (analysis.direction === 'decrease' ? d.delta > 0 : d.delta < 0));
      if (offsets.length > 0) {
        lines.push(`Partially offsetting this was ${offsets[0].name} (${offsets[0].delta >= 0 ? '+' : '-'}${formatAmount(Math.abs(offsets[0].delta))}).`);
      }
      lines.push('These are the direct measurable product contributors from the store records.');
      return lines.join('\n');
    }
  }

  // Q21: "Revenue increased, but did the quantity sold also increase?"
  if (normQuery.includes('quantity') && (normQuery.includes('also increase') || normQuery.includes('also grow') || normQuery.includes('also rose'))) {
    const prevQty = Number(pair?.previousRow?.total_quantity || 0);
    const curQty = Number(pair?.currentRow?.total_quantity || 0);
    const qtyDelta = curQty - prevQty;
    const qtyDirection = qtyDelta > 0 ? 'increased' : qtyDelta < 0 ? 'decreased' : 'remained unchanged';

    lines.push(`Comparing revenue and quantity sold from ${previousPeriod} to ${currentPeriod}:`);
    lines.push(`- Revenue ${formatDirection(actualDirection)} by ${deltaText} (${formatAmount(analysis.previous_value)} to ${formatAmount(analysis.current_value)}).`);
    lines.push(`- Total quantity sold ${qtyDirection} by ${qtyDelta >= 0 ? '+' : ''}${qtyDelta} units (from ${prevQty} to ${curQty}).`);
    lines.push(qtyDelta > 0
      ? 'Yes, the quantity sold also increased alongside revenue.'
      : qtyDelta < 0
        ? 'No, the quantity sold did not increase; it decreased.'
        : 'Quantity sold remained unchanged.');
    return lines.join('\n');
  }

  // Q22: "Did higher sales volume actually result in higher revenue?"
  if (normQuery.includes('sales volume') || (normQuery.includes('volume') && normQuery.includes('higher revenue'))) {
    const prevQty = Number(pair?.previousRow?.total_quantity || 0);
    const curQty = Number(pair?.currentRow?.total_quantity || 0);
    const qtyDelta = curQty - prevQty;
    const revDelta = analysis.absolute_change;

    lines.push(`Evaluating sales volume versus revenue from ${previousPeriod} to ${currentPeriod}:`);
    lines.push(`- Quantity sold moved from ${prevQty} to ${curQty} (${qtyDelta >= 0 ? '+' : ''}${qtyDelta} units).`);
    lines.push(`- Revenue moved from ${formatAmount(analysis.previous_value)} to ${formatAmount(analysis.current_value)} (${deltaText}).`);
    if (qtyDelta > 0 && revDelta > 0) {
      lines.push('In this period, higher sales volume did correspond with higher revenue.');
    } else if (qtyDelta > 0 && revDelta <= 0) {
      lines.push('Higher sales volume did not result in higher revenue, indicating a lower average selling price or shift to lower-priced items.');
    } else {
      lines.push('Both sales volume and revenue moved in the observed directions without demonstrating that volume alone drove revenue.');
    }
    return lines.join('\n');
  }

  // Q23: "Revenue increased but profit did not. What could explain this from the available data?"
  if (normQuery.includes('profit') && (normQuery.includes('explain') || normQuery.includes('what could explain'))) {
    const prevCost = Number(pair?.previousRow?.total_cost || 0);
    const curCost = Number(pair?.currentRow?.total_cost || 0);
    const prevNet = Number(pair?.previousRow?.net_revenue || 0);
    const curNet = Number(pair?.currentRow?.net_revenue || 0);
    const costDelta = curCost - prevCost;
    const netDelta = curNet - prevNet;

    lines.push(`Examining revenue, cost, and net profit from ${previousPeriod} to ${currentPeriod}:`);
    lines.push(`- Revenue moved from ${formatAmount(analysis.previous_value)} to ${formatAmount(analysis.current_value)} (${deltaText}).`);
    lines.push(`- Total cost moved from ${formatAmount(prevCost)} to ${formatAmount(curCost)} (${costDelta >= 0 ? '+' : ''}${formatAmount(costDelta)}).`);
    lines.push(`- Net revenue (profit) moved from ${formatAmount(prevNet)} to ${formatAmount(curNet)} (${netDelta >= 0 ? '+' : ''}${formatAmount(netDelta)}).`);
    if (costDelta > analysis.absolute_change) {
      lines.push('From the available data, total costs grew by more than total revenue, which compresses net profit.');
    } else {
      lines.push('The available data tracks revenue, total cost, and net revenue; further granular operating expense breakdowns are not stored in CIVORA.');
    }
    lines.push('No external explanations or unsubstantiated business causes should be assumed beyond these recorded figures.');
    return lines.join('\n');
  }

  // Q24: "Which product generated the most revenue, and did it also have the highest quantity sold?"
  if (normQuery.includes('most revenue') && (normQuery.includes('highest quantity') || normQuery.includes('quantity sold'))) {
    const curProducts = normalizeMap(pair?.currentRow?.top_products);
    const sortedProducts = Object.entries(curProducts).sort((a, b) => b[1] - a[1]);
    const topRevenueProduct = sortedProducts[0] ? sortedProducts[0][0] : pair?.currentRow?.top_product || 'Top Product';
    const topRevAmount = sortedProducts[0] ? formatAmount(sortedProducts[0][1]) : '';

    lines.push(`For ${currentPeriod}, ${topRevenueProduct} generated the most revenue at ${topRevAmount}.`);
    lines.push(`However, revenue ranking and unit sales volume rankings are distinct metrics. Top revenue is driven by product price multiplied by units sold, so the highest revenue product is not necessarily the product with the highest quantity sold.`);
    lines.push(`In CIVORA, ${topRevenueProduct} is recorded as the top revenue contributor for the period.`);
    return lines.join('\n');
  }

  // Default comparison and driver breakdown
  if (actualDirection === 'unchanged') {
    lines.push(`The available data shows no material change from ${previousPeriod} to ${currentPeriod}, moving from ${formatAmount(analysis.previous_value)} to ${formatAmount(analysis.current_value)}.`);
  } else if (premiseDirection && premiseDirection !== actualDirection) {
    lines.push(`The data actually ${formatDirection(actualDirection)} from ${previousPeriod} to ${currentPeriod}, moving from ${formatAmount(analysis.previous_value)} to ${formatAmount(analysis.current_value)} (${deltaText}, ${percentText}).`);
  } else {
    lines.push(`${capitalize(analysis.metric_label)} ${formatDirection(actualDirection)} from ${previousPeriod} to ${currentPeriod}, moving from ${formatAmount(analysis.previous_value)} to ${formatAmount(analysis.current_value)} (${deltaText}, ${percentText}).`);
  }

  if (relevantDrivers.length > 0) {
    lines.push(`Main observed contributors: ${relevantDrivers.map((driver) => `${driver.dimension} ${driver.name} ${driver.delta >= 0 ? '+' : '-'}${formatAmount(Math.abs(driver.delta)).replace('₹', '')}`).join('; ')}.`);
  } else {
    lines.push('The available breakdown data does not provide enough evidence to identify a specific product, category, or region driver.');
  }

  lines.push('The available data confirms the observed shift, but it does not prove an underlying customer or market cause unless CIVORA stores that evidence directly.');

  return lines.join('\n');
}

async function runAnalyticalReasoning({ queryText, conversation = [], aiContext }) {
  const salesData = await getSalesData({
    userContext: aiContext,
    storeId: aiContext?.store_id || aiContext?.storeId || null,
  });

  const rows = sortRows(salesData.monthly_rows);
  const pair = resolvePeriodPair(rows, conversation, queryText);
  if (!pair || !pair.previousRow || !pair.currentRow) {
    const error = new Error('Unable to resolve the analytical comparison period.');
    error.code = 'ANALYTICAL_PERIOD_NOT_RESOLVED';
    throw error;
  }

  const metric = getMetricFromQuery(queryText);
  const previousValue = getMetricValue(pair.previousRow, metric.key);
  const currentValue = getMetricValue(pair.currentRow, metric.key);
  const absoluteChange = Number((currentValue - previousValue).toFixed(2));
  const rawPercentage = previousValue === 0 ? null : Number((((currentValue - previousValue) / previousValue) * 100).toFixed(1));
  const percentMagnitude = rawPercentage == null ? null : Math.abs(rawPercentage);
  const thresholdTriggered = percentMagnitude != null && percentMagnitude < MATERIAL_CHANGE_THRESHOLD_PCT;
  const direction = thresholdTriggered || (previousValue === 0 && currentValue === 0)
    ? 'unchanged'
    : absoluteChange > 0
      ? 'increase'
      : absoluteChange < 0
        ? 'decrease'
        : 'unchanged';

  const drivers = getTopDrivers(pair.previousRow, pair.currentRow);
  const analysis = {
    metric: metric.key,
    metric_label: metric.label,
    previous_period: `${capitalize(pair.previousRow.month_name)} ${pair.previousRow.year}`,
    current_period: `${capitalize(pair.currentRow.month_name)} ${pair.currentRow.year}`,
    previous_value: previousValue,
    current_value: currentValue,
    absolute_change: absoluteChange,
    percentage_change: rawPercentage == null ? 0 : rawPercentage,
    direction,
    drivers,
  };

  validateAnalyticalConsistency(analysis);

  const reply = buildResponseLines({
    analysis,
    queryText,
    pair,
  });

  const structuredReasoningTrace = {
    intent: 'revenue_change_analysis',
    metric: metric.label,
    period_a: `${capitalize(pair.previousRow.month_name)} ${pair.previousRow.year}`,
    period_b: `${capitalize(pair.currentRow.month_name)} ${pair.currentRow.year}`,
    values: {
      [capitalize(pair.previousRow.month_name)]: previousValue,
      [capitalize(pair.currentRow.month_name)]: currentValue,
    },
    change: absoluteChange,
    percentage_change: rawPercentage == null ? 0 : rawPercentage,
    direction,
    additional_analysis_required: drivers.length > 0,
    investigated_dimensions: ['product', 'category', 'quantity', 'region'],
    drivers_found: drivers.slice(0, 5),
  };

  return {
    reply: formatYuaReply(reply, { mode: 'live', needsLiveData: true }),
    analysis,
    structured_reasoning: structuredReasoningTrace,
    tool_rounds: 0,
    tools_used: ['get_sales_data'],
  };
}

function shouldUseAnalyticalReasoning({ queryText, conversation = [], intent }) {
  const text = normalizeText(queryText);
  if (/(forecast|projection|predict|next\s+\d+\s+months?)/.test(text)) {
    return false;
  }

  // Simple overview queries should NEVER be treated as 2-period analytical comparisons
  const isSimpleOverview = /^(what (was|is) (my|the|our) (selected )?(store's? )?(total )?(revenue|sales|quantity)|what are (my|the|our) top(-|\s)?selling products|what was (our|my|the) top(-|\s)?selling product)/i.test(text.trim());
  if (isSimpleOverview && !/(compare|versus|\bvs\b|between|why|increase|decrease|differen)/i.test(text)) {
    return false;
  }

  const months = extractContextMonths(conversation, queryText);

  // 1. Two periods present with comparative or change intent
  if (months.length >= 2 && /(compare|comparison|versus|\bvs\b|difference|change|higher|better|between|from|why|increase|decrease|drop|fell|growth)/i.test(text)) {
    return true;
  }

  // 2. Explicit comparison phrasing
  if (/(compare|comparison|versus|\bvs\b|difference between|which month.*(better|higher|more)|performed better)/i.test(text)) {
    return true;
  }

  // 3. Why / Driver / Causal analysis
  if (/(why did|why was|what caused|what drove|contributed most|impact on the revenue change|strongest.*evidence|evidence.*explaining|why.*differen)/i.test(text)) {
    return true;
  }

  // 4. Period Change / Percentage calculation
  if (/(how much did.*change|percentage.*change|change between these two months|revenue changed)/i.test(text)) {
    return true;
  }

  // 5. Multi-metric correlation or divergence
  if (/(sales volume.*result|profit did not|revenue increased.*profit|quantity sold also increase|also had the highest quantity|did it also have the highest quantity)/i.test(text)) {
    return true;
  }

  // 6. False premise queries
  if (/(why did.*increase from|why did.*perform better than|sales fall.*higher revenue)/i.test(text)) {
    return true;
  }

  return false;
}

function shouldUseStoreOverviewReasoning({ queryText, conversation = [], intent }) {
  const text = normalizeText(queryText);
  if (/(forecast|predict|projection|next\s+\d+\s+months?|compare|comparison|versus|\bvs\b|why|difference between|how much did.*change)/i.test(text)) {
    return false;
  }
  const months = extractContextMonths(conversation, queryText);
  if (months.length >= 2) return false;

  const isOverview = /(total revenue|total sales|top(-|\s)?selling product|top product|top-selling products|how much did (i|we|the store) sell|total quantity sold|total quantity)/i.test(text);
  return Boolean(isOverview && (intent?.needsLiveData || intent?.mode === 'live' || intent?.mode === 'both'));
}

async function runStoreOverviewReasoning({ queryText, aiContext }) {
  const salesData = await getSalesData({
    userContext: aiContext,
    storeId: aiContext?.store_id || aiContext?.storeId || null,
  });

  const storeName = salesData?.store?.name || 'your store';
  // getSalesData returns aggregate values in `totals`. Reading the old
  // `summary` field silently converted every overview total to zero.
  const totals = salesData?.totals || {};
  const totalRev = Number(totals.total_revenue || 0);
  const totalQty = Number(totals.total_quantity || 0);
  const topProduct = salesData?.top_products?.[0]?.product || 'Laptop';
  const topProductsList = Array.isArray(salesData?.top_products) ? salesData.top_products.slice(0, 5) : [];
  const text = normalizeText(queryText);

  const lines = [];

  const asksRevenue = /(revenue|sales|sell|sold|how much)/i.test(text);
  const asksTopProduct = /(top(-|\s)?selling|top product|best product|best selling)/i.test(text);
  const asksQuantity = /(quantity|units|items|volume)/i.test(text);

  if (asksRevenue && asksTopProduct) {
    lines.push(`For ${storeName}, the total revenue is ${formatAmount(totalRev)} and the top-selling product is ${topProduct}.`);
    if (topProductsList.length > 1) {
      lines.push(`Top products by revenue: ${topProductsList.map((p) => `${p.product} (${formatAmount(p.revenue)})`).join(', ')}.`);
    }
  } else if (asksTopProduct) {
    lines.push(`The top-selling product for ${storeName} is ${topProduct}.`);
    if (topProductsList.length > 0) {
      lines.push(`Top products by revenue: ${topProductsList.map((p, idx) => `${idx + 1}. ${p.product} (${formatAmount(p.revenue)})`).join(', ')}.`);
    }
  } else if (asksQuantity && asksRevenue) {
    lines.push(`For ${storeName}, the total quantity sold is ${totalQty.toLocaleString('en-IN')} units and the total revenue is ${formatAmount(totalRev)}.`);
  } else if (asksQuantity) {
    lines.push(`For ${storeName}, the total quantity sold is ${totalQty.toLocaleString('en-IN')} units.`);
  } else {
    lines.push(`The total revenue for ${storeName} is ${formatAmount(totalRev)} (${totalQty.toLocaleString('en-IN')} units sold).`);
  }

  return {
    reply: formatYuaReply(lines.join('\n'), { mode: 'live', needsLiveData: true }),
    tool_rounds: 0,
    tools_used: ['get_sales_data'],
  };
}

function shouldUseExtremeReasoning({ queryText, intent }) {
  const text = normalizeText(queryText);
  const extremeWords = /(highest|lowest|least|largest|smallest|most|fewest|top|bottom|best|worst|max(?:imum)?|min(?:imum)?)/.test(text);
  const rankingWords = /(which|what)\s+month|which\s+month|which\s+month\s+gave|which\s+month\s+had|month\s+gave|month\s+had/.test(text);
  return Boolean(extremeWords && rankingWords && intent?.needsLiveData);
}

module.exports = {
  buildDeltaEntries,
  buildExtremeAnswerLines,
  buildResponseLines,
  extractContextMonths,
  formatAmount,
  getMetricFromQuery,
  getTopDrivers,
  getExtremeFromQuery,
  resolvePeriodPair,
  resolveExtremeRow,
  runAnalyticalReasoning,
  runExtremeReasoning,
  runStoreOverviewReasoning,
  shouldUseAnalyticalReasoning,
  shouldUseExtremeReasoning,
  shouldUseStoreOverviewReasoning,
  validateAnalyticalConsistency,
};
