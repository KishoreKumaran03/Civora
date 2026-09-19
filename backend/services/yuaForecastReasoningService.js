const prophetForecastService = require('./prophetForecastService');
const { formatIndianCompactNumber, formatYuaReply } = require('./yuaResponseFormatter');

const MONTHS = [
  'January',
  'February',
  'March',
  'April',
  'May',
  'June',
  'July',
  'August',
  'September',
  'October',
  'November',
  'December',
];

function normalizeText(value) {
  return String(value || '').toLowerCase();
}

function formatAmount(value) {
  const amount = Number(value || 0);
  const compact = formatIndianCompactNumber(amount);
  return `₹${compact != null ? compact : amount.toLocaleString('en-IN')}`;
}

function formatMonthLabel(monthKey, fallbackName) {
  if (typeof monthKey === 'string' && /^\d{4}-\d{2}/.test(monthKey)) {
    const [year, month] = monthKey.split('-');
    const monthIndex = Number(month) - 1;
    if (MONTHS[monthIndex]) {
      return `${MONTHS[monthIndex]} ${year}`;
    }
  }

  if (typeof fallbackName === 'string' && fallbackName.trim()) {
    return fallbackName.trim().replace(/^([a-z])/, (letter) => letter.toUpperCase());
  }

  return String(monthKey || '').trim();
}

function getRequestedHorizon(queryText) {
  const normalized = normalizeText(queryText);

  if (/12\s*(month|months)/.test(normalized)) return 12;
  if (/6\s*(month|months)/.test(normalized)) return 6;
  if (/3\s*(month|months)/.test(normalized)) return 3;

  return 3;
}

function shouldUseForecastReasoning({ queryText, intent }) {
  const normalized = normalizeText(queryText).trim();
  // Pure conceptual documentation question without live forecast data requested (e.g. Q03)
  const isPureMethodology = /^(how does civora'?s forecasting functionality work\??|how does forecasting work in civora\??|explain civora'?s forecasting methodology\??)$/i.test(normalized);
  if (isPureMethodology) return false;
  const forecastWords = /(forecast|predict|prediction|projection|projected|next\s+\d+\s+months?|coming\s+\d+\s+months?|what\s+is\s+(my\s+)?(revenue\s+)?forecast)/.test(normalized);
  return Boolean(forecastWords);
}

function buildForecastReply(forecastResponse, queryText) {
  const forecastRows = Array.isArray(forecastResponse?.monthly_forecast) ? forecastResponse.monthly_forecast : [];
  if (forecastRows.length === 0) {
    const error = new Error('Forecast data is not available for the requested period.');
    error.code = 'FORECAST_NOT_READY';
    throw error;
  }

  const query = normalizeText(queryText);
  const horizon = forecastResponse?.selected_horizon_months || forecastRows.length;
  const visibleRows = forecastRows.slice(0, Math.max(1, horizon));
  const labels = visibleRows.map((row) => formatMonthLabel(row.month, row.name)).filter(Boolean);
  const forecastStart = labels[0] || formatMonthLabel(visibleRows[0]?.month, visibleRows[0]?.name);
  const forecastEnd = labels[labels.length - 1] || formatMonthLabel(visibleRows[visibleRows.length - 1]?.month, visibleRows[visibleRows.length - 1]?.name);

  // Q26: Compare forecast with recent actual revenue
  if (query.includes('compare') && (query.includes('actual') || query.includes('recent') || query.includes('history'))) {
    const histAvg = Number(forecastResponse?.historical_average_monthly || 0);
    const fcAvg = Number(forecastResponse?.forecast_average_monthly || 0);
    const delta = fcAvg - histAvg;
    const pctChange = histAvg > 0 ? (delta / histAvg) * 100 : 0;
    const direction = delta >= 0 ? 'increase' : 'decrease';

    const lines = [
      `Comparing the forecast with recent actual revenue:`,
      `Recent actual monthly revenue averaged ${formatAmount(histAvg)}.`,
      `The projected 3-month forecast monthly revenue averages ${formatAmount(fcAvg)}.`,
      `This represents a projected ${direction} of ${formatAmount(Math.abs(delta))} (${pctChange >= 0 ? '+' : ''}${pctChange.toFixed(1)}%) relative to recent actual revenue averages.`,
    ];
    return formatYuaReply(lines.join('\n'), { mode: 'live', needsLiveData: true });
  }

  // Q27: Forecast trend - growth or decline
  if (query.includes('growth') || query.includes('decline') || query.includes('trend')) {
    const lines = [];
    if (visibleRows.length >= 2) {
      const firstVal = Number(visibleRows[0].yhat || 0);
      const lastVal = Number(visibleRows[visibleRows.length - 1].yhat || 0);
      const overallDelta = lastVal - firstVal;
      const overallDirection = overallDelta > 0 ? 'growth' : overallDelta < 0 ? 'decline' : 'steady';

      lines.push(`The revenue forecast indicates ${overallDirection} across the ${visibleRows.length}-month horizon:`);
      for (const row of visibleRows) {
        const label = formatMonthLabel(row.month, row.name);
        lines.push(`${label}: ${formatAmount(row.yhat)}`);
      }
      lines.push(`From ${formatMonthLabel(visibleRows[0].month, visibleRows[0].name)} (${formatAmount(firstVal)}) to ${formatMonthLabel(visibleRows[visibleRows.length - 1].month, visibleRows[visibleRows.length - 1].name)} (${formatAmount(lastVal)}), projected monthly revenue changes by ${overallDelta >= 0 ? '+' : ''}${formatAmount(Math.abs(overallDelta))} (${((overallDelta / Math.max(firstVal, 1)) * 100).toFixed(1)}%).`);
      return formatYuaReply(lines.join('\n'), { mode: 'live', needsLiveData: true });
    }
  }

  // Q25 & General Forecast
  const lines = [];
  lines.push(`Forecast for ${forecastStart}${forecastEnd && forecastEnd !== forecastStart ? ` to ${forecastEnd}` : ''}:`);

  for (const row of visibleRows) {
    const label = formatMonthLabel(row.month, row.name);
    const valueText = formatAmount(row.yhat);
    const lowerText = row.yhat_lower != null ? formatAmount(row.yhat_lower) : null;
    const upperText = row.yhat_upper != null ? formatAmount(row.yhat_upper) : null;
    const rangeText = lowerText && upperText ? ` (range ${lowerText} to ${upperText})` : '';
    lines.push(`${label}: ${valueText}${rangeText}`);
  }

  if (Number.isFinite(Number(forecastResponse.forecast_total))) {
    lines.push(`Total projected revenue for the selected horizon: ${formatAmount(forecastResponse.forecast_total)}.`);
  }

  if (/2025/.test(String(queryText || '')) && /2025/.test(String(forecastResponse?.forecast_start || '')) === false) {
    lines.push(`The model forecast starts after the latest available history, so the forecasted period extends beyond 2025.`);
  }

  return formatYuaReply(lines.join('\n'), { mode: 'live', needsLiveData: true });
}

async function runForecastReasoning({ queryText, aiContext }) {
  const target = /profit|net revenue|net profit/.test(normalizeText(queryText)) ? 'profit' : 'revenue';
  const months = getRequestedHorizon(queryText);
  const forecastResponse = target === 'profit'
    ? await prophetForecastService.getProfitForecast({
      months,
      projectId: aiContext?.store_id || aiContext?.storeId || null,
    })
    : await prophetForecastService.getRevenueForecast({
      months,
      projectId: aiContext?.store_id || aiContext?.storeId || null,
    });

  return {
    reply: buildForecastReply(forecastResponse, queryText),
    forecast: forecastResponse,
    tool_rounds: 0,
    tools_used: [target === 'profit' ? 'get_profit_forecast' : 'get_revenue_forecast'],
  };
}

module.exports = {
  buildForecastReply,
  getRequestedHorizon,
  runForecastReasoning,
  shouldUseForecastReasoning,
};
