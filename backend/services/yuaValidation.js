const TEMPLATE_ARTIFACT_PATTERN = /\{\{|\}\}|<%|%>/;
const NON_FINITE_TEXT_PATTERN = /^(?:nan|infinity|-infinity)$/i;
const NUMERIC_TEXT_PATTERN = /^[+-]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?$/;

function invalidToolResponse(message) {
  const error = new Error(message);
  error.code = 'INVALID_TOOL_RESPONSE';
  return error;
}

function ensureFiniteNumber(value, path) {
  if (!Number.isFinite(value)) {
    throw invalidToolResponse(`Tool response contains an invalid number at ${path}.`);
  }
  return value;
}

function normalizeNumericText(text) {
  const cleaned = String(text || '').trim();
  if (!cleaned || NON_FINITE_TEXT_PATTERN.test(cleaned)) {
    return cleaned;
  }

  if (!NUMERIC_TEXT_PATTERN.test(cleaned)) {
    return cleaned;
  }

  const normalized = Number(cleaned.replace(/,/g, ''));
  return Number.isFinite(normalized) ? normalized : cleaned;
}

function normalizeToolValue(value, path = 'tool_response') {
  if (value == null) {
    return value;
  }

  if (typeof value === 'number') {
    return ensureFiniteNumber(value, path);
  }

  if (typeof value === 'string') {
    const text = value.trim();
    if (!text) {
      return text;
    }

    if (TEMPLATE_ARTIFACT_PATTERN.test(text)) {
      throw invalidToolResponse(`Tool response contains template artifacts at ${path}.`);
    }

    if (NON_FINITE_TEXT_PATTERN.test(text)) {
      throw invalidToolResponse(`Tool response contains a non-finite value at ${path}.`);
    }

    return normalizeNumericText(text);
  }

  if (Array.isArray(value)) {
    return value.map((entry, index) => normalizeToolValue(entry, `${path}[${index}]`));
  }

  if (typeof value === 'object') {
    const normalized = {};
    for (const [key, entry] of Object.entries(value)) {
      normalized[key] = normalizeToolValue(entry, `${path}.${key}`);
    }
    return normalized;
  }

  throw invalidToolResponse(`Tool response contains an unsupported value at ${path}.`);
}

function assertObjectShape(value, path) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw invalidToolResponse(`${path} must be a JSON object.`);
  }
}

function assertRequiredKeys(value, path, keys) {
  for (const key of keys) {
    if (!(key in value)) {
      throw invalidToolResponse(`${path}.${key} is required.`);
    }
  }
}

function validateSalesToolResponse(result) {
  const normalized = normalizeToolValue(result);
  assertObjectShape(normalized, 'sales_tool_response');
  assertRequiredKeys(normalized, 'sales_tool_response', ['store', 'totals', 'monthly_rows', 'top_products']);
  assertObjectShape(normalized.store, 'sales_tool_response.store');
  assertObjectShape(normalized.totals, 'sales_tool_response.totals');

  const requiredNumericFields = [
    ['sales_tool_response.store.id', normalized.store.id],
    ['sales_tool_response.totals.total_revenue', normalized.totals.total_revenue],
    ['sales_tool_response.totals.total_cost', normalized.totals.total_cost],
    ['sales_tool_response.totals.net_revenue', normalized.totals.net_revenue],
    ['sales_tool_response.totals.total_quantity', normalized.totals.total_quantity],
    ['sales_tool_response.totals.month_count', normalized.totals.month_count],
  ];

  for (const [path, value] of requiredNumericFields) {
    if (typeof value !== 'number' || !Number.isFinite(value)) {
      throw invalidToolResponse(`${path} must be a finite number.`);
    }
  }

  if (!Array.isArray(normalized.monthly_rows)) {
    throw invalidToolResponse('sales_tool_response.monthly_rows must be an array.');
  }

  if (!Array.isArray(normalized.top_products)) {
    throw invalidToolResponse('sales_tool_response.top_products must be an array.');
  }

  return normalized;
}

function validateForecastToolResponse(result) {
  const normalized = normalizeToolValue(result);
  assertObjectShape(normalized, 'forecast_tool_response');
  assertRequiredKeys(normalized, 'forecast_tool_response', [
    'forecast_total',
    'forecast_average_daily',
    'forecast_average_monthly',
    'historical_total',
    'historical_average_monthly',
    'monthly_forecast',
    'daily_forecast',
    'combined_trend',
  ]);

  const requiredNumericFields = [
    ['forecast_tool_response.forecast_total', normalized.forecast_total],
    ['forecast_tool_response.forecast_average_daily', normalized.forecast_average_daily],
    ['forecast_tool_response.forecast_average_monthly', normalized.forecast_average_monthly],
    ['forecast_tool_response.historical_total', normalized.historical_total],
    ['forecast_tool_response.historical_average_monthly', normalized.historical_average_monthly],
  ];

  for (const [path, value] of requiredNumericFields) {
    if (typeof value !== 'number' || !Number.isFinite(value)) {
      throw invalidToolResponse(`${path} must be a finite number.`);
    }
  }

  if (!Array.isArray(normalized.monthly_forecast)) {
    throw invalidToolResponse('forecast_tool_response.monthly_forecast must be an array.');
  }

  if (!Array.isArray(normalized.daily_forecast)) {
    throw invalidToolResponse('forecast_tool_response.daily_forecast must be an array.');
  }

  if (!Array.isArray(normalized.combined_trend)) {
    throw invalidToolResponse('forecast_tool_response.combined_trend must be an array.');
  }

  return normalized;
}

function validateToolResponse(toolName, result) {
  if (toolName === 'get_sales_data') {
    return validateSalesToolResponse(result);
  }

  if (toolName === 'get_revenue_forecast') {
    return validateForecastToolResponse(result);
  }

  return normalizeToolValue(result);
}

function sanitizeAssistantText(text) {
  return String(text || '')
    .replace(/\r\n/g, '\n')
    .replace(/\{\{[^}]*\}\}/g, ' ')
    .replace(/<%[^%]*%>/g, ' ')
    .replace(/\b(?:NaN|Infinity|-Infinity)\b/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

module.exports = {
  invalidToolResponse,
  normalizeToolValue,
  sanitizeAssistantText,
  validateToolResponse,
};
