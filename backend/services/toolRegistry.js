const { getSalesData } = require('./civoraAiService');
const { getRevenueForecast } = require('./prophetForecastService');
const { validateToolResponse } = require('./yuaValidation');

const INTEGER_SCHEMA = { type: 'integer', minimum: 1 };
const DATE_SCHEMA = { type: 'string', pattern: '^\\d{4}-\\d{2}-\\d{2}$' };

function invalidArguments(message) {
  const error = new Error(message);
  error.code = 'INVALID_TOOL_ARGUMENTS';
  return error;
}

function optionalDate(value, fieldName) {
  if (value == null || value === '') return null;
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(trimmed)) {
    return trimmed.slice(0, 10);
  }
  if (/^\d{4}-\d{2}$/.test(trimmed)) {
    return fieldName === 'end_date' ? `${trimmed}-31` : `${trimmed}-01`;
  }
  return null;
}

function optionalStoreId(value) {
  if (value == null || value === '') return null;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    return null;
  }
  return parsed;
}

function normalizeToolArguments(toolCall) {
  try {
    const rawArguments = toolCall.function?.arguments;
    if (rawArguments && typeof rawArguments === 'object') {
      if (Array.isArray(rawArguments)) {
        throw invalidArguments('Tool arguments must be a JSON object.');
      }
      return rawArguments;
    }

    const parsed = JSON.parse(rawArguments || '{}');
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw invalidArguments('Tool arguments must be a JSON object.');
    }
    return parsed;
  } catch (error) {
    if (error.code === 'INVALID_TOOL_ARGUMENTS') throw error;
    throw invalidArguments('Tool arguments must contain valid JSON.');
  }
}

const toolDefinitions = [
  {
    type: 'function',
    function: {
      name: 'get_sales_data',
      description: 'Retrieve sales, revenue, cost, profit, quantity, monthly rows, and top products for an authorized CIVORA store.',
      parameters: {
        type: 'object',
        properties: {
          store_id: INTEGER_SCHEMA,
          start_date: DATE_SCHEMA,
          end_date: DATE_SCHEMA,
        },
        additionalProperties: false,
      },
    },
  },
  {
    type: 'function',
    function: {
      name: 'get_revenue_forecast',
      description: 'Retrieve the Prophet revenue forecast for an authorized CIVORA store or the global CIVORA dataset.',
      parameters: {
        type: 'object',
        properties: {
          store_id: INTEGER_SCHEMA,
          months: { type: 'integer', enum: [3, 6, 12] },
        },
        additionalProperties: false,
      },
    },
  },
];

const toolHandlers = {
  async get_sales_data({ arguments: rawArguments, userContext }) {
    const argumentsObject = normalizeToolArguments({ function: { arguments: rawArguments } });
    const storeId = optionalStoreId(argumentsObject.store_id);
    const startDate = optionalDate(argumentsObject.start_date, 'start_date');
    const endDate = optionalDate(argumentsObject.end_date, 'end_date');

    return getSalesData({
      userContext,
      storeId,
      startDate,
      endDate,
    });
  },

  async get_revenue_forecast({ arguments: rawArguments, userContext }) {
    const argumentsObject = normalizeToolArguments({ function: { arguments: rawArguments } });
    const storeId = optionalStoreId(argumentsObject.store_id);
    const months = argumentsObject.months == null ? 3 : Number(argumentsObject.months);
    if (![3, 6, 12].includes(months)) {
      throw invalidArguments('months must be one of 3, 6, or 12.');
    }

    const contextStoreId = optionalStoreId(userContext?.store_id);
    if (storeId != null && contextStoreId != null && storeId !== contextStoreId) {
      const error = new Error('You do not have access to that store.');
      error.code = 'STORE_ACCESS_DENIED';
      throw error;
    }

    return getRevenueForecast({
      months,
      projectId: storeId ?? contextStoreId,
    });
  },
};

async function executeToolCall(toolCall, { userContext }) {
  const toolName = toolCall?.function?.name;
  const handler = toolHandlers[toolName];
  if (!handler) {
    const error = new Error('The requested CIVORA tool is not available.');
    error.code = 'UNKNOWN_TOOL';
    throw error;
  }

  const result = await handler({
    arguments: toolCall.function.arguments,
    userContext,
  });

  return validateToolResponse(toolName, result);
}

module.exports = {
  executeToolCall,
  toolDefinitions,
};
