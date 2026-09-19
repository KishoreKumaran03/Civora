const assert = require('assert');

process.env.DB_HOST = process.env.DB_HOST || 'localhost';
process.env.DB_USER = process.env.DB_USER || 'user';
process.env.DB_PASSWORD = process.env.DB_PASSWORD || 'password';
process.env.DB_NAME = process.env.DB_NAME || 'datavis_db';
process.env.JWT_SECRET = process.env.JWT_SECRET || 'test-jwt-secret';
process.env.LLM_PROVIDER = process.env.LLM_PROVIDER || 'ollama';
process.env.OLLAMA_BASE_URL = process.env.OLLAMA_BASE_URL || 'http://localhost:11434';
process.env.OLLAMA_MODEL = 'qwen3:8b';

const dbModule = require('../db');
const prophetForecastService = require('../services/prophetForecastService');
dbModule.pool.execute = async (sql, params = []) => {
  const normalizedSql = sql.toLowerCase();
  if (normalizedSql.includes('from projects where id = ? and user_id = ?')) {
    return [Number(params[0]) === 1 && Number(params[1]) === 101
      ? [{ id: 1, user_id: 101, name: 'Authorized Store', currency_code: 'INR', timezone: 'Asia/Kolkata' }]
      : []];
  }
  if (normalizedSql.includes('from projects where id = ?')) {
    return [Number(params[0]) === 2 ? [{ id: 2 }] : []];
  }
  if (normalizedSql.includes('from sales_summaries')) return [[{
    id: 1,
    project_id: 1,
    month_name: 'June',
    year: 2024,
    total_revenue: '1980000',
    total_cost: '1410000',
    net_revenue: '570000',
    total_quantity: 120,
    top_product: 'Laptop',
    top_region: 'Andhra Pradesh',
    top_products: JSON.stringify({ Laptop: 580000, TV: 500000 }),
    category_data: JSON.stringify({ Electronics: 1500000, Fashion: 300000 }),
    region_data: JSON.stringify({ 'Andhra Pradesh': 800000, Karnataka: 700000 }),
  }, {
    id: 2,
    project_id: 1,
    month_name: 'July',
    year: 2024,
    total_revenue: '1760000',
    total_cost: '1330000',
    net_revenue: '430000',
    total_quantity: 110,
    top_product: 'Laptop',
    top_region: 'Andhra Pradesh',
    top_products: JSON.stringify({ Laptop: 520000, TV: 450000 }),
    category_data: JSON.stringify({ Electronics: 1510000, Fashion: 150000 }),
    region_data: JSON.stringify({ 'Andhra Pradesh': 650000, Karnataka: 600000 }),
  }, {
    id: 3,
    project_id: 1,
    month_name: 'January',
    year: 2025,
    total_revenue: '1000',
    total_cost: '600',
    net_revenue: '400',
    total_quantity: 10,
    top_products: JSON.stringify({ Widget: 1000 }),
  }]];
  return [[]];
};

async function run() {
  const { runCivoraAgent } = require('../services/civoraAgentService');
  const { formatYuaReply } = require('../services/yuaResponseFormatter');
  const { sanitizeAssistantText, validateToolResponse } = require('../services/yuaValidation');
  let calls = 0;
  let scenario = 'live';
  const originalFetch = global.fetch;
  global.fetch = async (url, options = {}) => {
    calls += 1;
    if (String(url).includes('/api/tags')) {
      return {
        ok: true,
        json: async () => ({
          models: [
            {
              name: 'qwen3:8b',
            },
          ],
        }),
      };
    }

    const request = JSON.parse(options.body);
    assert.strictEqual(request.model, 'qwen3:8b');
    const hasKnowledgeContext = request.messages.some(
      (message) => String(message?.content || '').includes('Relevant CIVORA documentation:')
    );

    if (scenario === 'live') {
      if (calls === 2 && request.tools) {
        return {
          ok: true,
          json: async () => ({
            message: {
              role: 'assistant',
              tool_calls: [{ id: 'call-1', type: 'function', function: { name: 'get_sales_data', arguments: { store_id: 1 } } }],
            },
          }),
        };
      }
    } else if (scenario === 'doc') {
      assert.strictEqual(Boolean(request.tools && request.tools.length > 0), false);
      assert.strictEqual(hasKnowledgeContext, true);
      return {
        ok: true,
        json: async () => ({
          message: {
            role: 'assistant',
            content: 'CIVORA is a business intelligence platform that helps users analyze sales, dashboards, and forecasts.',
          },
        }),
      };
    }

    if (calls === 2 && request.tools) {
      return {
        ok: true,
        json: async () => ({
          message: {
            role: 'assistant',
            tool_calls: [{ id: 'call-1', type: 'function', function: { name: 'get_sales_data', arguments: { store_id: 1 } } }],
          },
        }),
      };
    }
    return {
      ok: true,
      json: async () => ({
        message: {
          role: 'assistant',
          content: 'Your authorized store recorded INR 1,000 in revenue.',
        },
      }),
    };
  };

  try {
    const originalRevenueForecast = prophetForecastService.getRevenueForecast;

    const result = await runCivoraAgent({
      messages: [{ role: 'user', content: 'Show my sales.' }],
      aiContext: { user_id: 101, store_id: 1, role: 'Administrator' },
      selectionRequired: false,
      availableStoreCount: 1,
    });
    assert.match(result.reply, /INR 1,000/);
    assert.deepStrictEqual(result.tools_used, ['get_sales_data']);

    scenario = 'doc';
    const docResult = await runCivoraAgent({
      messages: [{ role: 'user', content: 'What is CIVORA?' }],
      aiContext: { user_id: 101, store_id: 1, role: 'Administrator' },
      selectionRequired: false,
      availableStoreCount: 1,
    });
    assert.match(docResult.reply, /business intelligence platform/i);
    assert.deepStrictEqual(docResult.tools_used, []);
    assert.strictEqual(docResult.knowledge_used, true);

    scenario = 'live';
    calls = 0;
    const highestMonthResult = await runCivoraAgent({
      messages: [{ role: 'user', content: 'Which month gave the highest revenue in 2024?' }],
      aiContext: { user_id: 101, store_id: 1, role: 'Administrator' },
      selectionRequired: false,
      availableStoreCount: 1,
    });
    assert.match(highestMonthResult.reply, /June 2024/i);
    assert.match(highestMonthResult.reply, /highest revenue/i);
    assert.strictEqual(calls, 0);

    prophetForecastService.getRevenueForecast = async ({ months }) => ({
      selected_horizon_months: months,
      monthly_forecast: [
        { month: '2025-01', name: 'Jan 25', yhat: 2100000, yhat_lower: 1900000, yhat_upper: 2300000 },
        { month: '2025-02', name: 'Feb 25', yhat: 2250000, yhat_lower: 2000000, yhat_upper: 2500000 },
        { month: '2025-03', name: 'Mar 25', yhat: 2400000, yhat_lower: 2150000, yhat_upper: 2650000 },
      ],
      daily_forecast: [],
      combined_trend: [],
      forecast_total: 6750000,
      forecast_average_daily: 75000,
      forecast_average_monthly: 2250000,
      historical_total: 1200000,
      historical_average_monthly: 400000,
      forecast_start: '2025-01-01',
      forecast_end: '2025-03-01',
    });

    calls = 0;
    const forecastResult = await runCivoraAgent({
      messages: [{ role: 'user', content: 'What is my forecast for the next 3 months of 2025?' }],
      aiContext: { user_id: 101, store_id: 1, role: 'Administrator' },
      selectionRequired: false,
      availableStoreCount: 1,
    });
    assert.match(forecastResult.reply, /January 2025/i);
    assert.match(forecastResult.reply, /February 2025/i);
    assert.match(forecastResult.reply, /March 2025/i);
    assert.match(forecastResult.reply, /Total forecast/i);
    assert.strictEqual(calls, 0);

    const originalPoolExecute = dbModule.pool.execute;
    dbModule.pool.execute = async (sql, params = []) => {
      const normalizedSql = String(sql).toLowerCase();
      if (normalizedSql.includes('from sales_summaries')) {
        return [[
          {
            id: 1,
            project_id: 1,
            month_name: 'June',
            year: 2024,
            total_revenue: '1000000',
            total_cost: '600000',
            net_revenue: '400000',
            total_quantity: 10,
            top_products: JSON.stringify({ Laptop: 600000 }),
          },
          {
            id: 2,
            project_id: 1,
            month_name: 'July',
            year: 2024,
            total_revenue: '1760000',
            total_cost: '1330000',
            net_revenue: '430000',
            total_quantity: 12,
            top_products: JSON.stringify({ Laptop: 568000 }),
          },
        ]];
      }
      return originalPoolExecute(sql, params);
    };

    calls = 0;
    scenario = 'live';
    const compareResult = await runCivoraAgent({
      messages: [{ role: 'user', content: 'compare the revenues of june and july 2024' }],
      aiContext: { user_id: 101, store_id: 1, role: 'Administrator' },
      selectionRequired: false,
      availableStoreCount: 1,
    });
    assert.match(compareResult.reply, /Revenue (increased|decreased) from June 2024 to July 2024/i);
    assert.match(compareResult.reply, /Main observed contributors/i);
    assert.match(compareResult.reply, /available data confirms/i);
    assert.strictEqual(/The sales data for July 2024 shows/i.test(compareResult.reply), false);
    assert.strictEqual(calls, 0);

    calls = 0;
    const whyResult = await runCivoraAgent({
      messages: [{ role: 'user', content: 'Why did the revenue decrease?' }],
      aiContext: { user_id: 101, store_id: 1, role: 'Administrator' },
      selectionRequired: false,
      availableStoreCount: 1,
    });
    assert.match(whyResult.reply, /actually increased/i);
    assert.match(whyResult.reply, /June 2024/i);
    assert.match(whyResult.reply, /July 2024/i);
    assert.strictEqual(calls, 0);

    prophetForecastService.getRevenueForecast = originalRevenueForecast;
    dbModule.pool.execute = originalPoolExecute;

    const paragraphReply = formatYuaReply('How to import data: click Import Data, choose the file, and select the month.', {
      mode: 'knowledge',
    });
    assert.ok(!paragraphReply.startsWith('- '));

    const bulletReply = formatYuaReply(
      'Your revenue was ₹22,667,493. Top products were TV, Mobile, and Tablet.',
      { mode: 'live', needsLiveData: true }
    );
    assert.ok(bulletReply.startsWith('- '));
    assert.match(bulletReply, /₹2\.27 crore/);

    const normalizedSales = validateToolResponse('get_sales_data', {
      store: { id: '1', name: 'Authorized Store' },
      totals: {
        total_revenue: '22667493',
        total_cost: '1000000',
        net_revenue: '1200000',
        total_quantity: '42',
        month_count: '1',
      },
      monthly_rows: [],
      top_products: [],
    });
    assert.strictEqual(typeof normalizedSales.totals.total_revenue, 'number');
    assert.strictEqual(normalizedSales.totals.total_revenue, 22667493);

    assert.throws(
      () => validateToolResponse('get_sales_data', {
        store: { id: 1, name: 'Authorized Store' },
        totals: {
          total_revenue: Number.NaN,
          total_cost: 1000,
          net_revenue: 400,
          total_quantity: 10,
          month_count: 1,
        },
        monthly_rows: [],
        top_products: [],
      }),
      (error) => error.code === 'INVALID_TOOL_RESPONSE'
    );

    assert.throws(
      () => validateToolResponse('get_sales_data', {
        store: { id: 1, name: 'Authorized Store' },
        totals: {
          total_revenue: 1000,
          total_cost: 600,
          net_revenue: 400,
          total_quantity: 10,
          month_count: 1,
        },
        monthly_rows: [{ insight: '{{bad template}}' }],
        top_products: [],
      }),
      (error) => error.code === 'INVALID_TOOL_RESPONSE'
    );

    assert.strictEqual(
      sanitizeAssistantText('Revenue is {{placeholder}} and NaN is not allowed.'),
      'Revenue is and is not allowed.'
    );

    const { executeToolCall } = require('../services/toolRegistry');
    await assert.rejects(
      executeToolCall({ function: { name: 'get_sales_data', arguments: '{"store_id":2}' } }, {
        userContext: { user_id: 101 },
      }),
      (error) => error.code === 'STORE_ACCESS_DENIED'
    );

    await assert.rejects(
      executeToolCall({ function: { name: 'get_sales_data', arguments: '{"start_date":"bad-date"}' } }, {
        userContext: { user_id: 101, store_id: 1 },
      }),
      (error) => error.code === 'INVALID_TOOL_ARGUMENTS'
    );

    console.log('CIVORA agent tests: passed');
  } finally {
    global.fetch = originalFetch;
  }
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
