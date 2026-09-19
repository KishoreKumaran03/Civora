const assert = require('assert');

process.env.DB_HOST = process.env.DB_HOST || 'localhost';
process.env.DB_USER = process.env.DB_USER || 'user';
process.env.DB_PASSWORD = process.env.DB_PASSWORD || 'password';
process.env.DB_NAME = process.env.DB_NAME || 'datavis_db';
process.env.JWT_SECRET = process.env.JWT_SECRET || 'test-jwt-secret';

const dbModule = require('../db');

function makeRow({
  id,
  month_name,
  year,
  total_revenue,
  total_cost,
  net_revenue,
  total_quantity,
  top_product,
  top_region,
  top_products,
  category_data,
  region_data,
}) {
  return {
    id,
    project_id: 1,
    month_name,
    year,
    total_revenue: String(total_revenue),
    total_cost: String(total_cost),
    net_revenue: String(net_revenue),
    total_quantity,
    top_product,
    top_region,
    top_products: JSON.stringify(top_products || {}),
    category_data: JSON.stringify(category_data || {}),
    region_data: JSON.stringify(region_data || {}),
    detailed_entries: JSON.stringify([]),
  };
}

const baseRows = [
  makeRow({
    id: 1,
    month_name: 'January',
    year: 2024,
    total_revenue: 0,
    total_cost: 0,
    net_revenue: 0,
    total_quantity: 0,
    top_products: {},
    category_data: {},
    region_data: {},
  }),
  makeRow({
    id: 2,
    month_name: 'February',
    year: 2024,
    total_revenue: 500000,
    total_cost: 300000,
    net_revenue: 200000,
    total_quantity: 30,
    top_product: 'Starter',
    top_region: 'Karnataka',
    top_products: { Starter: 500000 },
    category_data: { Electronics: 300000, Fashion: 200000 },
    region_data: { Karnataka: 250000, Punjab: 250000 },
  }),
  makeRow({
    id: 3,
    month_name: 'June',
    year: 2024,
    total_revenue: 19800000,
    total_cost: 14100000,
    net_revenue: 5700000,
    total_quantity: 1200,
    top_product: 'Laptop',
    top_region: 'Andhra Pradesh',
    top_products: {
      Laptop: 5800000,
      TV: 5000000,
      Mobile: 4200000,
      Tablet: 2500000,
      Headphones: 2300000,
    },
    category_data: {
      Electronics: 15000000,
      Fashion: 3000000,
      Groceries: 1800000,
    },
    region_data: {
      'Andhra Pradesh': 8000000,
      Karnataka: 7000000,
      Punjab: 4800000,
    },
  }),
  makeRow({
    id: 4,
    month_name: 'July',
    year: 2024,
    total_revenue: 17600000,
    total_cost: 13300000,
    net_revenue: 4300000,
    total_quantity: 1100,
    top_product: 'Laptop',
    top_region: 'Andhra Pradesh',
    top_products: {
      Laptop: 5200000,
      TV: 4500000,
      Mobile: 3800000,
      Tablet: 2000000,
      Headphones: 2100000,
    },
    category_data: {
      Electronics: 15100000,
      Fashion: 1500000,
      Groceries: 1000000,
    },
    region_data: {
      'Andhra Pradesh': 6500000,
      Karnataka: 6000000,
      Punjab: 5100000,
    },
  }),
  makeRow({
    id: 5,
    month_name: 'August',
    year: 2024,
    total_revenue: 18000000,
    total_cost: 13000000,
    net_revenue: 5000000,
    total_quantity: 1150,
    top_product: 'Laptop',
    top_region: 'Andhra Pradesh',
    top_products: {
      Laptop: 5400000,
      TV: 5000000,
      Mobile: 3900000,
      Tablet: 2400000,
      Headphones: 1300000,
    },
    category_data: {
      Electronics: 15500000,
      Fashion: 1500000,
      Groceries: 1000000,
    },
    region_data: {
      'Andhra Pradesh': 7000000,
      Karnataka: 6500000,
      Punjab: 4500000,
    },
  }),
  makeRow({
    id: 6,
    month_name: 'October',
    year: 2024,
    total_revenue: 17800000,
    total_cost: 13000000,
    net_revenue: 4800000,
    total_quantity: 1280,
    top_product: 'Laptop',
    top_region: 'Andhra Pradesh',
    top_products: {
      Laptop: 5800000,
      TV: 4700000,
      Mobile: 4000000,
      Tablet: 1400000,
      Headphones: 1900000,
    },
    category_data: {
      Electronics: 12000000,
      Fashion: 3000000,
      Groceries: 2800000,
    },
    region_data: {
      'Andhra Pradesh': 6500000,
      Karnataka: 6000000,
      Punjab: 5300000,
    },
  }),
  makeRow({
    id: 7,
    month_name: 'November',
    year: 2024,
    total_revenue: 16600000,
    total_cost: 12500000,
    net_revenue: 4100000,
    total_quantity: 1220,
    top_product: 'Laptop',
    top_region: 'Andhra Pradesh',
    top_products: {
      Laptop: 5200000,
      TV: 4300000,
      Mobile: 3500000,
      Tablet: 1800000,
      Headphones: 1800000,
    },
    category_data: {
      Electronics: 11000000,
      Fashion: 2600000,
      Groceries: 3000000,
    },
    region_data: {
      'Andhra Pradesh': 6000000,
      Karnataka: 5200000,
      Punjab: 5400000,
    },
  }),
  makeRow({
    id: 8,
    month_name: 'December',
    year: 2024,
    total_revenue: 16600000,
    total_cost: 12500000,
    net_revenue: 4100000,
    total_quantity: 1220,
    top_product: 'Laptop',
    top_region: 'Andhra Pradesh',
    top_products: {
      Laptop: 5200000,
      TV: 4300000,
      Mobile: 3500000,
      Tablet: 1800000,
      Headphones: 1800000,
    },
    category_data: {
      Electronics: 11000000,
      Fashion: 2600000,
      Groceries: 3000000,
    },
    region_data: {
      'Andhra Pradesh': 6000000,
      Karnataka: 5200000,
      Punjab: 5400000,
    },
  }),
  makeRow({
    id: 9,
    month_name: 'January',
    year: 2025,
    total_revenue: 23200000,
    total_cost: 17500000,
    net_revenue: 5700000,
    total_quantity: 1280,
    top_product: 'Headphones',
    top_region: 'Punjab',
    top_products: {
      Headphones: 6200000,
      Laptop: 5900000,
      Mobile: 4400000,
      Tablet: 3100000,
      TV: 3600000,
    },
    category_data: {
      Electronics: 15800000,
      Fashion: 3200000,
      Groceries: 4200000,
    },
    region_data: {
      'Andhra Pradesh': 6200000,
      Karnataka: 6800000,
      Punjab: 9000000,
    },
  }),
];

const sparseRows = [
  makeRow({
    id: 101,
    month_name: 'June',
    year: 2024,
    total_revenue: 1000000,
    total_cost: 600000,
    net_revenue: 400000,
    total_quantity: 50,
    top_products: {},
    category_data: {},
    region_data: {},
  }),
  makeRow({
    id: 102,
    month_name: 'July',
    year: 2024,
    total_revenue: 1200000,
    total_cost: 700000,
    net_revenue: 500000,
    total_quantity: 55,
    top_products: {},
    category_data: {},
    region_data: {},
  }),
];

const projectLookup = {
  1: { id: 1, user_id: 101, name: 'Authorized Store', currency_code: 'INR', timezone: 'Asia/Kolkata' },
  2: { id: 2, user_id: 202, name: 'Other Store', currency_code: 'INR', timezone: 'Asia/Kolkata' },
};

let scenario = 'base';

dbModule.pool.execute = async (sql, params = []) => {
  const normalizedSql = String(sql).toLowerCase();

  if (normalizedSql.includes('from projects where id = ? and user_id = ?')) {
    const projectId = Number(params[0]);
    const userId = Number(params[1]);
    const project = projectLookup[projectId];
    return [[project && project.user_id === userId ? project : []].flat()];
  }

  if (normalizedSql.includes('from projects where id = ?')) {
    const projectId = Number(params[0]);
    return [[projectLookup[projectId] ? { id: projectId } : []].flat()];
  }

  if (normalizedSql.includes('from projects where user_id = ? order by id asc')) {
    const userId = Number(params[0]);
    const rows = Object.values(projectLookup).filter((project) => project.user_id === userId);
    return [rows];
  }

  if (normalizedSql.includes('from sales_summaries')) {
    if (scenario === 'sparse') {
      return [[...sparseRows]];
    }

    return [[...baseRows]];
  }

  return [[]];
};

async function analyze(queryText, options = {}) {
  const { runAnalyticalReasoning, runExtremeReasoning, validateAnalyticalConsistency } = require('../services/yuaAnalyticsReasoningService');
  return runAnalyticalReasoning({
    queryText,
    conversation: options.conversation || [],
    aiContext: options.aiContext || { user_id: 101, store_id: 1, role: 'Administrator' },
  });
}

async function run() {
  const { validateAnalyticalConsistency, runExtremeReasoning } = require('../services/yuaAnalyticsReasoningService');

  try {
    scenario = 'base';

    const compareResult = await analyze('Compare the revenues of June and July 2024');
    assert.match(compareResult.reply, /June 2024/);
    assert.match(compareResult.reply, /July 2024/);
    assert.match(compareResult.reply, /decreased/i);

    const highestMonth = await runExtremeReasoning({
      queryText: 'Which month gave the highest revenue in 2024?',
      aiContext: { user_id: 101, store_id: 1, role: 'Administrator' },
    });
    assert.match(highestMonth.reply, /June 2024/);
    assert.match(highestMonth.reply, /highest revenue/i);

    const abbreviatedCompare = await analyze('Compare Jan vs Feb 2024 revenue');
    assert.match(abbreviatedCompare.reply, /January 2024/);
    assert.match(abbreviatedCompare.reply, /February 2024/);
    assert.match(abbreviatedCompare.reply, /increased/i);

    const premiseCorrection = await analyze('Why did revenue increase from June to July 2024?');
    assert.match(premiseCorrection.reply, /actually decreased/i);
    assert.strictEqual(/was the higher month/i.test(premiseCorrection.reply), false);

    const increaseAfterDecrease = await analyze('Why did revenue decrease from July to August 2024?');
    assert.match(increaseAfterDecrease.reply, /actually increased/i);

    const octoberNovember = await analyze('Why did revenue decrease from October to November 2024?');
    assert.match(octoberNovember.reply, /October 2024/);
    assert.match(octoberNovember.reply, /November 2024/);
    assert.match(octoberNovember.reply, /decreased/i);
    assert.match(octoberNovember.reply, /Laptop/);

    const noChange = await analyze('Compare the revenues of November and December 2024');
    assert.match(noChange.reply, /no material change/i);

    const quantityChange = await analyze('Why did quantity decrease from June to July 2024?');
    assert.match(quantityChange.reply, /quantity/i);
    assert.match(quantityChange.reply, /decreased/i);

    const costChange = await analyze('Why did cost decrease from October to November 2024?');
    assert.match(costChange.reply, /cost/i);
    assert.match(costChange.reply, /decreased/i);

    const profitGrowth = await analyze('Why did profit increase from July to August 2024?');
    assert.match(profitGrowth.reply, /profit/i);
    assert.match(profitGrowth.reply, /increased/i);

    const zeroBase = await analyze('Why did revenue increase from January to February 2024?');
    assert.match(zeroBase.reply, /January 2024/);
    assert.match(zeroBase.reply, /February 2024/);
    assert.match(zeroBase.reply, /n\/a/i);

    const comparisonConversation = [
      {
        role: 'assistant',
        content: 'October 2024 revenue was ₹1.78 crore.\nNovember 2024 revenue was ₹1.66 crore.\nRevenue decreased by ₹12.0 lakh (6.7%).',
      },
    ];

    const productFollowUp = await analyze('Which product caused most of it?', {
      conversation: comparisonConversation,
    });
    assert.match(productFollowUp.reply, /product/i);
    assert.match(productFollowUp.reply, /Laptop/i);

    const categoryFollowUp = await analyze('Which category drove the change?', {
      conversation: comparisonConversation,
    });
    assert.match(categoryFollowUp.reply, /category/i);
    assert.match(categoryFollowUp.reply, /Electronics/i);

    const regionFollowUp = await analyze('Which region drove the change?', {
      conversation: comparisonConversation,
    });
    assert.match(regionFollowUp.reply, /region/i);

    const whyFollowUp = await analyze('Why did it decrease?', {
      conversation: comparisonConversation,
    });
    assert.match(whyFollowUp.reply, /October 2024/);
    assert.match(whyFollowUp.reply, /November 2024/);

    const januaryFebruaryConversation = [
      {
        role: 'assistant',
        content: 'January 2024 revenue was â‚¹2.00 crore.\nFebruary 2024 revenue was â‚¹2.37 crore.\nRevenue increased by â‚¹36.1 lakh (18.0%).',
      },
    ];

    const vagueIncreaseFollowUp = await analyze('why did it increase?', {
      conversation: januaryFebruaryConversation,
    });
    assert.match(vagueIncreaseFollowUp.reply, /January 2024/);
    assert.match(vagueIncreaseFollowUp.reply, /February 2024/);
    assert.strictEqual(/January 2025/i.test(vagueIncreaseFollowUp.reply), false);

    const previousMonthFollowUp = await analyze('What about the previous month?', {
      conversation: comparisonConversation,
    });
    assert.match(previousMonthFollowUp.reply, /October 2024/);
    assert.match(previousMonthFollowUp.reply, /November 2024/);

    scenario = 'sparse';
    const sparseConversation = [
      {
        role: 'assistant',
        content: 'June 2024 revenue was â‚¹10.0 lakh.\nJuly 2024 revenue was â‚¹12.0 lakh.\nRevenue increased by â‚¹2.0 lakh (20.0%).',
      },
    ];
    const sparseFollowUp = await analyze('Which product caused it?', {
      conversation: sparseConversation,
    });
    assert.match(sparseFollowUp.reply, /does not provide enough evidence/i);

    scenario = 'base';
    const consistentAnalysis = {
      previous_value: 100,
      current_value: 125,
      absolute_change: 25,
      percentage_change: 25,
      direction: 'increase',
    };
    assert.strictEqual(validateAnalyticalConsistency(consistentAnalysis), true);

    assert.throws(
      () => validateAnalyticalConsistency({
        previous_value: 200,
        current_value: 100,
        absolute_change: 100,
        percentage_change: 50,
        direction: 'increase',
      }),
      (error) => error.code === 'ANALYTICAL_INCONSISTENCY'
    );

    await assert.rejects(
      analyze('Compare the revenues of June and July 2024', {
        aiContext: { user_id: 101, store_id: 2, role: 'Administrator' },
      }),
      (error) => error.code === 'STORE_ACCESS_DENIED'
    );

    await assert.rejects(
      analyze('Compare the revenues of June and July 2024', {
        aiContext: { user_id: 999, store_id: 1, role: 'Administrator' },
      }),
      (error) => error.code === 'STORE_ACCESS_DENIED' || error.code === 'USER_CONTEXT_REQUIRED'
    );

    const directDrivers = await analyze('Which category drove the growth from July to August 2024?');
    assert.match(directDrivers.reply, /Electronics/i);

    console.log('CIVORA analytics reasoning tests: passed');
  } finally {
    // no-op
  }
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
