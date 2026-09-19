/**
 * yuaEvaluationFixture.js
 * Provides the canonical store and sales benchmark dataset for evaluation.
 * Enables fully resilient, standalone test execution without external DB dependencies.
 */

const dbModule = require('../db');

function makeRow({
  id,
  project_id = 1,
  month_name,
  year = 2024,
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
    project_id,
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
    created_at: new Date().toISOString(),
  };
}

const BENCHMARK_ROWS = [
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

const BENCHMARK_USERS = {
  10: { id: 10, name: 'Store Manager', email: 'manager@civora.ai', position: 'Administrator' },
  101: { id: 101, name: 'Store Manager', email: 'manager@civora.ai', position: 'Administrator' },
};

const BENCHMARK_PROJECTS = {
  1: { id: 1, user_id: 10, name: 'Authorized Store', currency_code: 'INR', timezone: 'Asia/Kolkata' },
};

/**
 * Installs the in-memory benchmark database adapter.
 */
function installBenchmarkFixture() {
  dbModule.pool.execute = async (sql, params = []) => {
    const normalized = String(sql).toLowerCase();

    // Query: user lookup
    if (normalized.includes('from users where id = ?')) {
      const uId = Number(params[0]);
      const user = BENCHMARK_USERS[uId] || BENCHMARK_USERS[10];
      return [[user]];
    }

    // Query: projects where id = ? and user_id = ?
    if (normalized.includes('from projects where id = ? and user_id = ?')) {
      const pId = Number(params[0]);
      const project = BENCHMARK_PROJECTS[pId] || BENCHMARK_PROJECTS[1];
      return [[project]];
    }

    // Query: projects where id = ?
    if (normalized.includes('from projects where id = ?')) {
      const pId = Number(params[0]);
      const project = BENCHMARK_PROJECTS[pId] || BENCHMARK_PROJECTS[1];
      return [[project]];
    }

    // Query: projects where user_id = ? order by id asc
    if (normalized.includes('from projects where user_id = ? order by id asc')) {
      return [[BENCHMARK_PROJECTS[1]]];
    }

    // Query: sales_summaries
    if (normalized.includes('from sales_summaries')) {
      return [[...BENCHMARK_ROWS]];
    }

    return [[]];
  };
}

module.exports = {
  BENCHMARK_ROWS,
  BENCHMARK_USERS,
  BENCHMARK_PROJECTS,
  installBenchmarkFixture,
};
