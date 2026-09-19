const { pool } = require('../db');

const MONTH_NAME_TO_INDEX = {
  January: 0,
  February: 1,
  March: 2,
  April: 3,
  May: 4,
  June: 5,
  July: 6,
  August: 7,
  September: 8,
  October: 9,
  November: 10,
  December: 11,
};

function parseJsonMaybe(value) {
  if (!value) return null;
  if (typeof value === 'object') return value;

  try {
    return JSON.parse(value);
  } catch (error) {
    return null;
  }
}

function parseDateInput(value, fieldName = 'date') {
  if (value === undefined || value === null || value === '') return null;

  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      const error = new Error(`Invalid date format for ${fieldName}.`);
      error.code = 'INVALID_DATE';
      throw error;
    }
    return value;
  }

  const text = String(value).trim();
  if (!text) return null;

  const date = new Date(text);
  if (Number.isNaN(date.getTime())) {
    const error = new Error(`Invalid date format for ${fieldName}. Expected ISO date format (e.g., YYYY-MM-DD).`);
    error.code = 'INVALID_DATE';
    throw error;
  }
  return date;
}

function getMonthDate(year, monthName) {
  const monthIndex = MONTH_NAME_TO_INDEX[monthName];
  if (monthIndex === undefined) {
    return null;
  }

  return new Date(Date.UTC(Number(year), monthIndex, 1));
}

function formatDateOnly(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    return null;
  }

  return date.toISOString().slice(0, 10);
}

function normalizeMonthRows(rows) {
  return [...rows].sort((left, right) => {
    if (Number(left.year) !== Number(right.year)) {
      return Number(left.year) - Number(right.year);
    }

    return (MONTH_NAME_TO_INDEX[left.month_name] ?? 99) - (MONTH_NAME_TO_INDEX[right.month_name] ?? 99);
  });
}

function isRowWithinDateRange(rowDate, startDate, endDate) {
  if (!rowDate) return false;

  if (startDate && rowDate < startDate) {
    return false;
  }

  if (endDate && rowDate > endDate) {
    return false;
  }

  return true;
}

function summarizeTopProducts(rows) {
  const aggregated = {};

  for (const row of rows) {
    const topProducts = parseJsonMaybe(row.top_products) || {};
    for (const [product, value] of Object.entries(topProducts)) {
      aggregated[product] = (aggregated[product] || 0) + Number(value || 0);
    }
  }

  return Object.entries(aggregated)
    .sort((left, right) => right[1] - left[1])
    .map(([product, revenue]) => ({ product, revenue: Number(revenue || 0) }));
}

async function resolveAuthorizedProject(userId, storeId) {
  if (storeId) {
    const [rows] = await pool.execute(
      'SELECT id, name, currency_code, timezone, user_id FROM projects WHERE id = ? AND user_id = ?',
      [storeId, userId]
    );

    if (rows.length > 0) {
      return rows[0];
    }

    const [existingRows] = await pool.execute('SELECT id FROM projects WHERE id = ?', [storeId]);
    if (existingRows.length > 0) {
      const error = new Error('Unauthorized store access');
      error.code = 'STORE_ACCESS_DENIED';
      throw error;
    }

    const error = new Error('Store not found');
    error.code = 'STORE_NOT_FOUND';
    throw error;
  }

  const [rows] = await pool.execute(
    'SELECT id, name, currency_code, timezone, user_id FROM projects WHERE user_id = ? ORDER BY id ASC',
    [userId]
  );

  if (rows.length === 1) {
    return rows[0];
  }

  if (rows.length === 0) {
    const error = new Error('No stores are available for the current user.');
    error.code = 'STORE_NOT_FOUND';
    throw error;
  }

  const error = new Error('Store context is required when the user has multiple stores.');
  error.code = 'STORE_CONTEXT_REQUIRED';
  throw error;
}

async function getSalesData({ userContext, storeId, startDate, endDate }) {
  const userId = Number(userContext?.user_id || userContext?.userId || userContext?.id);
  if (!userId) {
    const error = new Error('A valid user context is required to load sales data.');
    error.code = 'USER_CONTEXT_REQUIRED';
    throw error;
  }

  const resolvedStore = await resolveAuthorizedProject(userId, storeId || userContext?.store_id || userContext?.storeId || null);

  const start = parseDateInput(startDate, 'startDate');
  const end = parseDateInput(endDate, 'endDate');
  if (start && end && start > end) {
    const error = new Error('startDate must be earlier than or equal to endDate.');
    error.code = 'INVALID_DATE_RANGE';
    throw error;
  }

  const [rows] = await pool.execute(
    `SELECT id, project_id, month_name, year, total_revenue, total_cost, net_revenue,
            total_quantity, top_product, top_region, insight, region_data, category_data,
            top_products, detailed_entries, created_at
     FROM sales_summaries
     WHERE project_id = ?
     ORDER BY year ASC, FIELD(month_name, 'January', 'February', 'March', 'April', 'May', 'June',
                              'July', 'August', 'September', 'October', 'November', 'December') ASC`,
    [resolvedStore.id]
  );

  const processedRows = normalizeMonthRows(rows).map((row) => {
    const monthDate = getMonthDate(row.year, row.month_name);
    const regionData = parseJsonMaybe(row.region_data);
    const categoryData = parseJsonMaybe(row.category_data);
    const topProducts = parseJsonMaybe(row.top_products);
    const detailedEntries = parseJsonMaybe(row.detailed_entries);

    return {
      ...row,
      month_date: formatDateOnly(monthDate),
      region_data: regionData,
      category_data: categoryData,
      top_products: topProducts,
      detailed_entries: detailedEntries,
    };
  });

  const filteredRows = processedRows.filter((row) => {
    const rowDate = parseDateInput(row.month_date);
    return isRowWithinDateRange(rowDate, start, end);
  });

  const totals = filteredRows.reduce(
    (accumulator, row) => {
      accumulator.total_revenue += Number(row.total_revenue || 0);
      accumulator.total_cost += Number(row.total_cost || 0);
      accumulator.net_revenue += Number(row.net_revenue || 0);
      accumulator.total_quantity += Number(row.total_quantity || 0);
      return accumulator;
    },
    {
      total_revenue: 0,
      total_cost: 0,
      net_revenue: 0,
      total_quantity: 0,
    }
  );

  return {
    user_context: {
      user_id: userId,
      role: userContext?.role || userContext?.position || null,
    },
    store: {
      id: resolvedStore.id,
      name: resolvedStore.name,
      currency_code: resolvedStore.currency_code || null,
      timezone: resolvedStore.timezone || null,
    },
    range: {
      start_date: formatDateOnly(start),
      end_date: formatDateOnly(end),
    },
    totals: {
      total_revenue: Number(totals.total_revenue.toFixed(2)),
      total_cost: Number(totals.total_cost.toFixed(2)),
      net_revenue: Number(totals.net_revenue.toFixed(2)),
      total_quantity: Number(totals.total_quantity.toFixed(0)),
      month_count: filteredRows.length,
    },
    monthly_rows: filteredRows,
    top_products: summarizeTopProducts(filteredRows),
  };
}

module.exports = {
  getSalesData,
};
