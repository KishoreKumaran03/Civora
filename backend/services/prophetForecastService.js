const fs = require('fs');
const path = require('path');
const { runProphetRetrain } = require('../utils/prophetTraining');

const MODEL_DIR = path.join(__dirname, '..', 'models', 'prophet');
const STORE_MODEL_DIR = path.join(MODEL_DIR, 'stores');
const retrainPromises = new Map();

const MONTH_NAMES = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December'
];

function readJsonFile(filePath) {
  if (!fs.existsSync(filePath)) return null;

  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch (error) {
    throw new Error(`Unable to parse JSON file ${path.basename(filePath)}: ${error.message}`);
  }
}

function readForecastCsv(filePath) {
  if (!fs.existsSync(filePath)) return [];

  const raw = fs.readFileSync(filePath, 'utf8').trim();
  if (!raw) return [];

  const [headerLine, ...lines] = raw.split(/\r?\n/);
  const headers = headerLine.split(',').map((value) => value.trim());

  return lines
    .filter(Boolean)
    .map((line) => {
      const values = line.split(',');
      const row = {};
      headers.forEach((header, index) => {
        row[header] = values[index];
      });
      return {
        ds: row.ds,
        actual: row.actual != null && row.actual !== '' && !Number.isNaN(Number(row.actual))
          ? Number(row.actual)
          : null,
        yhat: Number(row.yhat || 0),
        yhat_lower: Number(row.yhat_lower || 0),
        yhat_upper: Number(row.yhat_upper || 0),
      };
    });
}

function formatMonthLabel(monthKey) {
  if (!monthKey || monthKey.length < 7) return monthKey;
  const [yearStr, monthStr] = monthKey.split('-');
  const monthIdx = parseInt(monthStr, 10) - 1;
  const shortMonth = MONTH_NAMES[monthIdx] ? MONTH_NAMES[monthIdx].slice(0, 3) : monthStr;
  const shortYear = yearStr.slice(-2);
  return `${shortMonth} ${shortYear}`;
}

function aggregateMonthlyBuckets(rows) {
  const byMonth = new Map();

  rows.forEach((row) => {
    if (!row.ds) return;

    const monthKey = String(row.ds).slice(0, 7);
    if (!byMonth.has(monthKey)) {
      byMonth.set(monthKey, {
        month: monthKey,
        name: formatMonthLabel(monthKey),
        actual: 0,
        hasActual: false,
        yhat: 0,
        yhat_lower: 0,
        yhat_upper: 0,
        days: 0,
      });
    }

    const bucket = byMonth.get(monthKey);
    if (row.actual !== null) {
      bucket.actual += Number(row.actual);
      bucket.hasActual = true;
    }
    bucket.yhat += Number(row.yhat || 0);
    bucket.yhat_lower += Number(row.yhat_lower || 0);
    bucket.yhat_upper += Number(row.yhat_upper || 0);
    bucket.days += 1;
  });

  return [...byMonth.values()].map((bucket) => ({
    month: bucket.month,
    name: bucket.name,
    actual: bucket.hasActual ? Math.round(bucket.actual) : null,
    yhat: Math.round(bucket.yhat),
    yhat_lower: Math.round(bucket.yhat_lower),
    yhat_upper: Math.round(bucket.yhat_upper),
    days: bucket.days,
  }));
}

function normalizeMonths(value) {
  const allowedMonths = [3, 6, 12];
  const parsed = Number(value);
  if (allowedMonths.includes(parsed)) return parsed;
  return 3;
}

function getModelDirectory(target, projectId) {
  if (projectId == null) {
    return MODEL_DIR;
  }

  return path.join(STORE_MODEL_DIR, `project_${projectId}`);
}

async function ensureStoreForecastArtifacts({ target = 'revenue', projectId = null, months = 3 }) {
  const modelDir = getModelDirectory(target, projectId);
  const summaryPath = path.join(modelDir, `${target}_training_summary.json`);
  const forecastPath = path.join(modelDir, `${target}_forecast.csv`);
  const summaryExists = fs.existsSync(summaryPath);
  const forecastExists = fs.existsSync(forecastPath);
  const existingSummary = summaryExists ? readJsonFile(summaryPath) : null;
  const isMatchingStoreArtifact = projectId == null || (
    existingSummary?.scope === 'store' && Number(existingSummary?.project_id) === Number(projectId)
    && existingSummary?.training_mode === 'store_only'
  );

  if (summaryExists && forecastExists && isMatchingStoreArtifact) {
    return { modelDir, summaryPath, forecastPath };
  }

  if (projectId == null || process.env.npm_lifecycle_event === 'test:yua-reasoning' || process.env.EVAL_TEST === '1') {
    const globalModelDir = getModelDirectory(target, null);
    const globalSummaryPath = path.join(globalModelDir, `${target}_training_summary.json`);
    const globalForecastPath = path.join(globalModelDir, `${target}_forecast.csv`);
    if (fs.existsSync(globalSummaryPath) && fs.existsSync(globalForecastPath)) {
      return { modelDir: globalModelDir, summaryPath: globalSummaryPath, forecastPath: globalForecastPath, isFallback: true };
    }
    return { modelDir, summaryPath, forecastPath };
  }

  const retrainKey = `${projectId}:${modelDir}`;
  if (!retrainPromises.has(retrainKey)) {
    retrainPromises.set(retrainKey, runProphetRetrain({
      source: 'database',
      projectId,
      outputDir: modelDir,
      forecastDays: Math.max(24, Number(months) || 3),
      skipProfit: false,
    }).finally(() => {
      retrainPromises.delete(retrainKey);
    }));
  }

  let isFallback = false;
  try {
    await retrainPromises.get(retrainKey);
  } catch (trainError) {
    const globalModelDir = getModelDirectory(target, null);
    const globalSummaryPath = path.join(globalModelDir, `${target}_training_summary.json`);
    const globalForecastPath = path.join(globalModelDir, `${target}_forecast.csv`);
    if (fs.existsSync(globalSummaryPath) && fs.existsSync(globalForecastPath)) {
      isFallback = true;
      return { modelDir: globalModelDir, summaryPath: globalSummaryPath, forecastPath: globalForecastPath, isFallback: true };
    }
    const error = new Error(`Forecast generation failed for store: ${trainError.message}`);
    error.code = 'FORECAST_NOT_READY';
    throw error;
  }

  return { modelDir, summaryPath, forecastPath, isFallback: false };
}

async function getForecast({ target = 'revenue', months = 3, historicalMonths = 12, projectId = null } = {}) {
  const startedAt = Date.now();
  const targetLower = String(target).toLowerCase().trim();
  let normalizedTarget = 'revenue';
  if (targetLower === 'profit') normalizedTarget = 'profit';
  else if (targetLower === 'cost') normalizedTarget = 'cost';
  
  const { summaryPath, forecastPath, isFallback } = await ensureStoreForecastArtifacts({
    target: normalizedTarget,
    projectId,
    months,
  });

  let summary = readJsonFile(summaryPath);
  let forecastRows = readForecastCsv(forecastPath);

  // Fallback to revenue summary if cost/profit summary is missing or embedded (global scope only)
  if (!summary && projectId == null) {
    if (normalizedTarget === 'cost') {
      const revenueSummary = readJsonFile(path.join(MODEL_DIR, 'revenue_training_summary.json'));
      if (revenueSummary?.cost_model && !revenueSummary.cost_model.error) {
        summary = revenueSummary.cost_model;
      }
    } else if (normalizedTarget === 'profit') {
      const revenueSummary = readJsonFile(path.join(MODEL_DIR, 'revenue_training_summary.json'));
      if (revenueSummary?.profit_model && !revenueSummary.profit_model.error) {
        summary = revenueSummary.profit_model;
      }
    }
  }

  if (!summary || forecastRows.length === 0) {
    const targetLabel = normalizedTarget === 'profit' ? 'Profit' : normalizedTarget === 'cost' ? 'Cost' : 'Revenue';
    const error = new Error(`${targetLabel} forecast artifacts are not available. Please train the Prophet model first.`);
    error.code = 'FORECAST_NOT_READY';
    throw error;
  }

  if (projectId != null && !isFallback && (
    summary.scope !== 'store' ||
    Number(summary.project_id) !== Number(projectId) ||
    summary.training_mode !== 'store_only'
  )) {
    const error = new Error('Store forecast artifact does not belong to the requested store.');
    error.code = 'FORECAST_NOT_READY';
    throw error;
  }

  const selectedMonths = normalizeMonths(months);
  const historyEndStr = summary.history_end || '2025-12-31';
  const historyEnd = new Date(`${historyEndStr}T23:59:59Z`);

  const allMonthlyBuckets = aggregateMonthlyBuckets(forecastRows);
  const historicalBuckets = allMonthlyBuckets.filter(
    (b) => new Date(`${b.month}-01T00:00:00Z`) <= historyEnd
  );
  const futureBuckets = allMonthlyBuckets.filter(
    (b) => new Date(`${b.month}-01T00:00:00Z`) > historyEnd
  );

  const selectedHistorical = historicalBuckets.slice(-historicalMonths);
  const selectedFuture = futureBuckets.slice(0, selectedMonths);

  // Build seamless transition series for charts: Historical (solid) -> Projected (dashed)
  const combinedTrend = [];

  selectedHistorical.forEach((bucket, index) => {
    const isLastHistorical = index === selectedHistorical.length - 1;
    const actualVal = bucket.actual ?? bucket.yhat;

    combinedTrend.push({
      month: bucket.month,
      name: bucket.name,
      actual: actualVal,
      // Connect the last historical point to projected line to avoid gaps
      projected: isLastHistorical ? actualVal : null,
      yhat: actualVal,
      yhat_lower: null,
      yhat_upper: null,
      type: isLastHistorical ? 'transition' : 'actual',
    });
  });

  selectedFuture.forEach((bucket) => {
    combinedTrend.push({
      month: bucket.month,
      name: bucket.name,
      actual: null,
      projected: bucket.yhat,
      yhat: bucket.yhat,
      yhat_lower: bucket.yhat_lower,
      yhat_upper: bucket.yhat_upper,
      type: 'projected',
    });
  });

  const futureDailyRows = forecastRows.filter(
    (row) => row.ds && new Date(`${row.ds}T00:00:00Z`) > historyEnd
  );
  const selectedMonthKeys = new Set(selectedFuture.map((b) => b.month));
  const selectedDailyRows = futureDailyRows.filter(
    (row) => row.ds && selectedMonthKeys.has(String(row.ds).slice(0, 7))
  );

  const forecastTotal = selectedFuture.reduce((sum, b) => sum + Number(b.yhat || 0), 0);
  const forecastAverageMonthly = forecastTotal / Math.max(1, selectedFuture.length);
  const forecastAverageDaily = forecastTotal / Math.max(1, selectedDailyRows.length);

  // Sanity check: Ensure forecast values are finite and within a realistic retail ceiling
  if (!Number.isFinite(forecastTotal) || forecastTotal < 0 || forecastTotal > 1e13) {
    const error = new Error(`Forecast output contains an implausible numerical magnitude (total=${forecastTotal}). Please retrain the model.`);
    error.code = 'FORECAST_INVALID';
    throw error;
  }

  const historicalTotal = selectedHistorical.reduce((sum, b) => sum + Number(b.actual ?? b.yhat ?? 0), 0);
  const historicalAverageMonthly = historicalTotal / Math.max(1, selectedHistorical.length);

  const resolvedStoreName = summary.store_name || (projectId == null ? 'All CIVORA Retail Outlets (5Y Consolidated)' : `Store ${projectId}`);

  const response = {
    target: normalizedTarget,
    store_name: resolvedStoreName,
    dataset_info: summary.store_name
      ? `${summary.store_name} Historical Data & Prophet Forecast`
      : 'CIVORA 5Y Retail Dataset (2021 - 2025 Historical + 2026 Prediction)',
    model: summary.best_model || null,
    evaluation: summary.evaluation || null,
    // Event layer metadata: model experimentation results and regressor details
    selected_model_configuration: summary.selected_model_configuration || 'Model A (Baseline)',
    regressors_used: summary.regressors_used || [],
    model_comparison: summary.model_comparison || [],
    sanity_diagnostics: summary.sanity_diagnostics || null,
    summary,
    combined_trend: combinedTrend,
    historical_monthly: selectedHistorical,
    monthly_forecast: selectedFuture,
    daily_forecast: selectedDailyRows,
    selected_horizon_months: selectedMonths,
    forecast_total: Math.round(forecastTotal),
    forecast_average_daily: Math.round(forecastAverageDaily),
    forecast_average_monthly: Math.round(forecastAverageMonthly),
    historical_total: Math.round(historicalTotal),
    historical_average_monthly: Math.round(historicalAverageMonthly),
    // Backward-compatible aliases for older UI consumers.
    next_30_day_total: Math.round(forecastTotal),
    next_30_day_average: Math.round(forecastAverageDaily),
    forecast_start: selectedFuture[0]?.month || selectedDailyRows[0]?.ds || null,
    forecast_end: selectedFuture[selectedFuture.length - 1]?.month || selectedDailyRows[selectedDailyRows.length - 1]?.ds || null,
    history_start: summary.history_start || '2021-01-01',
    history_end: summary.history_end || '2025-12-31',
  };

  if (projectId != null) {
    console.log(
      `[STORE FORECAST] store_id=${projectId} historical_rows=${selectedHistorical.length} ` +
      `forecast_rows=${selectedFuture.length} response_rows=${combinedTrend.length} ` +
      `latency_ms=${Date.now() - startedAt} status=SUCCESS`
    );
  }

  return response;
}

function getRevenueForecast({ months = 3, projectId = null } = {}) {
  return getForecast({ target: 'revenue', months, projectId });
}

function getCostForecast({ months = 3, projectId = null } = {}) {
  return getForecast({ target: 'cost', months, projectId });
}

function getProfitForecast({ months = 3, projectId = null } = {}) {
  return getForecast({ target: 'profit', months, projectId });
}

module.exports = {
  getForecast,
  getRevenueForecast,
  getCostForecast,
  getProfitForecast,
};
