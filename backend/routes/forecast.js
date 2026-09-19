const express = require('express');
const { authenticateToken } = require('../middleware/auth');
const { getRevenueForecast, getCostForecast, getProfitForecast } = require('../services/prophetForecastService');

const router = express.Router();
router.use(authenticateToken);

router.get('/forecast/revenue', async (req, res) => {
  const months = Number(req.query.months || 3);
  const projectId = req.query.projectId || req.query.project_id || null;

  try {
    const forecast = await getRevenueForecast({ months, projectId });

    res.json({
      success: true,
      scope: projectId == null ? 'global' : 'store',
      store_id: projectId == null ? null : Number(projectId),
      source: 'prophet',
      target: 'revenue',
      ...forecast,
    });
  } catch (error) {
    if (error.code === 'FORECAST_NOT_READY') {
      return res.status(404).json({
        success: false,
        scope: projectId == null ? 'global' : 'store',
        store_id: projectId == null ? null : Number(projectId),
        error: error.message,
        code: error.code,
      });
    }

    return res.status(500).json({
      success: false,
      scope: projectId == null ? 'global' : 'store',
      store_id: projectId == null ? null : Number(projectId),
      error: error.message || 'Unable to load revenue forecast.',
    });
  }
});

router.get('/forecast/cost', async (req, res) => {
  const months = Number(req.query.months || 3);
  const projectId = req.query.projectId || req.query.project_id || null;

  try {
    const forecast = await getCostForecast({ months, projectId });

    res.json({
      success: true,
      scope: projectId == null ? 'global' : 'store',
      store_id: projectId == null ? null : Number(projectId),
      source: 'prophet',
      target: 'cost',
      ...forecast,
    });
  } catch (error) {
    if (error.code === 'FORECAST_NOT_READY') {
      return res.status(404).json({
        success: false,
        scope: projectId == null ? 'global' : 'store',
        store_id: projectId == null ? null : Number(projectId),
        error: error.message,
        code: error.code,
      });
    }

    return res.status(500).json({
      success: false,
      scope: projectId == null ? 'global' : 'store',
      store_id: projectId == null ? null : Number(projectId),
      error: error.message || 'Unable to load cost forecast.',
    });
  }
});

router.get('/forecast/profit', async (req, res) => {
  const months = Number(req.query.months || 3);
  const projectId = req.query.projectId || req.query.project_id || null;

  try {
    const forecast = await getProfitForecast({ months, projectId });

    res.json({
      success: true,
      scope: projectId == null ? 'global' : 'store',
      store_id: projectId == null ? null : Number(projectId),
      source: 'prophet',
      target: 'profit',
      ...forecast,
    });
  } catch (error) {
    if (error.code === 'FORECAST_NOT_READY') {
      return res.status(404).json({
        success: false,
        scope: projectId == null ? 'global' : 'store',
        store_id: projectId == null ? null : Number(projectId),
        error: error.message,
        code: error.code,
      });
    }

    return res.status(500).json({
      success: false,
      scope: projectId == null ? 'global' : 'store',
      store_id: projectId == null ? null : Number(projectId),
      error: error.message || 'Unable to load profit forecast.',
    });
  }
});

module.exports = router;
