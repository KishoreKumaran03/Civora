const express = require('express');
const { pool } = require('../db');
const { authenticateToken } = require('../middleware/auth');
const { normalizeStateRevenueMap } = require('../utils/stateNormalization');

const router = express.Router();
router.use(authenticateToken);

router.get('/dashboard/summary', async (req, res) => {
  const year = req.query.year || 2024;

  try {
    const [yearRows] = await pool.execute(
      `SELECT DISTINCT ss.year
       FROM sales_summaries ss
       JOIN projects p ON ss.project_id = p.id
       WHERE p.user_id = ?
       ORDER BY ss.year ASC`,
      [req.user.userId]
    );

    const availableYears = yearRows.map((row) => String(row.year)).filter(Boolean);

    const [rows] = await pool.execute(
      `SELECT 
        SUM(total_revenue) as total_revenue,
        SUM(total_cost) as total_cost,
        SUM(net_revenue) as net_revenue,
        SUM(total_quantity) as total_quantity,
        COUNT(DISTINCT project_id) as project_count
      FROM sales_summaries ss
      JOIN projects p ON ss.project_id = p.id
      WHERE p.user_id = ? AND ss.year = ?`,
      [req.user.userId, year]
    );

    const [trend] = await pool.execute(
      `SELECT month_name, SUM(total_revenue) as total_revenue, SUM(net_revenue) as net_revenue
      FROM sales_summaries ss
      JOIN projects p ON ss.project_id = p.id
      WHERE p.user_id = ? AND ss.year = ?
      GROUP BY month_name
      ORDER BY FIELD(month_name, 'January', 'February', 'March', 'April', 'May', 'June', 
                                 'July', 'August', 'September', 'October', 'November', 'December')`,
      [req.user.userId, year]
    );

    const [regionRows] = await pool.execute(
      `SELECT region_data FROM sales_summaries ss
      JOIN projects p ON ss.project_id = p.id
      WHERE p.user_id = ? AND ss.year = ?`,
      [req.user.userId, year]
    );

    let aggregatedRegionData = {};
    regionRows.forEach((row) => {
      if (row.region_data) {
        try {
          const regionData = typeof row.region_data === 'string' ? JSON.parse(row.region_data) : row.region_data;
          const normalizedRegionData = normalizeStateRevenueMap(regionData);
          Object.entries(normalizedRegionData).forEach(([region, value]) => {
            aggregatedRegionData[region] = (aggregatedRegionData[region] || 0) + value;
          });
        } catch (error) {
          console.error('Error parsing region_data:', error);
        }
      }
    });

    const [categoryRows] = await pool.execute(
      `SELECT category_data FROM sales_summaries ss
      JOIN projects p ON ss.project_id = p.id
      WHERE p.user_id = ? AND ss.year = ?`,
      [req.user.userId, year]
    );

    let aggregatedCategoryData = {};
    categoryRows.forEach((row) => {
      if (row.category_data) {
        try {
          const categoryData = typeof row.category_data === 'string' ? JSON.parse(row.category_data) : row.category_data;
          Object.entries(categoryData).forEach(([category, value]) => {
            aggregatedCategoryData[category] = (aggregatedCategoryData[category] || 0) + value;
          });
        } catch (error) {
          console.error('Error parsing category_data:', error);
        }
      }
    });

    const [productRows] = await pool.execute(
      `SELECT top_products FROM sales_summaries ss
      JOIN projects p ON ss.project_id = p.id
      WHERE p.user_id = ? AND ss.year = ?`,
      [req.user.userId, year]
    );

    let aggregatedProductData = {};
    productRows.forEach((row) => {
      if (row.top_products) {
        try {
          const productData = typeof row.top_products === 'string' ? JSON.parse(row.top_products) : row.top_products;
          Object.entries(productData || {}).forEach(([product, value]) => {
            aggregatedProductData[product] = (aggregatedProductData[product] || 0) + Number(value || 0);
          });
        } catch (error) {
          console.error('Error parsing top_products:', error);
        }
      }
    });

    res.json({
      stats: rows[0],
      trend,
      region_data: aggregatedRegionData,
      state_data: aggregatedRegionData,
      category_data: aggregatedCategoryData,
      product_data: aggregatedProductData,
      available_years: availableYears,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/dashboard/:projectId', async (req, res) => {
  const { projectId } = req.params;

  try {
    const [project] = await pool.execute('SELECT user_id FROM projects WHERE id = ?', [projectId]);
    if (project.length === 0 || project[0].user_id !== req.user.userId) {
      return res.status(403).json({ error: 'Unauthorized' });
    }

    const [rows] = await pool.execute(
      `SELECT id, project_id, month_name, year, total_revenue, total_cost, net_revenue, 
              total_quantity, region_data, category_data, top_product, top_region, insight, top_products, detailed_entries
       FROM sales_summaries WHERE project_id = ? ORDER BY year DESC, 
       FIELD(month_name, 'January', 'February', 'March', 'April', 'May', 'June', 
                         'July', 'August', 'September', 'October', 'November', 'December')`,
      [projectId]
    );

    const processedRows = rows.map((row) => ({
      ...row,
      region_data: normalizeStateRevenueMap(typeof row.region_data === 'string' ? JSON.parse(row.region_data) : row.region_data),
      category_data: typeof row.category_data === 'string' ? JSON.parse(row.category_data) : row.category_data,
      top_products: typeof row.top_products === 'string' ? JSON.parse(row.top_products) : row.top_products,
      detailed_entries: typeof row.detailed_entries === 'string' ? JSON.parse(row.detailed_entries) : row.detailed_entries,
    }));

    res.json(processedRows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.delete('/dashboard/:projectId', async (req, res) => {
  const { projectId } = req.params;

  try {
    const [project] = await pool.execute('SELECT user_id FROM projects WHERE id = ?', [projectId]);
    if (project.length === 0 || project[0].user_id !== req.user.userId) {
      return res.status(403).json({ error: 'Unauthorized' });
    }

    await pool.execute('DELETE FROM sales_summaries WHERE project_id = ?', [projectId]);
    res.json({ message: 'All data for this store has been cleared.' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.delete('/dashboard', async (req, res) => {
  try {
    const [result] = await pool.execute(
      `DELETE ss
       FROM sales_summaries ss
       INNER JOIN projects p ON p.id = ss.project_id
       WHERE p.user_id = ?`,
      [req.user.userId]
    );

    res.json({
      message: 'All imported analytics data has been cleared.',
      deleted_rows: result.affectedRows || 0,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
