const express = require('express');
const { pool } = require('../db');
const { authenticateToken } = require('../middleware/auth');

const router = express.Router();
router.use(authenticateToken);

router.get('/notifications', async (req, res) => {
  try {
    const userId = req.user.userId;
    const notifications = [];

    const [lowStockProjects] = await pool.execute(`
      SELECT p.name, p.low_stock_threshold, ss.total_quantity, ss.month_name, ss.year
      FROM projects p
      LEFT JOIN sales_summaries ss ON p.id = ss.project_id
      WHERE p.user_id = ? AND p.low_stock_threshold IS NOT NULL
      ORDER BY ss.year DESC, ss.created_at DESC
      LIMIT 10
    `, [userId]);

    lowStockProjects.forEach((project) => {
      if (project.low_stock_threshold && project.total_quantity <= project.low_stock_threshold) {
        notifications.push({
          id: `low_stock_${project.name}_${project.month_name}_${project.year}`,
          type: 'low_stock',
          title: 'Low Stock Alert',
          message: `${project.name} has low stock (${project.total_quantity} items) for ${project.month_name} ${project.year}`,
          severity: 'warning',
          timestamp: new Date().toISOString(),
        });
      }
    });

    const [salesTrend] = await pool.execute(`
      SELECT p.name, ss.month_name, ss.year, ss.total_revenue,
             LAG(ss.total_revenue) OVER (PARTITION BY p.id ORDER BY ss.year, FIELD(ss.month_name, 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December')) as prev_revenue
      FROM projects p
      JOIN sales_summaries ss ON p.id = ss.project_id
      WHERE p.user_id = ?
      ORDER BY p.id, ss.year DESC, FIELD(ss.month_name, 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December') DESC
    `, [userId]);

    salesTrend.forEach((trend) => {
      if (trend.prev_revenue && trend.total_revenue < trend.prev_revenue * 0.9) {
        const declinePercent = Math.round((1 - trend.total_revenue / trend.prev_revenue) * 100);
        notifications.push({
          id: `declining_sales_${trend.name}_${trend.month_name}_${trend.year}`,
          type: 'declining_sales',
          title: 'Declining Sales Alert',
          message: `${trend.name} sales dropped ${declinePercent}% in ${trend.month_name} ${trend.year}`,
          severity: 'error',
          timestamp: new Date().toISOString(),
        });
      }
    });

    const hasImportedData = lowStockProjects.some((project) => project.total_quantity !== null && project.total_quantity !== undefined) || salesTrend.length > 0;

    if (hasImportedData) {
      const mockThreats = [
        {
          id: 'market_threat_1',
          type: 'market_threat',
          title: 'Market Competition Alert',
          message: 'New competitor entering your region with similar product offerings',
          severity: 'info',
          timestamp: new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString(),
        },
        {
          id: 'business_news_1',
          type: 'business_news',
          title: 'Industry Regulation Update',
          message: 'New tax regulations may affect your business operations starting next quarter',
          severity: 'warning',
          timestamp: new Date(Date.now() - 1 * 24 * 60 * 60 * 1000).toISOString(),
        },
      ];
      notifications.push(...mockThreats);
    }

    notifications.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    res.json({ notifications });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/business-news', async (req, res) => {
  try {
    const businessNews = [
      {
        id: 'news_1',
        title: 'Retail Sector Growth Forecast',
        summary: 'Industry analysts predict 15% growth in retail sector for Q4 2024',
        source: 'Business Today',
        publishedAt: new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString(),
        category: 'forecast',
        impact: 'positive',
      },
      {
        id: 'news_2',
        title: 'Supply Chain Disruptions Expected',
        summary: 'Global supply chain issues may cause product shortages in coming months',
        source: 'Economic Times',
        publishedAt: new Date(Date.now() - 6 * 60 * 60 * 1000).toISOString(),
        category: 'threat',
        impact: 'negative',
      },
      {
        id: 'news_3',
        title: 'Digital Transformation Trends',
        summary: 'Small businesses adopting digital tools see 25% increase in efficiency',
        source: 'TechCrunch',
        publishedAt: new Date(Date.now() - 12 * 60 * 60 * 1000).toISOString(),
        category: 'opportunity',
        impact: 'positive',
      },
    ];

    res.json({ news: businessNews });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
