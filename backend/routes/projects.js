const express = require('express');
const { pool } = require('../db');
const { authenticateToken } = require('../middleware/auth');

const router = express.Router();
router.use(authenticateToken);

router.get('/projects', async (req, res) => {
  try {
    const [rows] = await pool.execute('SELECT * FROM projects WHERE user_id = ?', [req.user.userId]);
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/projects', async (req, res) => {
  const {
    name,
    store_type,
    store_segments,
    branch_location_id,
    store_logo_url,
    currency_code,
    timezone,
    tax_identification_number,
    default_tax_rate,
    low_stock_threshold,
    opening_balances,
    owner_admin_email,
    contact_number,
  } = req.body;

  if (!name || !String(name).trim()) {
    return res.status(400).json({ error: 'Store name is required' });
  }

  try {
    const [result] = await pool.execute(
      `INSERT INTO projects (
        user_id, name, store_type, store_segments, branch_location_id, store_logo_url, currency_code,
        timezone, tax_identification_number, default_tax_rate, low_stock_threshold, opening_balances,
        owner_admin_email, contact_number
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        req.user.userId,
        String(name).trim(),
        store_type || null,
        store_segments ? JSON.stringify(store_segments) : null,
        branch_location_id || null,
        store_logo_url || null,
        currency_code || null,
        timezone || null,
        tax_identification_number || null,
        default_tax_rate !== '' && default_tax_rate !== undefined && default_tax_rate !== null ? Number(default_tax_rate) : null,
        low_stock_threshold !== '' && low_stock_threshold !== undefined && low_stock_threshold !== null ? Number(low_stock_threshold) : null,
        opening_balances ? JSON.stringify(opening_balances) : null,
        owner_admin_email || null,
        contact_number || null,
      ]
    );

    const [[createdProject]] = await pool.execute('SELECT * FROM projects WHERE id = ?', [result.insertId]);
    res.status(201).json(createdProject);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.patch('/projects/:projectId/logo', async (req, res) => {
  const { projectId } = req.params;
  const { store_logo_url } = req.body || {};

  if (!store_logo_url || typeof store_logo_url !== 'string') {
    return res.status(400).json({ error: 'store_logo_url is required' });
  }

  try {
    const [projectRows] = await pool.execute('SELECT id, user_id FROM projects WHERE id = ?', [projectId]);
    if (projectRows.length === 0) {
      return res.status(404).json({ error: 'Project not found' });
    }
    if (projectRows[0].user_id !== req.user.userId) {
      return res.status(403).json({ error: 'Unauthorized' });
    }

    await pool.execute('UPDATE projects SET store_logo_url = ? WHERE id = ?', [store_logo_url, projectId]);
    const [[updatedProject]] = await pool.execute('SELECT * FROM projects WHERE id = ?', [projectId]);
    res.json(updatedProject);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.patch('/projects/:projectId', async (req, res) => {
  const { projectId } = req.params;
  const {
    name,
    store_type,
    store_segments,
    branch_location_id,
    store_logo_url,
    currency_code,
    timezone,
    tax_identification_number,
    default_tax_rate,
    low_stock_threshold,
    opening_balances,
    owner_admin_email,
    contact_number,
  } = req.body || {};

  if (!name || !String(name).trim()) {
    return res.status(400).json({ error: 'Store name is required' });
  }

  try {
    const [projectRows] = await pool.execute('SELECT id, user_id FROM projects WHERE id = ?', [projectId]);
    if (projectRows.length === 0) {
      return res.status(404).json({ error: 'Project not found' });
    }
    if (projectRows[0].user_id !== req.user.userId) {
      return res.status(403).json({ error: 'Unauthorized' });
    }

    await pool.execute(
      `UPDATE projects SET
        name = ?,
        store_type = ?,
        store_segments = ?,
        branch_location_id = ?,
        store_logo_url = ?,
        currency_code = ?,
        timezone = ?,
        tax_identification_number = ?,
        default_tax_rate = ?,
        low_stock_threshold = ?,
        opening_balances = ?,
        owner_admin_email = ?,
        contact_number = ?
      WHERE id = ?`,
      [
        String(name).trim(),
        store_type || null,
        store_segments ? JSON.stringify(store_segments) : null,
        branch_location_id || null,
        store_logo_url || null,
        currency_code || null,
        timezone || null,
        tax_identification_number || null,
        default_tax_rate !== '' && default_tax_rate !== undefined && default_tax_rate !== null ? Number(default_tax_rate) : null,
        low_stock_threshold !== '' && low_stock_threshold !== undefined && low_stock_threshold !== null ? Number(low_stock_threshold) : null,
        opening_balances ? JSON.stringify(opening_balances) : null,
        owner_admin_email || null,
        contact_number || null,
        projectId,
      ]
    );

    const [[updatedProject]] = await pool.execute('SELECT * FROM projects WHERE id = ?', [projectId]);
    res.json(updatedProject);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
