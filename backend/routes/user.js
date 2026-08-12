const express = require('express');
const { pool } = require('../db');
const { authenticateToken } = require('../middleware/auth');

const router = express.Router();
router.use(authenticateToken);

router.get('/user/profile', async (req, res) => {
  try {
    const [userRows] = await pool.execute(
      'SELECT id, name, email, position, profile_picture, phone, created_at FROM users WHERE id = ?',
      [req.user.userId]
    );

    if (userRows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    const [projects] = await pool.execute(
      'SELECT id, name, store_type, branch_location_id, created_at FROM projects WHERE user_id = ?',
      [req.user.userId]
    );

    res.json({
      user: userRows[0],
      stores: projects,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.put('/user/profile', async (req, res) => {
  const { name, email, position, profile_picture, phone } = req.body;

  try {
    await pool.execute(
      'UPDATE users SET name = ?, email = ?, position = ?, profile_picture = ?, phone = ? WHERE id = ?',
      [name, email, position, profile_picture, phone, req.user.userId]
    );

    const [userRows] = await pool.execute(
      'SELECT id, name, email, position, profile_picture, phone FROM users WHERE id = ?',
      [req.user.userId]
    );

    res.json({ user: userRows[0] });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') {
      return res.status(400).json({ error: 'Email already exists' });
    }
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
