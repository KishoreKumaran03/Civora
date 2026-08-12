const mysql = require('mysql2/promise');
require('dotenv').config();

const requiredEnv = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME', 'JWT_SECRET'];
const missingEnv = requiredEnv.filter((key) => !process.env[key]);
if (missingEnv.length > 0) {
  throw new Error(`Missing required environment variables: ${missingEnv.join(', ')}`);
}

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

async function ensureSchema() {
  const connection = await pool.getConnection();

  try {
    const [userColumns] = await connection.query(`
      SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'users'
    `);
    const userColumnNames = new Set(userColumns.map((c) => c.COLUMN_NAME));

    if (!userColumnNames.has('position')) {
      await connection.query("ALTER TABLE users ADD COLUMN position VARCHAR(255) DEFAULT 'Administrator'");
    }
    if (!userColumnNames.has('phone')) {
      await connection.query('ALTER TABLE users ADD COLUMN phone VARCHAR(64) DEFAULT NULL');
    }
    if (!userColumnNames.has('profile_picture')) {
      await connection.query('ALTER TABLE users ADD COLUMN profile_picture TEXT DEFAULT NULL');
    }

    const [projectColumns] = await connection.query(`
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'projects'
    `);
    const projectColumnNames = new Set(projectColumns.map((column) => column.COLUMN_NAME));

    if (!projectColumnNames.has('user_id')) {
      await connection.query('ALTER TABLE projects ADD COLUMN user_id INT NULL AFTER id');
      const [[firstUser]] = await connection.query('SELECT id FROM users ORDER BY id ASC LIMIT 1');
      if (firstUser?.id) {
        await connection.query('UPDATE projects SET user_id = ? WHERE user_id IS NULL', [firstUser.id]);
      }
      await connection.query('ALTER TABLE projects MODIFY COLUMN user_id INT NOT NULL');
    }

    const [projectForeignKeys] = await connection.query(`
      SELECT CONSTRAINT_NAME
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'projects'
        AND COLUMN_NAME = 'user_id'
        AND REFERENCED_TABLE_NAME = 'users'
    `);

    if (projectForeignKeys.length === 0) {
      await connection.query('ALTER TABLE projects ADD CONSTRAINT fk_projects_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE');
    }

    const projectColumnDefinitions = [
      ['store_type', 'ALTER TABLE projects ADD COLUMN store_type VARCHAR(255) NULL AFTER name'],
      ['store_segments', 'ALTER TABLE projects ADD COLUMN store_segments JSON NULL AFTER store_type'],
      ['branch_location_id', 'ALTER TABLE projects ADD COLUMN branch_location_id VARCHAR(255) NULL AFTER store_segments'],
      ['store_logo_url', 'ALTER TABLE projects ADD COLUMN store_logo_url TEXT NULL AFTER branch_location_id'],
      ['currency_code', 'ALTER TABLE projects ADD COLUMN currency_code VARCHAR(32) NULL AFTER store_logo_url'],
      ['timezone', 'ALTER TABLE projects ADD COLUMN timezone VARCHAR(128) NULL AFTER currency_code'],
      ['tax_identification_number', 'ALTER TABLE projects ADD COLUMN tax_identification_number VARCHAR(255) NULL AFTER timezone'],
      ['default_tax_rate', 'ALTER TABLE projects ADD COLUMN default_tax_rate DECIMAL(10, 2) NULL AFTER tax_identification_number'],
      ['low_stock_threshold', 'ALTER TABLE projects ADD COLUMN low_stock_threshold INT NULL AFTER default_tax_rate'],
      ['opening_balances', 'ALTER TABLE projects ADD COLUMN opening_balances JSON NULL AFTER low_stock_threshold'],
      ['owner_admin_email', 'ALTER TABLE projects ADD COLUMN owner_admin_email VARCHAR(255) NULL AFTER opening_balances'],
      ['contact_number', 'ALTER TABLE projects ADD COLUMN contact_number VARCHAR(64) NULL AFTER owner_admin_email'],
    ];

    for (const [columnName, query] of projectColumnDefinitions) {
      if (!projectColumnNames.has(columnName)) {
        await connection.query(query);
      }
    }

    const [salesSummaryColumns] = await connection.query(`
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'sales_summaries'
    `);
    const salesSummaryColumnNames = new Set(salesSummaryColumns.map((column) => column.COLUMN_NAME));
    if (!salesSummaryColumnNames.has('detailed_entries')) {
      await connection.query('ALTER TABLE sales_summaries ADD COLUMN detailed_entries JSON NULL AFTER top_products');
    }
  } finally {
    connection.release();
  }
}

module.exports = { pool, ensureSchema };
