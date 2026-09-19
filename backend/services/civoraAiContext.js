const { pool } = require('../db');

function normalizeRequestedStoreId(context = {}) {
  return (
    context.requested_store_id ||
    context.requestedStoreId ||
    context.selected_store_id ||
    context.selectedStoreId ||
    context.store_id ||
    context.storeId ||
    context.projectId ||
    context.project_id ||
    null
  );
}

async function loadUserById(userId) {
  const [rows] = await pool.execute(
    'SELECT id, name, email, position, created_at FROM users WHERE id = ?',
    [userId]
  );

  return rows[0] || null;
}

async function loadAuthorizedStore(userId, requestedStoreId) {
  if (requestedStoreId) {
    const [rows] = await pool.execute(
      'SELECT id, name, currency_code, timezone, user_id FROM projects WHERE id = ? AND user_id = ?',
      [requestedStoreId, userId]
    );

    if (rows.length === 0) {
      const [existingRows] = await pool.execute(
        'SELECT id FROM projects WHERE id = ?',
        [requestedStoreId]
      );

      if (existingRows.length > 0) {
        const error = new Error('Unauthorized store access');
        error.code = 'STORE_ACCESS_DENIED';
        throw error;
      }

      const error = new Error('Store not found');
      error.code = 'STORE_NOT_FOUND';
      throw error;
    }

    return rows[0];
  }

  const [rows] = await pool.execute(
    'SELECT id, name, currency_code, timezone, user_id FROM projects WHERE user_id = ? ORDER BY id ASC',
    [userId]
  );

  if (rows.length === 1) {
    return rows[0];
  }

  return null;
}

function buildCivoraAIContext(user, store) {
  if (!user) return null;

  return {
    user_id: user.id,
    store_id: store ? store.id : null,
    role: user.position || null,
    currency: store?.currency_code || null,
    timezone: store?.timezone || null,
  };
}

async function resolveCivoraAiContext({ userId, requestContext = {} }) {
  const user = await loadUserById(userId);
  if (!user) {
    return {
      user: null,
      store: null,
      aiContext: null,
      selectionRequired: false,
      reason: 'User not found',
    };
  }

  const requestedStoreId = normalizeRequestedStoreId(requestContext);
  const store = await loadAuthorizedStore(user.id, requestedStoreId);
  const [allStoreRows] = await pool.execute(
    'SELECT id, name, currency_code, timezone FROM projects WHERE user_id = ? ORDER BY id ASC',
    [user.id]
  );
  const allStores = allStoreRows || [];

  const selectionRequired = !store && !requestedStoreId && allStores.length > 1;

  const aiContext = buildCivoraAIContext(user, store);

  return {
    user,
    store,
    aiContext,
    selectionRequired,
    availableStoreCount: allStores.length,
  };
}

module.exports = {
  buildCivoraAIContext,
  normalizeRequestedStoreId,
  resolveCivoraAiContext,
};
