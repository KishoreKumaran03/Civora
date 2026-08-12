const express = require('express');
const multer = require('multer');
const fs = require('fs');
const crypto = require('crypto');
const { authenticateToken } = require('../middleware/auth');
const { runPythonUploadTask } = require('../utils/pythonTask');
const { uploadPreviewStore, cleanupUploadPreviews, removeUploadPreview } = require('../utils/previewStore');

const storage = multer.diskStorage({
  destination: 'uploads/',
  filename: (req, file, cb) => {
    const extension = file.originalname ? file.originalname.substring(file.originalname.lastIndexOf('.')) : '';
    cb(null, `${Date.now()}-${Math.round(Math.random() * 1e9)}${extension}`);
  },
});

const upload = multer({ storage });
const router = express.Router();

router.post('/upload', upload.single('file'), authenticateToken, async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'No file uploaded' });
  }

  const { project_id, month, year } = req.body;
  const userId = req.user.userId;
  const filePath = req.file.path;
  const targetYear = year || 2024;

  try {
    const result = await runPythonUploadTask('process', filePath, {
      projectId: project_id,
      month,
      year: targetYear,
      userId,
      columnMapping: {},
    });

    if (result.error) {
      return res.status(400).json({ error: result.error, columns: result.columns_found });
    }

    res.json({ message: 'Data processed successfully', data: result });
  } catch (error) {
    res.status(500).json({ error: 'Data processing failed', details: error.message });
  } finally {
    try {
      fs.unlinkSync(filePath);
    } catch (unlinkError) {
      if (unlinkError.code !== 'ENOENT') {
        console.error(`Failed to remove uploaded file ${filePath}:`, unlinkError.message);
      }
    }
  }
});

router.post('/upload/preview', upload.array('files'), authenticateToken, async (req, res) => {
  cleanupUploadPreviews();

  const files = Array.isArray(req.files) ? req.files : [];
  if (files.length === 0) {
    return res.status(400).json({ error: 'No files uploaded' });
  }

  let batchPeriods = [];
  try {
    batchPeriods = req.body.batch_periods ? JSON.parse(req.body.batch_periods) : [];
  } catch {
    return res.status(400).json({ error: 'Invalid batch period payload' });
  }

  try {
    const previewItems = [];
    const createdPreviewIds = [];

    for (let index = 0; index < files.length; index += 1) {
      const file = files[index];
      const previewResult = await runPythonUploadTask('preview', file.path);

      if (previewResult.error) {
        createdPreviewIds.forEach(removeUploadPreview);
        try {
          fs.unlinkSync(file.path);
        } catch (unlinkError) {
          if (unlinkError.code !== 'ENOENT') {
            console.error(`Failed to remove invalid preview file ${file.path}:`, unlinkError.message);
          }
        }
        return res.status(400).json({ error: previewResult.error });
      }

      const previewId = crypto.randomUUID();
      const assignedPeriod = batchPeriods[index] || {};
      const previewItem = {
        preview_id: previewId,
        file_name: file.originalname,
        filePath: file.path,
        columns: Array.isArray(previewResult.columns) ? previewResult.columns : [],
        sample_rows: Array.isArray(previewResult.sample_rows) ? previewResult.sample_rows : [],
        row_count: Number(previewResult.row_count || 0),
        month: assignedPeriod.month || req.body.month || 'January',
        year: assignedPeriod.year || req.body.year || '2024',
        user_id: req.user.userId,
        createdAt: Date.now(),
      };

      uploadPreviewStore.set(previewId, previewItem);
      createdPreviewIds.push(previewId);
      previewItems.push({
        preview_id: previewItem.preview_id,
        file_name: previewItem.file_name,
        columns: previewItem.columns,
        sample_rows: previewItem.sample_rows,
        row_count: previewItem.row_count,
        month: previewItem.month,
        year: previewItem.year,
      });
    }

    const firstColumns = JSON.stringify(previewItems[0]?.columns || []);
    const schemaMismatch = previewItems.some((item) => JSON.stringify(item.columns || []) !== firstColumns);

    res.json({
      mode: files.length > 1 ? 'batch' : 'single',
      preview_items: previewItems,
      schema_mismatch: schemaMismatch,
    });
  } catch (error) {
    files.forEach((file) => {
      try {
        fs.unlinkSync(file.path);
      } catch (unlinkError) {
        if (unlinkError.code !== 'ENOENT') {
          console.error(`Failed to remove preview file ${file.path}:`, unlinkError.message);
        }
      }
    });

    res.status(500).json({ error: 'Unable to inspect uploaded file', details: error.message });
  }
});

router.get('/upload/preview/:previewId', authenticateToken, async (req, res) => {
  cleanupUploadPreviews();

  const preview = uploadPreviewStore.get(req.params.previewId);
  if (!preview) {
    return res.status(404).json({ error: 'Upload preview not found or expired' });
  }

  if (preview.user_id !== req.user.userId) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  res.json({
    preview_id: preview.preview_id,
    file_name: preview.file_name,
    columns: preview.columns,
    sample_rows: preview.sample_rows,
    row_count: preview.row_count,
    month: preview.month,
    year: preview.year,
  });
});

router.post('/upload/complete', authenticateToken, async (req, res) => {
  cleanupUploadPreviews();

  const { project_id, preview_id, preview_items, column_mapping } = req.body || {};
  const userId = req.user.userId;
  const itemsToProcess = Array.isArray(preview_items) && preview_items.length > 0
    ? preview_items
    : (preview_id ? [{ preview_id, month: req.body.month, year: req.body.year }] : []);

  if (!project_id) {
    return res.status(400).json({ error: 'project_id is required' });
  }

  if (itemsToProcess.length === 0) {
    return res.status(400).json({ error: 'No preview selected for import' });
  }

  if (!column_mapping || typeof column_mapping !== 'object') {
    return res.status(400).json({ error: 'column_mapping is required' });
  }

  const processedResults = [];

  try {
    for (const item of itemsToProcess) {
      const preview = uploadPreviewStore.get(item.preview_id);
      if (!preview) {
        return res.status(404).json({ error: `Upload preview ${item.preview_id} not found or expired` });
      }

      if (preview.user_id !== userId) {
        return res.status(403).json({ error: 'Unauthorized' });
      }

      const result = await runPythonUploadTask('process', preview.filePath, {
        projectId: project_id,
        month: item.month || preview.month,
        year: item.year || preview.year,
        userId,
        columnMapping: column_mapping,
      });

      if (result.error) {
        return res.status(400).json({ error: result.error, columns: result.columns_found });
      }

      processedResults.push(result);
      removeUploadPreview(item.preview_id);
    }

    res.json({
      message: itemsToProcess.length > 1 ? 'Batch import completed successfully' : 'Data processed successfully',
      data: processedResults,
    });
  } catch (error) {
    res.status(500).json({ error: 'Data processing failed', details: error.message });
  }
});

module.exports = router;
