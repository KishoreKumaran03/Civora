const fs = require('fs');

const uploadPreviewStore = new Map();

function cleanupUploadPreviews() {
  const expiryMs = 1000 * 60 * 60;
  const now = Date.now();

  for (const [previewId, preview] of uploadPreviewStore.entries()) {
    if (now - preview.createdAt < expiryMs) {
      continue;
    }

    try {
      fs.unlinkSync(preview.filePath);
    } catch (error) {
      if (error.code !== 'ENOENT') {
        console.error(`Failed to remove expired upload preview file ${preview.filePath}:`, error.message);
      }
    }

    uploadPreviewStore.delete(previewId);
  }
}

function removeUploadPreview(previewId) {
  const preview = uploadPreviewStore.get(previewId);
  if (!preview) {
    return;
  }

  try {
    fs.unlinkSync(preview.filePath);
  } catch (error) {
    if (error.code !== 'ENOENT') {
      console.error(`Failed to delete upload preview file ${preview.filePath}:`, error.message);
    }
  }

  uploadPreviewStore.delete(previewId);
}

module.exports = { uploadPreviewStore, cleanupUploadPreviews, removeUploadPreview };
