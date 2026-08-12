import { apiRequest } from './api';

export function uploadPreview(formData, token) {
  return apiRequest({
    method: 'post',
    url: '/api/upload/preview',
    data: formData,
    headers: { Authorization: `Bearer ${token}` }
  });
}

export function getUploadPreview(previewId, token) {
  return apiRequest({
    method: 'get',
    url: `/api/upload/preview/${previewId}`,
    headers: { Authorization: `Bearer ${token}` }
  });
}

export function completeUpload(payload, token) {
  return apiRequest({
    method: 'post',
    url: '/api/upload/complete',
    headers: { Authorization: `Bearer ${token}` },
    data: payload
  });
}
