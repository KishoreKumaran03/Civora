import { apiRequest } from './api';

export function sendAIAssistantMessage(payload, token) {
  return apiRequest({
    method: 'post',
    url: '/api/ai/chat',
    headers: { Authorization: `Bearer ${token}` },
    data: payload,
  });
}

export function getAIAssistantHealth(token) {
  return apiRequest({
    method: 'get',
    url: '/api/ai/health',
    headers: { Authorization: `Bearer ${token}` },
  });
}
