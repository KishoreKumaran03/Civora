import { apiRequest } from './api';

export function sendAIAssistantMessage(payload, token) {
  return apiRequest({
    method: 'post',
    url: '/api/ai/chat',
    headers: { Authorization: `Bearer ${token}` },
    data: payload,
  });
}
