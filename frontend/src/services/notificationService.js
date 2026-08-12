import { apiRequest } from './api';

export function getNotifications(token) {
  return apiRequest({
    method: 'get',
    url: '/api/notifications',
    headers: { Authorization: `Bearer ${token}` }
  });
}
