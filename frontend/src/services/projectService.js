import { apiRequest } from './api';

export function getProjects(token) {
  return apiRequest({
    method: 'get',
    url: '/api/projects',
    headers: { Authorization: `Bearer ${token}` }
  });
}

export function createProject(projectForm, token) {
  return apiRequest({
    method: 'post',
    url: '/api/projects',
    headers: { Authorization: `Bearer ${token}` },
    data: projectForm
  });
}

export function updateProject(projectId, projectForm, token) {
  return apiRequest({
    method: 'patch',
    url: `/api/projects/${projectId}`,
    headers: { Authorization: `Bearer ${token}` },
    data: projectForm
  });
}

export function deleteProjectMetrics(projectId, token) {
  return apiRequest({
    method: 'delete',
    url: `/api/dashboard/${projectId}`,
    headers: { Authorization: `Bearer ${token}` }
  });
}

export function getStoreData(projectId, token) {
  return apiRequest({
    method: 'get',
    url: `/api/dashboard/${projectId}`,
    headers: { Authorization: `Bearer ${token}` }
  });
}

export function getDashboardSummary(year, token) {
  return apiRequest({
    method: 'get',
    url: `/api/dashboard/summary?year=${year}`,
    headers: { Authorization: `Bearer ${token}` }
  });
}

export function getRevenueForecast(months, token, projectId = null) {
  const projectQuery = projectId ? `&projectId=${projectId}` : '';
  return apiRequest({
    method: 'get',
    url: `/api/forecast/revenue?months=${months}${projectQuery}`,
    headers: { Authorization: `Bearer ${token}` }
  });
}

export function getCostForecast(months, token, projectId = null) {
  const projectQuery = projectId ? `&projectId=${projectId}` : '';
  return apiRequest({
    method: 'get',
    url: `/api/forecast/cost?months=${months}${projectQuery}`,
    headers: { Authorization: `Bearer ${token}` }
  });
}

export function getProfitForecast(months, token, projectId = null) {
  const projectQuery = projectId ? `&projectId=${projectId}` : '';
  return apiRequest({
    method: 'get',
    url: `/api/forecast/profit?months=${months}${projectQuery}`,
    headers: { Authorization: `Bearer ${token}` }
  });
}
