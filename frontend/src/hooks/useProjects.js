import { useState, useEffect } from 'react';
import { getProjects, createProject as createProjectApi, updateProject as updateProjectApi, deleteProjectMetrics } from '../services/projectService';
import { getStoredFavoriteProjectIds, setStoredFavoriteProjectIds } from '../utils/storage';

export function useProjects(token) {
  const [projects, setProjects] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [favoriteProjectIds, setFavoriteProjectIds] = useState(() => getStoredFavoriteProjectIds());

  const fetchProjects = async () => {
    if (!token) return;
    setLoading(true);
    setError(null);
    try {
      const res = await getProjects(token);
      setProjects(res.data);
    } catch (err) {
      setError(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchProjects();
  }, [token]);

  const toggleFavorite = (projectId) => {
    const normalizedId = String(projectId);
    setFavoriteProjectIds((currentFavorites) => {
      const nextFavorites = currentFavorites.includes(normalizedId)
        ? currentFavorites.filter((favoriteId) => favoriteId !== normalizedId)
        : [...currentFavorites, normalizedId];
      setStoredFavoriteProjectIds(nextFavorites);
      return nextFavorites;
    });
  };

  const createNewProject = async (projectForm) => {
    const res = await createProjectApi(projectForm, token);
    setProjects((current) => [...current, res.data]);
    return res.data;
  };

  const updateExistingProject = async (projectId, projectForm) => {
    const res = await updateProjectApi(projectId, projectForm, token);
    setProjects((current) => current.map((p) => String(p.id) === String(projectId) ? res.data : p));
    return res.data;
  };

  const deleteMetrics = async (projectId) => {
    await deleteProjectMetrics(projectId, token);
  };

  return {
    projects,
    setProjects,
    loading,
    error,
    favoriteProjectIds,
    toggleFavorite,
    fetchProjects,
    createProject: createNewProject,
    updateProject: updateExistingProject,
    deleteMetrics
  };
}
export default useProjects;
