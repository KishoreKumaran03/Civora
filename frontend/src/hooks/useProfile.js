import { useState, useEffect } from 'react';
import { apiRequest } from '../services/api';

export function useProfile(token) {
  const [profileData, setProfileData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const fetchProfile = async () => {
    if (!token) return;
    setLoading(true);
    setError(null);
    try {
      const res = await apiRequest({ method: 'get', url: '/api/user/profile', headers: { 'Authorization': `Bearer ${token}` } });
      setProfileData(res.data);
    } catch (err) {
      setError(err);
    } finally {
      setLoading(false);
    }
  };

  return { profileData, loading, error, fetchProfile };
}
export default useProfile;
