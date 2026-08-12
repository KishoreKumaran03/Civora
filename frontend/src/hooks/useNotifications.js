import { useState, useEffect } from 'react';
import notificationStore from '../store/notificationStore';

export function useNotifications(token) {
  const [notifications, setNotifications] = useState(() => notificationStore.getNotifications());
  const [loading, setLoading] = useState(() => notificationStore.isLoading());
  const [error, setError] = useState(() => notificationStore.getError());

  useEffect(() => {
    const unsubscribe = notificationStore.subscribe(() => {
      setNotifications(notificationStore.getNotifications());
      setLoading(notificationStore.isLoading());
      setError(notificationStore.getError());
    });
    return unsubscribe;
  }, []);

  useEffect(() => {
    if (token) {
      notificationStore.fetchNotifications(token);
    }
  }, [token]);

  return { notifications, loading, error };
}
export default useNotifications;
