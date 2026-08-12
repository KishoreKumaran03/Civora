import { getNotifications } from '../services/notificationService';

let notifications = [];
let loading = false;
let error = null;

const listeners = new Set();

export const notificationStore = {
  getNotifications() {
    return notifications;
  },
  isLoading() {
    return loading;
  },
  getError() {
    return error;
  },
  async fetchNotifications(token) {
    if (!token) return;
    loading = true;
    error = null;
    this.notify();
    try {
      const response = await getNotifications(token);
      notifications = Array.isArray(response.data?.notifications) ? response.data.notifications : [];
    } catch (err) {
      error = err.message;
      notifications = [];
    } finally {
      loading = false;
      this.notify();
    }
  },
  subscribe(listener) {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },
  notify() {
    for (const listener of listeners) {
      listener();
    }
  }
};
export default notificationStore;
