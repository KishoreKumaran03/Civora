let user = null;
let token = null;

try {
  const storedUser = localStorage.getItem('user');
  user = storedUser ? JSON.parse(storedUser) : null;
} catch {
  localStorage.removeItem('user');
}
token = localStorage.getItem('token');

const listeners = new Set();

export const authStore = {
  getUser() {
    return user;
  },
  getToken() {
    return token;
  },
  setUser(newUser) {
    user = newUser;
    if (newUser) {
      localStorage.setItem('user', JSON.stringify(newUser));
    } else {
      localStorage.removeItem('user');
    }
    this.notify();
  },
  setToken(newToken) {
    token = newToken;
    if (newToken) {
      localStorage.setItem('token', newToken);
    } else {
      localStorage.removeItem('token');
    }
    this.notify();
  },
  logout() {
    this.setUser(null);
    this.setToken(null);
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
export default authStore;
