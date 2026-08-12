import { createContext, useContext, useState, useEffect } from 'react';
import authStore from '../store/authStore';

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [user, setUserState] = useState(() => authStore.getUser());
  const [token, setTokenState] = useState(() => authStore.getToken());

  useEffect(() => {
    const unsubscribe = authStore.subscribe(() => {
      setUserState(authStore.getUser());
      setTokenState(authStore.getToken());
    });
    return unsubscribe;
  }, []);

  const setUser = (user) => authStore.setUser(user);
  const setToken = (token) => authStore.setToken(token);
  const logout = () => authStore.logout();

  return (
    <AuthContext.Provider value={{ user, token, setUser, setToken, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
