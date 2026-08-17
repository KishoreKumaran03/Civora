import { useState } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider, useAuth } from './context/AuthContext';
import { ThemeProvider } from './context/ThemeContext';
import { AIAssistantProvider } from './context/AIAssistantContext';

// Pages
import { DashboardSummary } from './pages/Dashboard/DashboardSummary';
import { ProjectsList } from './pages/Projects/ProjectsList';
import { FavoritesList } from './pages/Projects/FavoritesList';
import { ReportsPage } from './pages/Reports/ReportsPage';
import { SettingsPage } from './pages/Settings/SettingsPage';
import { UploadColumnMappingPage } from './pages/Analytics/UploadColumnMappingPage';
import { AdvancedAnalyticsBoard } from './pages/Analytics/AdvancedAnalyticsBoard';
import { StoreDetailAnalytics } from './pages/Analytics/StoreDetailAnalytics';
import { Login } from './pages/Auth/Login';
import { Signup } from './pages/Auth/Signup';

// Layout
import { Sidebar } from './components/layout/Sidebar';
import { Header } from './components/layout/Header';
import { AskYuaChatPanel } from './components/ai/AskYuaChatPanel';

function AppRoutes() {
  const { token } = useAuth();
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false);

  return (
    <Routes>
      <Route
        path="/login"
        element={token ? <Navigate to="/" replace /> : <Login />}
      />
      <Route
        path="/signup"
        element={token ? <Navigate to="/" replace /> : <Signup />}
      />

      <Route
        path="/*"
        element={
          token ? (
            <div className={`bg-[#f8fafc] dark:bg-[#0f172a] text-slate-900 dark:text-slate-100 min-h-screen flex transition-all duration-300 ${isSidebarCollapsed ? 'sidebar-collapsed' : ''}`}>
              <Sidebar isSidebarCollapsed={isSidebarCollapsed} setIsSidebarCollapsed={setIsSidebarCollapsed} />

              <div className={`flex-1 flex flex-col min-w-0 transition-all duration-300 ${isSidebarCollapsed ? 'ml-20' : 'ml-64'}`}>
                <Header />
                <main className="flex-1 overflow-y-auto">
                  <Routes>
                    <Route path="/" element={<DashboardSummary />} />
                    <Route path="/projects" element={<ProjectsList />} />
                    <Route path="/favorites" element={<FavoritesList />} />
                    <Route path="/reports" element={<ReportsPage />} />
                    <Route path="/settings" element={<SettingsPage />} />
                    <Route path="/import/mapping/:previewId" element={<UploadColumnMappingPage />} />
                    <Route path="/advanced-analytics" element={<AdvancedAnalyticsBoard />} />
                    <Route path="/advanced-analytics/:projectId" element={<AdvancedAnalyticsBoard />} />
                    <Route path="/analytics/:projectId" element={<StoreDetailAnalytics />} />
                  </Routes>
                </main>
              </div>
              <AskYuaChatPanel />
            </div>
          ) : (
            <Navigate to="/login" replace />
          )
        }
      />
    </Routes>
  );
}

function App() {
  return (
    <AuthProvider>
      <ThemeProvider>
        <AIAssistantProvider>
          <Router>
            <AppRoutes />
          </Router>
        </AIAssistantProvider>
      </ThemeProvider>
    </AuthProvider>
  );
}

export default App;
