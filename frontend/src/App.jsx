import { useState, useEffect, useRef } from 'react';
import { BrowserRouter as Router, Routes, Route, Link, useParams, useNavigate, useLocation, Navigate } from 'react-router-dom';
import axios from 'axios';
import { PieChart, Pie, Cell, ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, Legend, LineChart, Line, ComposedChart, CartesianGrid } from 'recharts';
import { ComposableMap, Geographies, Geography } from 'react-simple-maps';
import { scaleLinear } from 'd3-scale';
import { jsPDF } from 'jspdf';

const configuredApiBase = (import.meta.env.VITE_API_URL || '').replace(/\/$/, '');
const fallbackApiBases = ['', configuredApiBase, 'http://localhost:8001', 'http://localhost:8000']
  .filter((value, index, array) => value !== undefined && array.indexOf(value) === index);
let activeApiBase = configuredApiBase || '';

async function apiRequest(config) {
  const candidates = [activeApiBase, ...fallbackApiBases.filter(base => base !== activeApiBase)];
  let lastError;

  for (const base of candidates) {
    try {
      const response = await axios({
        ...config,
        url: `${base}${config.url}`,
      });
      activeApiBase = base;
      return response;
    } catch (error) {
      const status = error.response?.status;
      const shouldTryNext = !error.response || status === 404 || status >= 500;

      if (!shouldTryNext || base === candidates[candidates.length - 1]) {
        throw error;
      }

      lastError = error;
    }
  }

  throw lastError;
}

const monthMap = {
  'JAN': 'January', 'FEB': 'February', 'MAR': 'March', 'APR': 'April',
  'MAY': 'May', 'JUN': 'June', 'JUL': 'July', 'AUG': 'August',
  'SEP': 'September', 'OCT': 'October', 'NOV': 'November', 'DEC': 'December'
};
const months = Object.values(monthMap);
const years = ['2023', '2024', '2025'];
const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#f43f5e', '#10b981'];
const INDIA_GEO_URL = '/india-states-simplified.geojson';
const ADVANCED_ANALYTICS_SEED = [
  { id: 1, label: 'North Star Phones', category: 'Electronics', region: 'Tamil Nadu', month: 'March', year: 2024, revenue: 125000, cost: 76000, quantity: 14, recordedAt: '2024-03-01T09:30:00' },
  { id: 2, label: 'Kerala Living Set', category: 'Furniture', region: 'Kerala', month: 'March', year: 2024, revenue: 84000, cost: 47000, quantity: 7, recordedAt: '2024-03-01T11:00:00' },
  { id: 3, label: 'Metro Smart Hub', category: 'Electronics', region: 'NCT of Delhi', month: 'March', year: 2024, revenue: 98000, cost: 59000, quantity: 10, recordedAt: '2024-03-01T13:15:00' },
  { id: 4, label: 'Odisha Decor Pack', category: 'Furniture', region: 'Odisha', month: 'March', year: 2024, revenue: 61000, cost: 33000, quantity: 9, recordedAt: '2024-03-01T15:10:00' },
  { id: 5, label: 'Bengaluru Sound Grid', category: 'Electronics', region: 'Karnataka', month: 'March', year: 2024, revenue: 142000, cost: 87000, quantity: 16, recordedAt: '2024-03-01T17:45:00' },
  { id: 6, label: 'South Essentials', category: 'Others', region: 'Tamil Nadu', month: 'March', year: 2024, revenue: 52000, cost: 26000, quantity: 11, recordedAt: '2024-03-02T09:05:00' }
];
const MONTH_ORDER = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const ANALYTICS_X_AXIS_OPTIONS = [
  { value: 'month', label: 'Month' },
  { value: 'category', label: 'Category' },
  { value: 'region', label: 'Region' },
  { value: 'year', label: 'Year' },
  { value: 'label', label: 'Product' }
];
const ANALYTICS_Y_AXIS_OPTIONS = [
  { value: 'revenue', label: 'Revenue' },
  { value: 'cost', label: 'Cost' },
  { value: 'quantity', label: 'Quantity' },
  { value: 'profit', label: 'Profit' }
];
const ANALYTICS_TIME_WINDOW_OPTIONS = [
  { value: 'all', label: 'All Data' },
  { value: '3', label: 'Last 3 Months' },
  { value: '6', label: 'Last 6 Months' },
  { value: '12', label: 'Last 12 Months' },
  { value: 'year', label: 'Latest Year' },
];
const ANALYTICS_PROJECTION_WINDOW_OPTIONS = [
  { value: '3', label: '3 Months' },
  { value: '6', label: '6 Months' },
  { value: '12', label: '12 Months' },
];

function getSequentialBatchPeriod(startMonth, startYear, offset) {
  const initialMonthIndex = Math.max(0, MONTH_ORDER.indexOf(startMonth));
  const initialYear = Number.parseInt(startYear, 10) || new Date().getFullYear();
  const totalMonthIndex = initialMonthIndex + offset;
  const monthIndex = ((totalMonthIndex % 12) + 12) % 12;
  const yearOffset = Math.floor(totalMonthIndex / 12);

  return {
    month: MONTH_ORDER[monthIndex],
    year: String(initialYear + yearOffset),
  };
}

function buildBatchItems(files, startMonth, startYear, startIndex = 0) {
  return files.map((file, index) => {
    const period = getSequentialBatchPeriod(startMonth, startYear, startIndex + index);
    return {
      id: `${Date.now()}-${startIndex + index}-${file.name}`,
      file,
      name: file.name,
      month: period.month,
      year: period.year,
    };
  });
}
const MAP_STATE_NAME_ALIASES = {
  Delhi: 'NCT of Delhi',
  'New Delhi': 'NCT of Delhi',
  Pondicherry: 'Puducherry',
  Kerla: 'Kerala',
  Tamilnadu: 'Tamil Nadu',
  'Tamil Nadu ': 'Tamil Nadu',
  Chhatisgarh: 'Chhattisgarh',
  Chattisgarh: 'Chhattisgarh',
  UP: 'Uttar Pradesh',
  'U.P.': 'Uttar Pradesh',
  'Andra Pradesh': 'Andhra Pradesh',
};
const FAVORITE_PROJECTS_STORAGE_KEY = 'favoriteProjectIds';

function getStoredFavoriteProjectIds() {
  try {
    const parsed = JSON.parse(localStorage.getItem(FAVORITE_PROJECTS_STORAGE_KEY) || '[]');
    return Array.isArray(parsed) ? parsed.map((value) => String(value)) : [];
  } catch {
    return [];
  }
}

function setStoredFavoriteProjectIds(projectIds) {
  localStorage.setItem(FAVORITE_PROJECTS_STORAGE_KEY, JSON.stringify(projectIds.map((value) => String(value))));
}

function formatInrCompact(value) {
  const amount = Number(value || 0);
  if (amount >= 10000000) {
    return `Rs ${(amount / 10000000).toFixed(2)} Cr`;
  }
  if (amount >= 100000) {
    return `Rs ${(amount / 100000).toFixed(1)} L`;
  }

  return `Rs ${amount.toLocaleString('en-IN', { maximumFractionDigits: 0 })}`;
}

function formatMetricValue(metric, value) {
  if (metric === 'quantity') {
    return Number(value || 0).toLocaleString('en-IN');
  }

  return formatInrCompact(value);
}

function normalizeMapStateName(value) {
  const text = String(value || '').trim();
  return MAP_STATE_NAME_ALIASES[text] || text;
}

// Main Application Component with Router
function App() {
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false);
  const [user, setUser] = useState(() => {
    try {
      const storedUser = localStorage.getItem('user');
      return storedUser ? JSON.parse(storedUser) : null;
    } catch {
      localStorage.removeItem('user');
      return null;
    }
  });
  const [token, setToken] = useState(() => localStorage.getItem('token'));
  const [darkMode, setDarkMode] = useState(false);

  useEffect(() => {
    if (darkMode) document.documentElement.classList.add('dark');
    else document.documentElement.classList.remove('dark');
  }, [darkMode]);

  const logout = () => {
    localStorage.removeItem('user');
    localStorage.removeItem('token');
    setUser(null);
    setToken(null);
  };

  return (
    <Router>
      <Routes>
        <Route
          path="/login"
          element={token ? <Navigate to="/" replace /> : <Login setUser={setUser} setToken={setToken} />}
        />
        <Route
          path="/signup"
          element={token ? <Navigate to="/" replace /> : <Signup />}
        />
        
        <Route path="/*" element={
          token ? (
            <div className={`bg-[#f8fafc] dark:bg-[#0f172a] text-slate-900 dark:text-slate-100 min-h-screen flex transition-all duration-300 ${isSidebarCollapsed ? 'sidebar-collapsed' : ''}`}>
              
              {/* Sidebar: Analytics Pro Legacy Style */}
              <aside className={`${isSidebarCollapsed ? 'w-20' : 'w-64'} border-r border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 flex flex-col fixed h-full z-20 transition-all duration-300 shadow-2xl`}>
                <button
                  onClick={() => setIsSidebarCollapsed(!isSidebarCollapsed)}
                  className="sidebar-toggle absolute top-1/2 right-[-28px] z-30 flex h-16 w-16 -translate-y-1/2 items-center justify-center text-slate-500 transition-all duration-300 hover:text-primary dark:text-slate-300"
                  aria-label={isSidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
                  title={isSidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
                >
                  <span className="sidebar-toggle__inner flex h-14 w-14 items-center justify-center rounded-full bg-white text-slate-500 shadow-[0_10px_28px_rgba(15,23,42,0.12)] ring-1 ring-slate-200/80 transition-all duration-300 dark:bg-slate-900 dark:text-slate-300 dark:ring-slate-700">
                    <span className="material-symbols-outlined text-[24px]">
                      {isSidebarCollapsed ? 'chevron_right' : 'chevron_left'}
                    </span>
                  </span>
                </button>
                <div className="p-6 flex items-center">
                   <div className="flex items-center gap-3 overflow-hidden">
                      <div className="size-8 text-primary shrink-0">
                        <span className="material-symbols-outlined text-3xl font-black">finance_mode</span>
                      </div>
                      {!isSidebarCollapsed && <span className="text-xl font-bold tracking-tight text-primary font-display uppercase whitespace-nowrap">Analytics Pro</span>}
                   </div>
                </div>
                
                <nav className="flex-1 px-4 py-4 space-y-6 overflow-y-auto no-scrollbar">
                   <button className={`bg-indigo-50 dark:bg-indigo-900/20 text-indigo-600 dark:text-indigo-400 rounded-xl font-semibold border border-indigo-100 dark:border-indigo-800 transition-all ui-hover shadow-sm ${isSidebarCollapsed ? 'mx-auto flex h-12 w-12 items-center justify-center' : 'w-full flex items-center gap-3 p-3'}`}>
                      <span className="material-symbols-outlined text-indigo-500">auto_awesome</span>
                      {!isSidebarCollapsed && <span>Ask Yua AI</span>}
                   </button>
                   
                   <div className="space-y-1">
                      <NavLink to="/" icon="dashboard" label="Dashboard" isCollapsed={isSidebarCollapsed} />
                      <NavLink to="/favorites" icon="star" label="Favorites" isCollapsed={isSidebarCollapsed} />
                   </div>

                   <div className="space-y-1">
                      {!isSidebarCollapsed && <h3 className="px-3 text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-[2px] mb-2 mt-4">MY DATA</h3>}
                      <NavLink to="/projects" icon="folder_open" label="My Projects" isCollapsed={isSidebarCollapsed} />
                   </div>

                   <div className="space-y-1">
                      {!isSidebarCollapsed && <h3 className="px-3 text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-[2px] mb-2 mt-4">ANALYSIS</h3>}
                      <NavLink to="/advanced-analytics" icon="insights" label="Advanced Analytics" isCollapsed={isSidebarCollapsed} />
                      <NavLink to="/reports" icon="description" label="Reports" isCollapsed={isSidebarCollapsed} />
                   </div>
                   
                </nav>
                
                <div className="p-4 border-t border-slate-200 dark:border-slate-800">
                   <NavLink to="/settings" icon="settings" label="Settings" isCollapsed={isSidebarCollapsed} />
                </div>
              </aside>

              {/* Main Workspace */}
              <div className={`flex-1 flex flex-col min-w-0 transition-all duration-300 ${isSidebarCollapsed ? 'ml-20' : 'ml-64'}`}>
                <Header user={user} setDarkMode={setDarkMode} darkMode={darkMode} logout={logout} />
                <main className="flex-1 overflow-y-auto">
                   <Routes>
                     <Route path="/" element={<DashboardSummary />} />
                     <Route path="/projects" element={<ProjectsList />} />
                     <Route path="/favorites" element={<FavoritesList />} />
                     <Route path="/reports" element={<ReportsPage />} />
                     <Route path="/settings" element={<SettingsPage user={user} />} />
                     <Route path="/advanced-analytics" element={<AdvancedAnalyticsBoard />} />
                     <Route path="/advanced-analytics/:projectId" element={<AdvancedAnalyticsBoard />} />
                     <Route path="/analytics/:projectId" element={<StoreDetailAnalytics />} />
                   </Routes>
                </main>
              </div>
            </div>
          ) : <Navigate to="/login" />
        } />
      </Routes>
    </Router>
  );
}

// Nav Link Component
function NavLink({ to, icon, label, isCollapsed }) {
  const { pathname } = useLocation();
  const isActive = (to === '/' ? pathname === '/' : pathname.startsWith(to)) && to !== '#';
  return (
    <Link to={to} className={`flex items-center gap-3 p-3 rounded-xl transition-all ${isActive ? 'bg-primary/10 text-primary font-bold shadow-sm' : 'text-slate-500 dark:text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-800'} ${isCollapsed ? 'justify-center mx-2' : ''}`}>
      <span className="material-symbols-outlined text-[22px]">{icon}</span>
      {!isCollapsed && <span className="text-sm tracking-tight">{label}</span>}
    </Link>
  );
}

function Header({ user, setDarkMode, darkMode, logout }) {
  const location = useLocation();
  const navigate = useNavigate();
  const token = localStorage.getItem('token');
  const [isExportOpen, setIsExportOpen] = useState(false);
  const [isImportDialogOpen, setIsImportDialogOpen] = useState(false);
  const [isNotifOpen, setIsNotifOpen] = useState(false);
  const [isProfileOpen, setIsProfileOpen] = useState(false);
  const [notifications, setNotifications] = useState([]);
  const [profileData, setProfileData] = useState(null);
  const [selectedMonth, setSelectedMonth] = useState('January');
  const [selectedYear, setSelectedYear] = useState('2024');
  const [selectedFile, setSelectedFile] = useState(null);
  const [importMode, setImportMode] = useState('single');
  const [batchFiles, setBatchFiles] = useState([]);
  const [isUploading, setIsUploading] = useState(false);
  const [openImportMenu, setOpenImportMenu] = useState(null);
  const [availableProjects, setAvailableProjects] = useState([]);
  const [selectedImportProjectId, setSelectedImportProjectId] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [isSearchOpen, setIsSearchOpen] = useState(false);
  const fileInputRef = useRef(null);
  const notifRef = useRef(null);
  const profileRef = useRef(null);
  const exportRef = useRef(null);
  const searchRef = useRef(null);
  const activeProjectId = (location.pathname.match(/^\/advanced-analytics\/(\d+)/) || [])[1] || '';
  const normalizedSearch = searchQuery.trim().toLowerCase();
  const searchMatches = normalizedSearch
    ? availableProjects.filter((project) => String(project.name || '').toLowerCase().includes(normalizedSearch)).slice(0, 6)
    : [];

  useEffect(() => {
    apiRequest({ method: 'get', url: '/api/projects', headers: { 'Authorization': `Bearer ${token}` } })
      .then((response) => {
        const projects = Array.isArray(response.data) ? response.data : [];
        setAvailableProjects(projects);
        if (activeProjectId) setSelectedImportProjectId(activeProjectId);
        else if (projects.length > 0) setSelectedImportProjectId(String(projects[0].id));
      })
      .catch((error) => console.error('Error fetching projects for import:', error));
  }, [activeProjectId, token]);

  useEffect(() => {
    apiRequest({ method: 'get', url: '/api/notifications', headers: { 'Authorization': `Bearer ${token}` } })
      .then((res) => setNotifications(Array.isArray(res.data?.notifications) ? res.data.notifications : []))
      .catch(() => setNotifications([]));
  }, [token]);

  useEffect(() => {
    if (isProfileOpen && !profileData) {
      apiRequest({ method: 'get', url: '/api/user/profile', headers: { 'Authorization': `Bearer ${token}` } })
        .then((res) => setProfileData(res.data))
        .catch(() => {});
    }
  }, [isProfileOpen, profileData, token]);

  // Close dropdowns on outside click
  useEffect(() => {
    const handler = (e) => {
      if (notifRef.current && !notifRef.current.contains(e.target)) setIsNotifOpen(false);
      if (profileRef.current && !profileRef.current.contains(e.target)) setIsProfileOpen(false);
      if (exportRef.current && !exportRef.current.contains(e.target)) setIsExportOpen(false);
      if (searchRef.current && !searchRef.current.contains(e.target)) setIsSearchOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const handleFileSelect = (e) => { const file = e.target.files[0]; if (!file) return; setSelectedFile(file); };
  const openImportDialog = () => setIsImportDialogOpen(true);
  const resetImportDialog = () => {
    setIsImportDialogOpen(false); setSelectedFile(null); setBatchFiles([]); setImportMode('single'); setIsUploading(false); setOpenImportMenu(null);
    if (fileInputRef.current) fileInputRef.current.value = '';
  };

  const handleBatchFileSelect = (event) => {
    const files = Array.from(event.target.files || []);
    if (!files.length) return;
    setBatchFiles((currentFiles) => {
      const nextItems = buildBatchItems(files, selectedMonth, selectedYear, currentFiles.length);
      return [...currentFiles, ...nextItems];
    });
  };

  useEffect(() => {
    if (importMode !== 'batch' || batchFiles.length === 0) return;

    setBatchFiles((currentFiles) => currentFiles.map((item, index) => ({
      ...item,
      ...getSequentialBatchPeriod(selectedMonth, selectedYear, index),
    })));
  }, [batchFiles.length, importMode, selectedMonth, selectedYear]);

  const openProjectFromSearch = (project) => {
    setSearchQuery(project.name || '');
    setIsSearchOpen(false);
    navigate(`/advanced-analytics/${project.id}`, { state: { projectName: project.name } });
  };

  const handleSearchSubmit = (event) => {
    if (event) event.preventDefault();
    if (!normalizedSearch) return;

    const exactMatch = availableProjects.find((project) => String(project.name || '').toLowerCase() === normalizedSearch);
    const firstPartialMatch = searchMatches[0];
    const targetProject = exactMatch || firstPartialMatch;

    if (targetProject) {
      openProjectFromSearch(targetProject);
      return;
    }

    alert('No matching project found. Try searching by store name like "Kannan Stores".');
  };

  const handleUpload = async () => {
    if (isUploading) return;
    if (!selectedImportProjectId) { alert('Select a project before importing data.'); return; }

    if (importMode === 'single' && !selectedFile) return;
    if (importMode === 'batch' && batchFiles.length === 0) return;

    try {
      setIsUploading(true);
      if (importMode === 'single') {
        const formData = new FormData();
        formData.append('file', selectedFile);
        formData.append('project_id', selectedImportProjectId);
        formData.append('month', selectedMonth);
        formData.append('year', selectedYear);
        await apiRequest({ method: 'post', url: '/api/upload', data: formData, headers: { 'Authorization': `Bearer ${token}` } });
      } else {
        for (const batchItem of batchFiles) {
          const formData = new FormData();
          formData.append('file', batchItem.file);
          formData.append('project_id', selectedImportProjectId);
          formData.append('month', batchItem.month);
          formData.append('year', batchItem.year);
          await apiRequest({ method: 'post', url: '/api/upload', data: formData, headers: { 'Authorization': `Bearer ${token}` } });
        }
      }
      resetImportDialog();
      const targetProject = availableProjects.find((p) => String(p.id) === String(selectedImportProjectId));
      alert(importMode === 'single' ? "Successful! Opening the selected project's analytics." : `Successful! Imported ${batchFiles.length} month file(s). Opening project analytics.`);
      navigate(`/advanced-analytics/${selectedImportProjectId}`, { state: { projectName: targetProject?.name || `Project ${selectedImportProjectId}` } });
      window.location.reload();
    } catch (err) { setIsUploading(false); alert("Upload failed: " + (err.response?.data?.error || err.message)); }
  };

  const handleExportPdf = () => {
    const doc = new jsPDF();
    doc.setFontSize(20); doc.text('Analytics Pro - Dashboard Report', 20, 20);
    doc.setFontSize(12); doc.text(`Generated: ${new Date().toLocaleDateString()}`, 20, 35);
    doc.text(`User: ${user?.name || 'Administrator'}`, 20, 45);
    doc.text('Export your project reports from the Reports page for full detail.', 20, 60);
    doc.save('analytics_dashboard_report.pdf');
    setIsExportOpen(false);
  };

  const handleExportCsv = async () => {
    try {
      const res = await apiRequest({ method: 'get', url: '/api/projects', headers: { 'Authorization': `Bearer ${token}` } });
      const projects = Array.isArray(res.data) ? res.data : [];
      const rows = [['Project Name', 'Store Type', 'Currency', 'Timezone', 'Contact']];
      projects.forEach(p => rows.push([p.name, p.store_type || '', p.currency_code || '', p.timezone || '', p.contact_number || '']));
      const csv = rows.map(r => r.map(v => `"${String(v).replace(/"/g, '""')}"`).join(',')).join('\n');
      const blob = new Blob([csv], { type: 'text/csv' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a'); a.href = url; a.download = 'projects_export.csv'; a.click();
      URL.revokeObjectURL(url);
    } catch (e) { alert('Export failed: ' + e.message); }
    setIsExportOpen(false);
  };

  const unreadCount = notifications.length;
  const notifSeverityIcon = { error: 'error', warning: 'warning', info: 'info', low_stock: 'inventory_2', declining_sales: 'trending_down', market_threat: 'shield', business_news: 'newspaper' };
  const notifSeverityColor = { error: 'text-rose-500 bg-rose-50', warning: 'text-amber-500 bg-amber-50', info: 'text-sky-500 bg-sky-50' };

  return (
    <>
      <header className="h-20 bg-white/80 dark:bg-slate-900/80 backdrop-blur-md border-b border-slate-200 dark:border-slate-800 flex items-center justify-between px-8 sticky top-0 z-30">
        <div className="flex-1 max-w-xl relative" ref={searchRef}>
           <span className="material-symbols-outlined absolute left-4 top-1/2 -translate-y-1/2 text-indigo-500 text-lg">auto_awesome</span>
           <form onSubmit={handleSearchSubmit}>
             <input
               className="w-full pl-12 pr-16 py-3 bg-slate-100 dark:bg-slate-800 border-none rounded-2xl focus:ring-2 focus:ring-primary/20 transition-all text-sm outline-none placeholder:text-slate-400 font-medium"
               placeholder="Search projects (e.g., Kannan Stores)"
               type="text"
               value={searchQuery}
               onChange={(event) => {
                 setSearchQuery(event.target.value);
                 setIsSearchOpen(true);
               }}
               onFocus={() => setIsSearchOpen(true)}
             />
           </form>
           {isSearchOpen && (normalizedSearch || searchMatches.length > 0) && (
             <div className="absolute left-0 right-0 top-full mt-2 overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-2xl dark:border-slate-700 dark:bg-slate-900">
               {searchMatches.length > 0 ? (
                 searchMatches.map((project) => (
                   <button
                     key={project.id}
                     type="button"
                     onClick={() => openProjectFromSearch(project)}
                     className="flex w-full items-center justify-between px-4 py-3 text-left text-sm font-semibold text-slate-700 transition-colors hover:bg-slate-50 dark:text-slate-200 dark:hover:bg-slate-800"
                   >
                     <span className="truncate">{project.name}</span>
                     <span className="material-symbols-outlined text-base text-slate-300">arrow_forward</span>
                   </button>
                 ))
               ) : (
                 <div className="px-4 py-3 text-sm font-medium text-slate-500 dark:text-slate-300">No matching project found.</div>
               )}
             </div>
           )}
        </div>
        <div className="flex items-center gap-4">
          <button onClick={() => setDarkMode(!darkMode)} className="p-2.5 text-gray-400 hover:bg-gray-100 dark:hover:bg-slate-800 rounded-xl transition-all">
             <span className="material-symbols-outlined">{darkMode ? 'light_mode' : 'dark_mode'}</span>
          </button>

          {/* Notifications */}
          <div className="relative" ref={notifRef}>
            <button onClick={() => { setIsNotifOpen(!isNotifOpen); setIsProfileOpen(false); setIsExportOpen(false); }} className="p-2.5 text-gray-400 hover:bg-gray-100 dark:hover:bg-slate-800 rounded-xl transition-all relative">
               <span className="material-symbols-outlined">notifications</span>
               {unreadCount > 0 && <span className="absolute top-1.5 right-1.5 min-w-[18px] h-[18px] bg-rose-500 rounded-full border-2 border-white dark:border-slate-900 text-white text-[9px] font-black flex items-center justify-center px-0.5">{unreadCount > 9 ? '9+' : unreadCount}</span>}
            </button>
            {isNotifOpen && (
              <div className="absolute right-0 top-full mt-3 w-96 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-3xl shadow-2xl z-50 overflow-hidden">
                <div className="px-6 py-4 border-b border-slate-100 dark:border-slate-800 flex items-center justify-between">
                  <div>
                    <div className="font-black text-slate-900 dark:text-white">Alerts & Notifications</div>
                    <div className="text-[10px] font-black text-slate-400 uppercase tracking-widest mt-0.5">{unreadCount} active alerts</div>
                  </div>
                  <span className="material-symbols-outlined text-slate-300">notifications_active</span>
                </div>
                <div className="max-h-[420px] overflow-y-auto divide-y divide-slate-50 dark:divide-slate-800">
                  {notifications.length === 0 ? (
                    <div className="px-6 py-10 text-center text-slate-400 text-sm font-bold">No alerts right now</div>
                  ) : notifications.map((n) => {
                    const iconName = notifSeverityIcon[n.type] || notifSeverityIcon[n.severity] || 'info';
                    const colorClass = notifSeverityColor[n.severity] || 'text-slate-500 bg-slate-50';
                    return (
                      <div key={n.id} className="px-6 py-4 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors">
                        <div className="flex items-start gap-3">
                          <div className={`mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-xl ${colorClass}`}>
                            <span className="material-symbols-outlined text-base">{iconName}</span>
                          </div>
                          <div className="flex-1 min-w-0">
                            <div className="text-sm font-black text-slate-900 dark:text-white">{n.title}</div>
                            <div className="text-xs font-medium text-slate-500 dark:text-slate-400 mt-0.5 leading-relaxed">{n.message}</div>
                            <div className="text-[10px] font-black text-slate-300 uppercase tracking-widest mt-1">{new Date(n.timestamp).toLocaleDateString()}</div>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
          </div>

          <button onClick={openImportDialog} className="bg-primary text-white px-6 py-2.5 rounded-xl font-bold flex items-center gap-2 hover:shadow-lg hover:shadow-primary/30 transition-all ui-hover">
             <span className="material-symbols-outlined text-sm">add</span> Import Data
          </button>
          <input
            type="file"
            ref={fileInputRef}
            onChange={importMode === 'single' ? handleFileSelect : handleBatchFileSelect}
            className="hidden"
            accept=".csv,.xlsx,.xls"
            multiple={importMode === 'batch'}
          />
          
          {/* Export */}
          <div className="relative" ref={exportRef}>
             <button onClick={() => { setIsExportOpen(!isExportOpen); setIsNotifOpen(false); setIsProfileOpen(false); }} className="text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-200 px-4 py-2.5 rounded-xl font-bold flex items-center gap-2 hover:bg-slate-100 dark:hover:bg-slate-800 transition-all">
                <span className="material-symbols-outlined text-sm">download</span> Export
             </button>
             {isExportOpen && (
               <div className="absolute right-0 top-full mt-2 w-64 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-2xl shadow-2xl z-50 overflow-hidden ring-1 ring-black/5">
                  <button onClick={handleExportPdf} className="w-full text-left px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-700 flex items-center gap-4 transition-colors">
                    <div className="p-2 rounded-lg bg-slate-50 dark:bg-slate-800 text-rose-500"><span className="material-symbols-outlined text-base leading-none">picture_as_pdf</span></div>
                    <div><div className="text-sm font-bold text-slate-900 dark:text-white">PDF Report</div><div className="text-[10px] text-slate-400 font-medium">Dashboard summary</div></div>
                  </button>
                  <button onClick={handleExportCsv} className="w-full text-left px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-700 flex items-center gap-4 transition-colors">
                    <div className="p-2 rounded-lg bg-slate-50 dark:bg-slate-800 text-emerald-500"><span className="material-symbols-outlined text-base leading-none">table_chart</span></div>
                    <div><div className="text-sm font-bold text-slate-900 dark:text-white">Excel CSV</div><div className="text-[10px] text-slate-400 font-medium">Projects raw data</div></div>
                  </button>
                  <button onClick={() => { navigate('/reports'); setIsExportOpen(false); }} className="w-full text-left px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-700 flex items-center gap-4 transition-colors bg-indigo-50/30 dark:bg-indigo-950/20">
                    <div className="p-2 rounded-lg bg-slate-50 dark:bg-slate-800 text-indigo-600"><span className="material-symbols-outlined text-base leading-none">auto_awesome</span></div>
                    <div><div className="text-sm font-bold text-slate-900 dark:text-white flex items-center gap-2">Full Reports <span className="bg-indigo-100 text-indigo-600 text-[8px] px-1.5 py-0.5 rounded font-black">PDF</span></div><div className="text-[10px] text-slate-400 font-medium">Per-store SWOT analysis</div></div>
                  </button>
               </div>
             )}
          </div>

          {/* User Profile */}
          <div className="relative border-l border-slate-200 dark:border-slate-800 pl-4" ref={profileRef}>
            <button onClick={() => { setIsProfileOpen(!isProfileOpen); setIsNotifOpen(false); setIsExportOpen(false); }} className="flex items-center gap-3 hover:bg-slate-50 dark:hover:bg-slate-800 rounded-2xl px-3 py-2 transition-all">
               <div className="text-right hidden xl:block">
                  <div className="text-sm font-black text-slate-900 dark:text-white leading-none">{user?.name || 'User'}</div>
                  <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mt-1">{user?.position || 'Administrator'}</div>
               </div>
               <div className="w-10 h-10 rounded-full bg-primary/10 flex items-center justify-center text-primary font-black overflow-hidden border-2 border-primary/20">
                  {user?.profile_picture ? <img src={user.profile_picture} alt="avatar" className="w-full h-full object-cover" /> : (user?.name?.charAt(0) || 'U')}
               </div>
               <span className="material-symbols-outlined text-slate-400 text-base hidden xl:block">{isProfileOpen ? 'expand_less' : 'expand_more'}</span>
            </button>

            {isProfileOpen && (
              <div className="absolute right-0 top-full mt-3 w-80 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-3xl shadow-2xl z-50 overflow-hidden">
                {/* Profile header */}
                <div className="px-6 py-5 bg-gradient-to-br from-primary/10 to-indigo-50 dark:from-primary/20 dark:to-slate-800 border-b border-slate-100 dark:border-slate-800">
                  <div className="flex items-center gap-4">
                    <div className="w-14 h-14 rounded-2xl bg-primary/10 border-2 border-primary/20 flex items-center justify-center text-primary font-black text-xl overflow-hidden">
                      {(profileData?.user || user)?.profile_picture ? <img src={(profileData?.user || user).profile_picture} alt="avatar" className="w-full h-full object-cover" /> : ((profileData?.user?.name || user?.name || 'U').charAt(0))}
                    </div>
                    <div>
                      <div className="font-black text-slate-900 dark:text-white text-base">{profileData?.user?.name || user?.name || 'User'}</div>
                      <div className="text-xs font-bold text-slate-500 dark:text-slate-400">{profileData?.user?.email || user?.email || ''}</div>
                      <div className="mt-1 inline-flex items-center gap-1 bg-primary/10 text-primary text-[9px] font-black uppercase tracking-widest px-2 py-0.5 rounded-full">{profileData?.user?.position || user?.position || 'Administrator'}</div>
                    </div>
                  </div>
                </div>

                {/* Stores owned */}
                <div className="px-6 py-4">
                  <div className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400 mb-3">Stores Owned</div>
                  {profileData?.stores?.length > 0 ? (
                    <div className="space-y-2 max-h-40 overflow-y-auto">
                      {profileData.stores.map((store) => (
                        <button key={store.id} onClick={() => { navigate(`/advanced-analytics/${store.id}`, { state: { projectName: store.name } }); setIsProfileOpen(false); }} className="w-full flex items-center gap-3 p-3 rounded-2xl hover:bg-slate-50 dark:hover:bg-slate-800 transition-colors text-left">
                          <div className="w-8 h-8 rounded-xl bg-primary/10 flex items-center justify-center text-primary shrink-0"><span className="material-symbols-outlined text-base">storefront</span></div>
                          <div className="min-w-0">
                            <div className="text-sm font-black text-slate-900 dark:text-white truncate">{store.name}</div>
                            <div className="text-[10px] font-bold text-slate-400 uppercase">{store.store_type || 'General'}</div>
                          </div>
                          <span className="material-symbols-outlined text-slate-300 text-base ml-auto shrink-0">arrow_forward</span>
                        </button>
                      ))}
                    </div>
                  ) : (
                    <div className="text-sm text-slate-400 font-medium py-2">No stores yet</div>
                  )}
                </div>

                <div className="px-6 pb-4 border-t border-slate-100 dark:border-slate-800 pt-3">
                  <button onClick={() => { navigate('/settings'); setIsProfileOpen(false); }} className="w-full flex items-center gap-3 p-3 rounded-2xl hover:bg-slate-50 dark:hover:bg-slate-800 transition-colors text-left">
                    <span className="material-symbols-outlined text-slate-400 text-base">settings</span>
                    <span className="text-sm font-bold text-slate-700 dark:text-slate-200">Account Settings</span>
                  </button>
                  <button onClick={() => { setIsProfileOpen(false); logout(); }} className="mt-2 w-full flex items-center gap-3 p-3 rounded-2xl text-left text-red-500 hover:bg-red-50 dark:hover:bg-red-950/30 transition-colors">
                    <span className="material-symbols-outlined text-base">logout</span>
                    <span className="text-sm font-bold">Logout Session</span>
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      </header>

      {isImportDialogOpen && (
        <div className="fixed inset-0 z-[100] flex items-start justify-center overflow-y-auto p-4 pt-8 md:items-center md:p-6">
          <div className="absolute inset-0 bg-slate-950/55 backdrop-blur-sm transition-opacity duration-300 animate-in fade-in" onClick={resetImportDialog}></div>
          <div className="relative max-h-[calc(100vh-4rem)] overflow-y-auto p-6 md:max-h-[min(85vh,900px)] md:p-10">
              <div className="mb-8 flex items-center gap-4">
                <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-white text-indigo-600 shadow-sm ring-1 ring-indigo-100 dark:bg-slate-800 dark:ring-slate-700"><span className="material-symbols-outlined text-3xl">upload_file</span></div>
                <div>
                  <h3 className="text-2xl font-black tracking-tighter">Import Data</h3>
                  <p className="text-sm text-slate-500 dark:text-slate-400">Import one file or multiple month files in one run.</p>
                </div>
              </div>

              <div className="mb-6 inline-flex rounded-2xl border border-slate-200 bg-slate-50 p-1 dark:border-slate-700 dark:bg-slate-800">
                <button
                  type="button"
                  onClick={() => setImportMode('single')}
                  className={`rounded-xl px-4 py-2 text-xs font-black uppercase tracking-[0.2em] transition-all ${importMode === 'single' ? 'bg-white text-primary shadow-sm dark:bg-slate-900' : 'text-slate-500'}`}
                >
                  Single Month
                </button>
                <button
                  type="button"
                  onClick={() => setImportMode('batch')}
                  className={`rounded-xl px-4 py-2 text-xs font-black uppercase tracking-[0.2em] transition-all ${importMode === 'batch' ? 'bg-white text-primary shadow-sm dark:bg-slate-900' : 'text-slate-500'}`}
                >
                  Multi-Month Batch
                </button>
              </div>

              <div className={`grid gap-5 ${importMode === 'single' ? 'md:grid-cols-2 xl:grid-cols-3' : 'md:grid-cols-1 xl:grid-cols-1'}`}>
                <ImportDropdown
                  label="Project"
                  value={availableProjects.find((project) => String(project.id) === String(selectedImportProjectId))?.name || 'Select project'}
                  options={availableProjects.map((project) => ({ value: String(project.id), label: project.name }))}
                  isOpen={openImportMenu === 'project'}
                  onToggle={() => setOpenImportMenu(openImportMenu === 'project' ? null : 'project')}
                  onSelect={(projectId) => {
                    setSelectedImportProjectId(projectId);
                    setOpenImportMenu(null);
                  }}
                />

                {(importMode === 'single' || importMode === 'batch') && (
                  <>
                    <ImportDropdown
                      label={importMode === 'single' ? 'Month' : 'Start Month'}
                      value={selectedMonth}
                      options={months.map((month) => ({ value: month, label: month }))}
                      isOpen={openImportMenu === 'month'}
                      onToggle={() => setOpenImportMenu(openImportMenu === 'month' ? null : 'month')}
                      onSelect={(month) => {
                        setSelectedMonth(month);
                        setOpenImportMenu(null);
                      }}
                    />

                    <ImportDropdown
                      label={importMode === 'single' ? 'Year' : 'Start Year'}
                      value={selectedYear}
                      options={years.map((year) => ({ value: year, label: year }))}
                      isOpen={openImportMenu === 'year'}
                      onToggle={() => setOpenImportMenu(openImportMenu === 'year' ? null : 'year')}
                      onSelect={(year) => {
                        setSelectedYear(year);
                        setOpenImportMenu(null);
                      }}
                    />
                  </>
                )}
              </div>

              <div className="mt-6 rounded-[1.75rem] border border-dashed border-slate-300 bg-slate-50/80 p-5 dark:border-slate-700 dark:bg-slate-800/50">
                <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
                  <div>
                    <p className="text-sm font-black text-slate-900 dark:text-white">Data file</p>
                    <p className="mt-1 text-sm text-slate-500 dark:text-slate-400">
                      {importMode === 'single'
                        ? (selectedFile ? selectedFile.name : 'Upload a CSV or Excel file for the selected reporting period.')
                        : batchFiles.length > 0
                          ? `${batchFiles.length} file(s) queued. Months will auto-increment from ${selectedMonth} ${selectedYear}.`
                          : `Choose files once, then we'll auto-assign months starting from ${selectedMonth} ${selectedYear}.`}
                    </p>
                  </div>
                  <button
                    type="button"
                    onClick={() => fileInputRef.current?.click()}
                    className="inline-flex items-center justify-center gap-2 rounded-2xl border border-slate-200 bg-white px-5 py-3 font-bold text-slate-700 shadow-sm transition-all hover:-translate-y-0.5 hover:border-slate-300 hover:text-slate-900 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:border-slate-600 dark:hover:text-white"
                  >
                    <span className="material-symbols-outlined text-sm">attach_file</span>
                    {importMode === 'single' ? (selectedFile ? 'Change File' : 'Choose File') : (batchFiles.length > 0 ? 'Add / Change Files' : 'Choose Files')}
                  </button>
                </div>
              </div>

              {importMode === 'batch' && batchFiles.length > 0 && (
                <div className="mt-6 overflow-hidden rounded-2xl border border-slate-200 dark:border-slate-700">
                  <div className="grid grid-cols-[1.6fr_1fr_1fr] bg-slate-50 px-4 py-3 text-[10px] font-black uppercase tracking-[0.2em] text-slate-500 dark:bg-slate-800/70 dark:text-slate-300">
                    <span>File</span>
                    <span>Month</span>
                    <span>Year</span>
                  </div>
                  <div className="max-h-56 overflow-y-auto bg-white dark:bg-slate-900">
                    {batchFiles.map((batchItem, index) => (
                      <div key={batchItem.id} className="grid grid-cols-[1.6fr_1fr_1fr] items-center gap-3 border-t border-slate-100 px-4 py-3 dark:border-slate-800">
                        <div className="min-w-0">
                          <span className="truncate text-sm font-bold text-slate-700 dark:text-slate-200">{batchItem.name}</span>
                          <div className="mt-1 text-[11px] font-medium text-slate-400 dark:text-slate-500">File {index + 1}</div>
                        </div>
                        <div className="inline-flex h-10 items-center rounded-xl border border-slate-200 bg-slate-50 px-3 text-sm font-semibold text-slate-700 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100">
                          {batchItem.month}
                        </div>
                        <div className="inline-flex h-10 items-center rounded-xl border border-slate-200 bg-slate-50 px-3 text-sm font-semibold text-slate-700 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100">
                          {batchItem.year}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              <div className="mt-6 flex items-center justify-between rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm shadow-sm dark:border-slate-800 dark:bg-slate-950/50">
                <div>
                  <p className="font-bold text-slate-900 dark:text-white">Import target</p>
                  <p className="text-slate-500 dark:text-slate-400">
                    {importMode === 'single'
                      ? `${(availableProjects.find((project) => String(project.id) === String(selectedImportProjectId))?.name || 'Select project')} | ${selectedMonth} ${selectedYear}`
                      : `${(availableProjects.find((project) => String(project.id) === String(selectedImportProjectId))?.name || 'Select project')} | ${batchFiles.length} month file(s) from ${selectedMonth} ${selectedYear}`}
                  </p>
                </div>
                <span className="rounded-full bg-primary/10 px-3 py-1 text-xs font-black uppercase tracking-[0.2em] text-primary">Ready</span>
              </div>

              <div className="mt-8 flex flex-col-reverse gap-3 sm:flex-row sm:justify-end">
                <button
                  onClick={resetImportDialog}
                  className="rounded-2xl border border-slate-200 px-5 py-3 font-bold text-slate-700 transition-all hover:bg-slate-50 dark:border-slate-700 dark:text-slate-200 dark:hover:bg-slate-800"
                >
                  Cancel
                </button>
                <button
                  onClick={handleUpload}
                  disabled={(importMode === 'single' ? !selectedFile : batchFiles.length === 0) || isUploading}
                  className="rounded-2xl bg-primary px-5 py-3 font-bold text-white shadow-lg shadow-primary/20 transition-all hover:-translate-y-0.5 hover:shadow-xl hover:shadow-primary/25 disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:translate-y-0"
                >
                  {isUploading ? 'Importing...' : (importMode === 'single' ? 'Import Now' : 'Import All Months')}
                </button>
              </div>
          </div>
        </div>
      )}
    </>
  );
}

function ExportItem({ icon, label, sub, color, isPremium }) {
  return (
    <button className={`w-full text-left px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-700 flex items-center gap-4 transition-colors ${isPremium ? 'bg-indigo-50/30 dark:bg-indigo-950/20' : ''}`}>
      <div className={`p-2 rounded-lg bg-slate-50 dark:bg-slate-800 ${color}`}><span className="material-symbols-outlined text-base leading-none">{icon}</span></div>
      <div>
         <div className="text-sm font-bold text-slate-900 dark:text-white flex items-center gap-2">
           {label}
           {isPremium && <span className="bg-indigo-100 text-indigo-600 text-[8px] px-1.5 py-0.5 rounded font-black">AI+</span>}
         </div>
         <div className="text-[10px] text-slate-400 font-medium">{sub}</div>
      </div>
    </button>
  );
}

function ImportDropdown({ label, value, options, isOpen, onToggle, onSelect }) {
  return (
    <div className="relative space-y-2">
      <label className="text-xs font-black uppercase tracking-[0.2em] text-slate-400">{label}</label>
      <button
        type="button"
        onClick={onToggle}
        className={`flex w-full items-center justify-between rounded-2xl border px-4 py-3.5 text-left font-semibold transition-all ${
          isOpen
            ? 'border-primary bg-white shadow-lg shadow-primary/10 ring-4 ring-primary/10 dark:bg-slate-800'
            : 'border-slate-200 bg-slate-50 hover:border-slate-300 hover:bg-white dark:border-slate-700 dark:bg-slate-800 dark:hover:border-slate-600'
        } text-slate-900 dark:text-white`}
      >
        <span>{value}</span>
        <span className={`material-symbols-outlined text-base text-slate-400 transition-transform ${isOpen ? 'rotate-180 text-primary' : ''}`}>expand_more</span>
      </button>

      {isOpen && (
        <div className="absolute left-0 right-0 top-full z-20 mt-2 overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-2xl ring-1 ring-black/5 animate-in fade-in zoom-in-95 duration-200 dark:border-slate-700 dark:bg-slate-900">
          <div className="max-h-60 overflow-y-auto p-2">
            {options.map((option) => (
              <button
                key={option.value}
                type="button"
                onClick={() => onSelect(option.value)}
                className={`flex w-full items-center justify-between rounded-xl px-3 py-2.5 text-sm font-semibold transition-colors ${
                  option.label === value
                    ? 'bg-primary/10 text-primary'
                    : 'text-slate-700 hover:bg-slate-100 dark:text-slate-200 dark:hover:bg-slate-800'
                }`}
              >
                <span>{option.label}</span>
                {option.label === value && <span className="material-symbols-outlined text-base">check</span>}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function MultiSelectDropdown({ label, options, selectedValues, onChange, placeholder = 'Select values', compactLabel = false }) {
  const [isOpen, setIsOpen] = useState(false);
  const dropdownRef = useRef(null);
  const selectedSet = new Set(selectedValues || []);

  useEffect(() => {
    const handleOutside = (event) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setIsOpen(false);
      }
    };
    document.addEventListener('mousedown', handleOutside);
    return () => document.removeEventListener('mousedown', handleOutside);
  }, []);

  const selectedLabels = options
    .filter((option) => selectedSet.has(option.value))
    .map((option) => option.label);

  const buttonText = selectedLabels.length === 0
    ? placeholder
    : selectedLabels.length <= 2
      ? selectedLabels.join(', ')
      : `${selectedLabels.length} selected`;

  const toggleValue = (value) => {
    if (selectedSet.has(value)) {
      onChange(selectedValues.filter((item) => item !== value));
    } else {
      onChange([...(selectedValues || []), value]);
    }
  };

  return (
    <div className="relative" ref={dropdownRef}>
      {label && <label className={`${compactLabel ? 'mb-1' : 'mb-2'} block text-[10px] font-black uppercase tracking-[0.2em] text-slate-400`}>{label}</label>}
      <button
        type="button"
        onClick={() => setIsOpen((current) => !current)}
        className={`flex h-12 w-full items-center justify-between rounded-2xl border px-4 text-left text-sm font-medium transition-all ${
          isOpen
            ? 'border-sky-400 bg-white ring-4 ring-sky-100 dark:bg-slate-900 dark:ring-sky-900/20'
            : 'border-slate-200 bg-white hover:border-slate-300 dark:border-slate-700 dark:bg-slate-900'
        }`}
      >
        <span className="truncate text-slate-700 dark:text-slate-200">{buttonText}</span>
        <span className={`material-symbols-outlined text-base text-slate-400 transition-transform ${isOpen ? 'rotate-180 text-sky-500' : ''}`}>expand_more</span>
      </button>

      {isOpen && (
        <div className="absolute left-0 right-0 top-full z-20 mt-2 overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-2xl ring-1 ring-black/5 dark:border-slate-700 dark:bg-slate-900">
          <div className="flex items-center justify-between border-b border-slate-100 px-3 py-2 dark:border-slate-800">
            <button
              type="button"
              onClick={() => onChange(options.map((option) => option.value))}
              className="text-[10px] font-black uppercase tracking-[0.2em] text-sky-600"
            >
              Select All
            </button>
            <button
              type="button"
              onClick={() => onChange([])}
              className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-500"
            >
              Clear
            </button>
          </div>
          <div className="max-h-64 overflow-y-auto p-2">
            {options.map((option) => {
              const checked = selectedSet.has(option.value);
              return (
                <button
                  key={option.value}
                  type="button"
                  onClick={() => toggleValue(option.value)}
                  className={`flex w-full items-center gap-3 rounded-xl px-3 py-2.5 text-left text-sm font-semibold transition-colors ${
                    checked
                      ? 'bg-sky-50 text-sky-700 dark:bg-sky-950/30 dark:text-sky-300'
                      : 'text-slate-700 hover:bg-slate-100 dark:text-slate-200 dark:hover:bg-slate-800'
                  }`}
                >
                  <span className={`material-symbols-outlined text-base ${checked ? 'text-sky-600' : 'text-slate-300 dark:text-slate-600'}`}>
                    {checked ? 'check_box' : 'check_box_outline_blank'}
                  </span>
                  <span className="truncate">{option.label}</span>
                </button>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

// Authentication Pages
function Login({ setUser, setToken }) {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const navigate = useNavigate();

  const handleLogin = async (e) => {
    e.preventDefault();
    try {
      setError('');
      const res = await apiRequest({
        method: 'post',
        url: '/api/auth/login',
        data: { email, password },
      });
      localStorage.setItem('token', res.data.token);
      localStorage.setItem('user', JSON.stringify(res.data.user));
      setToken(res.data.token);
      setUser(res.data.user);
      navigate('/');
    } catch (err) { setError(err.response?.data?.error || "Login failed"); }
  };

  return (
    <div className="relative isolate flex min-h-screen w-full bg-white text-slate-900 font-display">
      <div className="relative z-20 flex flex-col w-full lg:w-[45%] xl:w-[40%] bg-white border-r border-slate-200 shadow-2xl p-12 lg:p-24 justify-center">
         <div className="flex items-center gap-3 mb-12"><div className="size-8 text-primary"><span className="material-symbols-outlined text-3xl font-black">finance_mode</span></div><h2 className="text-xl font-black tracking-tight uppercase">Analytics Pro</h2></div>
         <h1 className="text-5xl font-black tracking-tighter mb-4">Sign in.</h1>
         <p className="text-slate-400 text-lg mb-10">Access your enterprise data hub.</p>
         {error && <div className="mb-6 p-4 bg-red-50 text-red-600 rounded-2xl text-sm font-bold">{error}</div>}
         <form onSubmit={handleLogin} className="relative z-10 space-y-6">
            <div className="space-y-2"><label className="text-xs font-black uppercase text-slate-400 tracking-widest">Email</label><input className="relative z-10 block w-full appearance-none bg-slate-50 text-slate-900 caret-slate-900 border border-slate-100 rounded-2xl h-16 px-6 font-bold placeholder:text-slate-300 focus:border-primary/30 focus:bg-white focus:ring-4 focus:ring-primary/5 transition-all outline-none text-lg" value={email} onChange={e => setEmail(e.target.value)} type="email" autoComplete="email" placeholder="Enter your email" required /></div>
            <div className="space-y-2"><label className="text-xs font-black uppercase text-slate-400 tracking-widest">Password</label><input className="relative z-10 block w-full appearance-none bg-slate-50 text-slate-900 caret-slate-900 border border-slate-100 rounded-2xl h-16 px-6 font-bold placeholder:text-slate-300 focus:border-primary/30 focus:bg-white focus:ring-4 focus:ring-primary/5 transition-all outline-none text-lg" value={password} onChange={e => setPassword(e.target.value)} type="password" autoComplete="current-password" placeholder="Enter your password" required /></div>
            <button className="w-full bg-primary hover:bg-blue-700 text-white font-black text-xl h-16 rounded-2xl shadow-2xl shadow-primary/30 transition-all active:scale-95 mt-4" type="submit">LOGIN</button>
         </form>
         <p className="mt-10 text-center font-bold text-slate-500">Need an account? <Link to="/signup" className="text-primary hover:underline">Register Hub</Link></p>
      </div>
      <div className="pointer-events-none flex-1 bg-slate-50 relative overflow-hidden hidden lg:block">
         <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[120%] h-[120%] opacity-20"><div className="animate-wave-1 absolute inset-0 bg-blue-500 rounded-[40%] blur-[100px]"></div></div>
         <div className="relative z-10 p-24 h-full flex flex-col justify-end">
            <div className="glass-panel p-12 rounded-[3.5rem] shadow-2xl max-w-lg animate-float">
               <div className="text-sm font-black text-primary uppercase tracking-[4px] mb-4">Precision First</div>
               <h2 className="text-4xl font-black tracking-tighter leading-tight">Predictive insights for high-growth teams.</h2>
            </div>
         </div>
      </div>
    </div>
  );
}

function Signup() {
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const navigate = useNavigate();

  const handleSignup = async (e) => {
    e.preventDefault();
    try {
      setError('');
      await apiRequest({
        method: 'post',
        url: '/api/auth/register',
        data: { name, email, password },
      });
      alert('Registration was successful. Redirecting to the login page.');
      navigate('/login');
    } catch (err) {
      const reason = err.response?.data?.error || err.message || 'Registration failed';
      setError(reason);
      alert(`Registration failed: ${reason}`);
    }
  };

  return (
    <div className="relative isolate flex min-h-screen w-full bg-white text-slate-900 font-display">
      <div className="relative z-20 flex flex-col w-full lg:w-[45%] xl:w-[40%] bg-white border-r border-slate-200 shadow-2xl p-12 lg:p-24 justify-center">
         <h1 className="text-5xl font-black tracking-tighter mb-4 text-primary">Join.</h1>
         <p className="text-slate-400 text-lg mb-10">Create your unified dashboard profile.</p>
         {error && <div className="mb-6 p-4 bg-red-50 text-red-600 rounded-2xl text-sm font-bold">{error}</div>}
         <form onSubmit={handleSignup} className="space-y-6">
            <div className="space-y-2"><label className="text-xs font-black uppercase text-slate-400">NAME</label><input className="relative z-10 block w-full appearance-none bg-slate-50 text-slate-900 caret-slate-900 border border-slate-100 rounded-2xl h-16 px-6 font-bold placeholder:text-slate-300 outline-none" value={name} onChange={e => setName(e.target.value)} type="text" autoComplete="name" placeholder="Your name" required /></div>
            <div className="space-y-2"><label className="text-xs font-black uppercase text-slate-400">EMAIL</label><input className="relative z-10 block w-full appearance-none bg-slate-50 text-slate-900 caret-slate-900 border border-slate-100 rounded-2xl h-16 px-6 font-bold placeholder:text-slate-300 outline-none" value={email} onChange={e => setEmail(e.target.value)} type="email" autoComplete="email" placeholder="Your email" required /></div>
            <div className="space-y-2"><label className="text-xs font-black uppercase text-slate-400">PASSWORD</label><input className="relative z-10 block w-full appearance-none bg-slate-50 text-slate-900 caret-slate-900 border border-slate-100 rounded-2xl h-16 px-6 font-bold placeholder:text-slate-300 outline-none" value={password} onChange={e => setPassword(e.target.value)} type="password" autoComplete="new-password" placeholder="Create a password" required /></div>
            <button className="w-full bg-primary text-white font-black h-16 rounded-2xl text-xl mt-4" type="submit">CREATE HUB</button>
         </form>
         <p className="mt-8 text-center font-bold text-slate-500">Already a member? <Link to="/login" className="text-primary hover:underline">Log In</Link></p>
      </div>
      <div className="pointer-events-none flex-1 bg-primary text-white p-24 flex flex-col justify-center items-center text-center">
         <div className="text-[15rem] font-black opacity-10 absolute select-none">DATA</div>
         <h2 className="text-6xl font-black tracking-tighter relative">Empower your strategy with AI.</h2>
      </div>
    </div>
  );
}


// Dashboard Summary
function DashboardSummary() {
  const [summary, setSummary] = useState(null);
  const [viewYear, setViewYear] = useState('2024');
  const [showExplain, setShowExplain] = useState(false);
  const [drillMonth, setDrillMonth] = useState(null);
  const [hasData, setHasData] = useState(true);
  const token = localStorage.getItem('token');

  useEffect(() => {
    const fetchData = async () => {
      try {
        const res = await apiRequest({ method: 'get', url: `/api/dashboard/summary?year=${viewYear}`, headers: { 'Authorization': `Bearer ${token}` } });
        setSummary(res.data);
        setHasData(res.data && Object.keys(res.data).length > 0 && res.data.stats && res.data.stats.project_count > 0);
      } catch (err) { console.error('Error fetching dashboard:', err); setHasData(false); }
    };
    fetchData();
  }, [viewYear, token]);

  const stats = summary?.stats || {};
  const rawRegionData = summary?.state_data || summary?.region_data || {};
  const rawCategoryData = summary?.category_data || {};
  const regionData = rawRegionData;
  const categoryData = rawCategoryData;
  const trendData = summary?.trend || [];

  const topStateEntry = Object.entries(regionData).sort(([, a], [, b]) => b - a)[0];
  const activeStatesCount = Object.keys(regionData).length;
  const totalQuantity = Number(stats.total_quantity || 0);
  const trendSeries = (trendData || []).map((row) => Number(row.total_revenue || 0)).filter((value) => Number.isFinite(value));
  const growthIndex = trendSeries.length > 1 && trendSeries[0] > 0
    ? ((trendSeries[trendSeries.length - 1] - trendSeries[0]) / trendSeries[0]) * 100
    : 0;
  const categoryArray = Object.entries(categoryData).map(([name, value]) => {
    const total = Object.values(categoryData).reduce((a, b) => a + b, 0);
    return {
      name,
      value: Math.round((value / total) * 100),
      amount: formatInrCompact(value).replace('Rs ', '₹'),
      color: COLORS[Object.keys(rawCategoryData).indexOf(name) % COLORS.length]
    };
  });

  const revenueDisplay = formatInrCompact(stats.total_revenue || 0).replace('Rs ', '₹');
  const forecastDisplay = formatInrCompact(Math.round(Number(stats.total_revenue || 0) * 1.1)).replace('Rs ', '₹');

  return (
    <div className="p-8 space-y-8 max-w-[1600px] mx-auto">
      {/* Header Info */}
      <div className="flex items-center justify-between">
         <div>
            <h1 className="text-3xl font-black tracking-tighter">Global Dashboard</h1>
            <p className="text-slate-400 font-bold uppercase text-[10px] tracking-[2px] mt-1">Cross-Functional Intelligence Hub | {viewYear}</p>
         </div>
         <div className="flex bg-slate-100 dark:bg-slate-800 p-1 rounded-xl">
            {['2023', '2024', '2025'].map(y => (
              <button key={y} onClick={() => setViewYear(y)} className={`px-5 py-1.5 rounded-lg text-sm font-bold transition-all ${viewYear === y ? 'bg-white dark:bg-slate-700 text-primary shadow-sm' : 'text-slate-400'}`}>{y}</button>
            ))}
         </div>
      </div>

      {/* KPI Stats */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <StatCard title="Revenue" value={revenueDisplay} change="+12.5%" icon="payments" color="indigo" forecast={`Forecast: ${forecastDisplay} next quarter`} />
        <StatCard title="Market Hubs" value={stats.project_count || 0} change="+3" icon="public" color="emerald" forecast="Expansion: 2 new nodes pending" />
        <StatCard title="Global Usage" value={totalQuantity.toLocaleString()} change={`${activeStatesCount} states`} icon="person_add" color="blue" forecast="total units across uploaded rows" />
        <StatCard title="Growth Index" value={`${growthIndex >= 0 ? '+' : ''}${growthIndex.toFixed(1)}%`} change={`${trendSeries.length} months`} icon="trending_up" color="amber" forecast="revenue trend movement in selected year" />
      </div>

      {/* Main Charts Section */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-8">
        {/* Trend Chart Card */}
        <div className="bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 p-8 rounded-[2.5rem] shadow-xl shadow-slate-200/50 dark:shadow-none flex flex-col h-[500px]">
           <div className="flex justify-between items-start mb-8">
              <div>
                 <h4 className="text-2xl font-black text-slate-900 dark:text-white tracking-tighter">Revenue & Profit Trend</h4>
                 <div className="flex items-center gap-2 mt-1">
                    <span className="w-2 h-2 rounded-full bg-primary animate-pulse"></span>
                    <span className="text-[10px] text-slate-400 font-black uppercase tracking-widest">Real-time analytical sync</span>
                 </div>
              </div>
              <div className="flex gap-2">
                 <button onClick={() => setShowExplain(true)} className="px-4 py-2 bg-indigo-50 dark:bg-indigo-900/20 text-indigo-600 text-[10px] font-black rounded-xl border border-indigo-100 dark:border-indigo-800 hover:scale-105 active:scale-95 transition-all">EXPLAIN AI</button>
                 <button className="px-4 py-2 bg-slate-50 dark:bg-slate-800 text-slate-500 text-[10px] font-black rounded-xl border border-slate-100 dark:border-slate-700">COMPARE</button>
              </div>
           </div>
           
           <div className="flex-1 min-h-0">
              <ResponsiveContainer width="100%" height="100%">
                 <ComposedChart data={trendData} onClick={(e) => e && setDrillMonth(e.activeLabel)}>
                    <defs>
                       <linearGradient id="barGrad" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="0%" stopColor="#4f46e5" stopOpacity={1} />
                          <stop offset="100%" stopColor="#818cf8" stopOpacity={0.8} />
                       </linearGradient>
                    </defs>
                    <XAxis dataKey="month_name" axisLine={false} tickLine={false} tick={{fontSize: 10, fontWeight: 900, fill: '#94a3b8'}} dy={10} />
                    <YAxis hide domain={[0, 'auto']} />
                    <Tooltip cursor={{fill: 'rgba(79, 70, 229, 0.05)'}} contentStyle={{ borderRadius: '20px', border: 'none', background: '#0f172a', color: '#fff', padding: '15px' }} />
                    <Bar dataKey="total_revenue" name="Revenue" fill="url(#barGrad)" radius={[10, 10, 0, 0]} barSize={35} />
                    <Line type="monotone" dataKey="net_revenue" name="Profit" stroke="#f97316" strokeWidth={4} dot={{ r: 6, fill: '#f97316', strokeWidth: 3, stroke: '#fff' }} activeDot={{ r: 8 }} />
                 </ComposedChart>
              </ResponsiveContainer>
           </div>
           
           <div className="flex justify-center gap-10 mt-8 pt-6 border-t border-slate-50 dark:border-slate-800">
              <div className="flex items-center gap-3">
                 <div className="w-8 h-2 bg-primary rounded-full"></div>
                 <span className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Revenue</span>
              </div>
              <div className="flex items-center gap-3">
                 <div className="w-8 h-2 bg-orange-500 rounded-full"></div>
                 <span className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Profit</span>
              </div>
           </div>
        </div>

        {/* India Map Card */}
        <div className="bg-white dark:bg-slate-900 p-8 rounded-[2.5rem] shadow-xl shadow-slate-200/50 dark:shadow-none border border-slate-100 dark:border-slate-800 flex flex-col h-[500px] relative overflow-hidden group">
           <div className="w-full flex justify-between items-start relative z-10">
              <div>
                 <h2 className="text-2xl font-black tracking-tighter">India Market Hubs</h2>
                 <p className="text-xs text-slate-400 font-bold mt-1 uppercase">Strategic Regional distribution</p>
              </div>
              {hasData ? (
                <div className="flex items-center gap-2 bg-rose-50 text-rose-500 px-3 py-1 rounded-full border border-rose-100">
                   <div className="w-2 h-2 bg-rose-500 rounded-full animate-ping"></div>
                   <span className="text-[10px] font-black uppercase">Active Nodes</span>
                </div>
              ) : (
                <div className="flex items-center gap-2 bg-slate-50 text-slate-500 px-3 py-1 rounded-full border border-slate-100">
                   <span className="text-[10px] font-black uppercase">No Data</span>
                </div>
              )}
           </div>
           
           <div className="relative flex-1 -mt-4">
              <IndiaHeatMap stateData={regionData} hasData={hasData} showHeatScale={false} />
           </div>
           
           <div className="grid grid-cols-2 gap-4 mt-auto relative z-10">
              <div className="bg-slate-50 dark:bg-slate-800/50 p-4 rounded-2xl flex items-center gap-4 border border-slate-100 dark:border-slate-800 group/item hover:bg-white dark:hover:bg-slate-800 transition-all cursor-default">
                 <div className="w-12 h-12 bg-white dark:bg-slate-900 rounded-2xl flex items-center justify-center text-primary shadow-sm group-hover/item:rotate-12 transition-all"><span className="material-symbols-outlined">location_on</span></div>
                 <div><div className="text-[10px] font-black text-slate-400 uppercase tracking-[2px]">Top State</div><div className="text-sm font-black text-slate-900 dark:text-white">{hasData && topStateEntry ? topStateEntry[0] : 'No Data'}</div></div>
              </div>
              <div className="bg-slate-50 dark:bg-slate-800/50 p-4 rounded-2xl flex items-center gap-4 border border-slate-100 dark:border-slate-800 group/item hover:bg-white dark:hover:bg-slate-800 transition-all cursor-default">
                 <div className="w-12 h-12 bg-white dark:bg-slate-900 rounded-2xl flex items-center justify-center text-rose-500 shadow-sm group-hover/item:rotate-12 transition-all"><span className="material-symbols-outlined">local_fire_department</span></div>
                 <div><div className="text-[10px] font-black text-slate-400 uppercase tracking-[2px]">Active States</div><div className="text-sm font-black text-slate-900 dark:text-white">{hasData ? activeStatesCount : 'No Data'}</div></div>
              </div>
           </div>
        </div>
      </div>

      {/* Categories & Performance Area */}
      {hasData ? (
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-8 pb-12">
        <DonutChartCard data={categoryArray} />
        <div className="bg-white dark:bg-slate-900 p-8 rounded-[2.5rem] shadow-xl border border-slate-100 dark:border-slate-800 flex flex-col h-full ring-1 ring-black/5">
           <div className="flex justify-between items-start mb-10">
              <div>
                 <h2 className="text-2xl font-black tracking-tighter uppercase">Sales by Category</h2>
                 <p className="text-xs text-slate-400 font-bold mt-1">Imported category totals from your latest data</p>
              </div>
              <div className="text-primary text-[10px] font-black uppercase tracking-widest px-4 py-2 bg-primary/5 rounded-xl">{Object.keys(categoryData).length} categories</div>
           </div>
           <div className="space-y-4">
              {Object.entries(categoryData)
                .sort(([, leftValue], [, rightValue]) => rightValue - leftValue)
                .map(([name, value], index, entries) => {
                  const totalCategoryRevenue = entries.reduce((sum, [, categoryValue]) => sum + Number(categoryValue || 0), 0);
                  const progress = totalCategoryRevenue > 0 ? Math.round((Number(value) / totalCategoryRevenue) * 100) : 0;
                  const colorOptions = ['bg-indigo-600', 'bg-emerald-500', 'bg-amber-500', 'bg-rose-500', 'bg-sky-500', 'bg-fuchsia-500'];
                  const iconOptions = ['devices', 'weekend', 'category', 'shopping_bag', 'inventory_2', 'sell'];

                  return (
                    <PerformanceBar
                      key={name}
                      label={name}
                      value={formatInrCompact(value)}
                      progress={progress}
                      icon={iconOptions[index % iconOptions.length]}
                      growth={`${progress}% of sales`}
                      trendTone="neutral"
                      color={colorOptions[index % colorOptions.length]}
                      badge={index === 0 ? 'TOP' : undefined}
                    />
                  );
                })}
           </div>
        </div>
      </div>
      ) : (
      <div className="pb-12 bg-white dark:bg-slate-900 rounded-[2.5rem] border border-slate-100 dark:border-slate-800 p-12 text-center">
        <div className="text-5xl mb-4">📊</div>
        <h3 className="text-xl font-black text-slate-900 dark:text-white mb-2">No Data Available</h3>
        <p className="text-slate-500 dark:text-slate-400">Import data to see market distribution and performance metrics</p>
      </div>
      )}

      {/* Modal Overlays */}
      {showExplain && <ExplainModal onClose={() => setShowExplain(false)} />}
      {drillMonth && <DrilldownModal month={drillMonth} onClose={() => setDrillMonth(null)} />}
    </div>
  );
}

function ExplainModal({ onClose }) {
  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center p-6 animate-in fade-in duration-300">
       <div className="absolute inset-0 bg-slate-900/60 backdrop-blur-md" onClick={onClose}></div>
       <div className="relative w-full max-w-xl bg-white dark:bg-slate-900 rounded-[3rem] shadow-2xl p-10 border border-slate-200 dark:border-slate-800 group">
          <button onClick={onClose} className="absolute top-8 right-8 text-slate-400 hover:text-slate-900 dark:hover:text-white transition-all"><span className="material-symbols-outlined">close</span></button>
          <div className="flex items-center gap-4 mb-8">
             <div className="w-14 h-14 bg-indigo-50 dark:bg-indigo-900/40 rounded-2xl flex items-center justify-center text-indigo-600 animate-bounce"><span className="material-symbols-outlined text-3xl">auto_awesome</span></div>
             <div>
                <h3 className="text-2xl font-black tracking-tighter">AI Explainability</h3>
                <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Powered by Yua AI Intelligence</p>
             </div>
          </div>
          <div className="space-y-6 text-slate-600 dark:text-slate-400 font-medium leading-relaxed">
             <div className="bg-slate-50 dark:bg-slate-800/50 p-6 rounded-3xl border-l-4 border-indigo-500">
                <p className="text-sm font-bold text-slate-900 dark:text-white mb-2">Trend Analysis Summary</p>
                <p className="text-sm">Revenue peaked in June (₹6L) due to Q2 fiscal closures. Festive spending uplift of 18% noted in Q4. April dip is a recurring seasonal procurement lull.</p>
             </div>
             <div className="grid grid-cols-2 gap-4">
                <div className="p-4 bg-emerald-50 dark:bg-emerald-950/20 rounded-2xl border border-emerald-100 dark:border-emerald-800">
                   <div className="text-[10px] font-black text-emerald-600 uppercase mb-1">Growth Driver</div>
                   <p className="text-sm font-bold text-slate-900 dark:text-white">Cloud Systems (+42%)</p>
                </div>
                <div className="p-4 bg-rose-50 dark:bg-rose-950/20 rounded-2xl border border-rose-100 dark:border-rose-800">
                   <div className="text-[10px] font-black text-rose-600 uppercase mb-1">Alert Factor</div>
                   <p className="text-sm font-bold text-slate-900 dark:text-white">Retail Attrition (-5%)</p>
                </div>
             </div>
          </div>
          <button onClick={onClose} className="w-full mt-10 bg-indigo-600 text-white font-black py-4 rounded-2xl shadow-xl shadow-indigo-600/20 hover:scale-[1.02] active:scale-95 transition-all">GENERATE FULL REPORT</button>
       </div>
    </div>
  );
}

function DrilldownModal({ month, onClose }) {
  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center p-6 animate-in fade-in duration-300">
       <div className="absolute inset-0 bg-slate-900/80 backdrop-blur-sm" onClick={onClose}></div>
       <div className="relative w-full max-w-lg bg-card-light dark:bg-slate-800 rounded-[2.5rem] shadow-2xl p-8 border border-slate-200 dark:border-slate-700">
          <div className="flex justify-between items-center mb-8">
             <h3 className="text-2xl font-black tracking-tighter uppercase">{month} Breakdown</h3>
             <button onClick={onClose} className="text-slate-400 hover:text-white"><span className="material-symbols-outlined">close</span></button>
          </div>
          <div className="space-y-4">
             <div className="p-4 bg-slate-100 dark:bg-slate-700/50 rounded-2xl flex justify-between items-center">
                <span className="text-sm font-bold text-slate-500">Peak Performance Day</span>
                <span className="text-sm font-black">21st (₹45k)</span>
             </div>
             <div className="p-4 bg-slate-100 dark:bg-slate-700/50 rounded-2xl flex justify-between items-center">
                <span className="text-sm font-bold text-slate-500">Active Clients</span>
                <span className="text-sm font-black">42 Entities</span>
             </div>
             <div className="p-4 bg-indigo-50 dark:bg-indigo-900/30 rounded-2xl border border-indigo-100 dark:border-indigo-800">
                <div className="text-xs font-black text-indigo-600 uppercase mb-2">Category Split</div>
                <div className="space-y-1">
                   <div className="flex justify-between text-xs font-bold"><span>Electronics</span><span>55%</span></div>
                   <div className="w-full h-1 bg-indigo-200 dark:bg-indigo-800 rounded-full overflow-hidden"><div className="h-full bg-indigo-600 w-[55%]"></div></div>
                </div>
             </div>
          </div>
          <button onClick={onClose} className="w-full mt-8 py-3 bg-slate-900 text-white rounded-xl font-black">CLOSE DATA VIEW</button>
       </div>
    </div>
  );
}

function StatCard({ title, value, change, icon, color, forecast }) {
  const colorClasses = { 
    indigo: 'bg-indigo-500 text-white shadow-indigo-500/20', 
    emerald: 'bg-emerald-500 text-white shadow-emerald-500/20', 
    blue: 'bg-blue-500 text-white shadow-blue-500/20', 
    amber: 'bg-amber-500 text-white shadow-amber-500/20' 
  };
  const isPositive = change.startsWith('+');
  const isNegative = change.startsWith('-');
  const trendClasses = isPositive ? 'text-emerald-500' : isNegative ? 'text-rose-500' : 'text-slate-400';
  const trendIcon = isPositive ? 'trending_up' : isNegative ? 'trending_down' : 'remove';

  return (
    <div className="bg-white dark:bg-slate-900 p-8 rounded-[2rem] shadow-lg border border-slate-100 dark:border-slate-800 transition-all hover:-translate-y-2 group">
       <div className="flex justify-between items-start mb-6">
          <div className={`p-3 rounded-2xl ${colorClasses[color]} group-hover:scale-110 transition-transform`}><span className="material-symbols-outlined text-2xl leading-none">{icon}</span></div>
          <div className={`flex items-center gap-1 ${trendClasses} font-black text-xs`}>
             <span className="material-symbols-outlined text-sm">{trendIcon}</span> {change}
          </div>
       </div>
       <div className="text-xs font-black text-slate-400 uppercase tracking-widest mb-1">{title}</div>
       <div className="text-4xl font-black tracking-tighter mb-4 text-slate-900 dark:text-white uppercase">{value}</div>
       <div className="pt-4 border-t border-slate-50 dark:border-slate-800 flex items-center gap-2">
          <span className="w-1.5 h-1.5 bg-slate-300 rounded-full"></span>
          <p className="text-[10px] text-slate-400 font-bold italic truncate uppercase">{forecast}</p>
       </div>
    </div>
  );
}

function MapDot({ top, left, label, value, color }) {
  const [show, setShow] = useState(false);
  return (
    <div className="absolute cursor-pointer perspective-container" style={{ top, left }} onMouseEnter={() => setShow(true)} onMouseLeave={() => setShow(false)}>
       <div className={`w-4 h-4 ${color} rounded-full border-2 border-white dark:border-slate-800 shadow-2xl relative z-20 transition-transform hover:scale-150`}>
          <div className={`absolute inset-0 rounded-full ${color} animate-ping opacity-75`}></div>
       </div>
       {show && (
         <div className="absolute left-1/2 -translate-x-1/2 bottom-8 bg-slate-900 text-white px-4 py-2.5 rounded-2xl shadow-2xl text-[10px] whitespace-nowrap z-50 animate-in zoom-in duration-200">
            <div className="font-black border-b border-white/10 pb-1 mb-1">{label}</div>
            <div className="flex justify-between gap-4 font-bold opacity-80"><span>Revenue:</span><span>{value}</span></div>
            <div className="flex justify-between gap-4 font-bold opacity-80"><span>Status:</span><span className="text-emerald-400">OPTIMAL</span></div>
            <div className="absolute -bottom-1 left-1/2 -translate-x-1/2 w-2 h-2 bg-slate-900 rotate-45"></div>
         </div>
       )}
    </div>
  );
}

function IndiaHeatMap({ stateData, hasData, showHeatScale = true, mapScale = 1 }) {
  const [hoveredState, setHoveredState] = useState(null);
  const [hoverPosition, setHoverPosition] = useState({ x: 0, y: 0 });
  const [mapGeometry, setMapGeometry] = useState(null);
  const [mapGeometryError, setMapGeometryError] = useState('');
  const stateEntries = Object.entries(stateData || {});
  const values = stateEntries.map(([, value]) => Number(value || 0)).filter(value => value > 0);
  const minValue = values.length ? Math.min(...values) : 0;
  const maxValue = values.length ? Math.max(...values) : 0;
  const colorScale = scaleLinear()
    .domain([minValue || 0, maxValue || 1])
    .range(['#dbeafe', '#1d4ed8']);

  useEffect(() => {
    let isMounted = true;

    const loadGeometry = async () => {
      try {
        setMapGeometryError('');
        const primary = await fetch(INDIA_GEO_URL);
        if (!primary.ok) {
          throw new Error(`Primary map fetch failed: ${primary.status}`);
        }
        const primaryJson = await primary.json();
        if (isMounted) {
          setMapGeometry(primaryJson);
        }
      } catch (primaryError) {
        try {
          const fallback = await fetch('/india-states.geojson');
          if (!fallback.ok) {
            throw new Error(`Fallback map fetch failed: ${fallback.status}`);
          }
          const fallbackJson = await fallback.json();
          if (isMounted) {
            setMapGeometry(fallbackJson);
          }
        } catch (fallbackError) {
          if (isMounted) {
            setMapGeometry(null);
            setMapGeometryError(String(fallbackError?.message || primaryError?.message || 'Unable to load map geometry'));
          }
        }
      }
    };

    loadGeometry();
    return () => {
      isMounted = false;
    };
  }, []);

  return (
    <div className="relative h-full w-full rounded-[2rem] bg-[radial-gradient(circle_at_top,rgba(59,130,246,0.12),transparent_45%),linear-gradient(180deg,rgba(248,250,252,0.96),rgba(241,245,249,0.78))] p-4 dark:bg-[radial-gradient(circle_at_top,rgba(59,130,246,0.16),transparent_45%),linear-gradient(180deg,rgba(15,23,42,0.96),rgba(15,23,42,0.82))]">
      {(() => {
        const features = Array.isArray(mapGeometry?.features) ? mapGeometry.features : [];
        const viewBoxSize = 800;
        const padding = 42;

        if (features.length === 0) {
          return null;
        }

        let minLon = Number.POSITIVE_INFINITY;
        let maxLon = Number.NEGATIVE_INFINITY;
        let minLat = Number.POSITIVE_INFINITY;
        let maxLat = Number.NEGATIVE_INFINITY;

        const walk = (coords) => {
          if (!Array.isArray(coords)) return;
          if (typeof coords[0] === 'number' && typeof coords[1] === 'number') {
            const lon = Number(coords[0]);
            const lat = Number(coords[1]);
            if (Number.isFinite(lon) && Number.isFinite(lat)) {
              minLon = Math.min(minLon, lon);
              maxLon = Math.max(maxLon, lon);
              minLat = Math.min(minLat, lat);
              maxLat = Math.max(maxLat, lat);
            }
            return;
          }
          coords.forEach(walk);
        };

        features.forEach((feature) => walk(feature?.geometry?.coordinates));

        const lonRange = Math.max(0.0001, maxLon - minLon);
        const latRange = Math.max(0.0001, maxLat - minLat);
        const drawableWidth = viewBoxSize - padding * 2;
        const drawableHeight = viewBoxSize - padding * 2;
        const scale = Math.min(drawableWidth / lonRange, drawableHeight / latRange) * mapScale;
        const offsetX = (viewBoxSize - lonRange * scale) / 2;
        const offsetY = (viewBoxSize - latRange * scale) / 2;

        const projectPoint = (lon, lat) => {
          const x = offsetX + (lon - minLon) * scale;
          const y = offsetY + (maxLat - lat) * scale;
          return [x, y];
        };

        const ringToPath = (ring) => {
          if (!Array.isArray(ring) || ring.length === 0) return '';
          return ring.map((point, index) => {
            const [x, y] = projectPoint(Number(point[0]), Number(point[1]));
            return `${index === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`;
          }).join(' ') + ' Z';
        };

        const geometryToPath = (geometry) => {
          if (!geometry || !geometry.type || !geometry.coordinates) return '';
          if (geometry.type === 'Polygon') {
            return geometry.coordinates.map(ringToPath).join(' ');
          }
          if (geometry.type === 'MultiPolygon') {
            return geometry.coordinates.flatMap((polygon) => polygon.map(ringToPath)).join(' ');
          }
          return '';
        };

        return (
          <svg viewBox={`0 0 ${viewBoxSize} ${viewBoxSize}`} className="h-full w-full">
            {features.map((feature, index) => {
              const stateName = feature?.properties?.NAME_1 || feature?.properties?.name || `State ${index + 1}`;
              const value = Number(stateData?.[stateName] || 0);
              const fill = value > 0 ? colorScale(value) : '#bfdbfe';
              const d = geometryToPath(feature.geometry);
              if (!d) return null;

              return (
                <path
                  key={`${stateName}-${index}`}
                  d={d}
                  fill={fill}
                  stroke="#1e293b"
                  strokeWidth="0.9"
                  fillRule="evenodd"
                  onMouseEnter={() => setHoveredState({ name: stateName, value })}
                  onMouseMove={(event) => {
                    const nativeEvent = event.nativeEvent;
                    setHoverPosition({ x: nativeEvent.offsetX, y: nativeEvent.offsetY });
                  }}
                  onMouseLeave={() => setHoveredState(null)}
                />
              );
            })}
          </svg>
        );
      })()}

      {mapGeometryError && (
        <div className="pointer-events-none absolute left-1/2 top-1/2 w-[70%] -translate-x-1/2 -translate-y-1/2 rounded-2xl border border-rose-200 bg-white/90 px-4 py-3 text-center text-xs font-bold text-rose-600 shadow-sm">
          Map geometry is unavailable: {mapGeometryError}
        </div>
      )}

      {hoveredState && (
        <div
          className="pointer-events-none absolute rounded-xl border border-slate-200 bg-white/95 px-3 py-2 text-xs shadow-lg"
          style={{
            left: `${Math.min(Math.max(hoverPosition.x + 12, 12), 420)}px`,
            top: `${Math.min(Math.max(hoverPosition.y - 10, 12), 360)}px`,
          }}
        >
          <div className="font-black text-slate-900">{hoveredState.name}</div>
          <div className="mt-0.5 font-bold text-slate-600">
            {hoveredState.value > 0 ? formatInrCompact(hoveredState.value) : 'No uploaded data'}
          </div>
        </div>
      )}
    </div>
  );
}

function DonutChartCard({ data }) {
  const [hoveredData, setHoveredData] = useState(null);
  
  const chartData = data && data.length > 0 ? data : [
    { name: 'No Data', value: 100, color: '#e2e8f0', amount: '₹0L' }
  ];

  return (
    <div className="bg-white dark:bg-slate-900 p-8 rounded-[2.5rem] shadow-xl border border-slate-100 dark:border-slate-800 flex flex-col items-center group">
       <div className="w-full flex justify-between items-start mb-4">
          <div><h2 className="text-2xl font-black tracking-tighter uppercase">Category Split</h2><p className="text-xs text-slate-400 font-bold uppercase mt-1">Market distribution analysis</p></div>
          <span className="material-symbols-outlined text-slate-300">info</span>
       </div>
       <div className="relative w-64 h-64 my-6">
          <ResponsiveContainer width="100%" height="100%">
             <PieChart>
                <Pie data={chartData} dataKey="value" nameKey="name" cx="50%" cy="50%" innerRadius={80} outerRadius={110} paddingAngle={4} onMouseEnter={(e) => setHoveredData(e)} onMouseLeave={() => setHoveredData(null)}>
                   {chartData.map((entry, index) => <Cell key={index} fill={entry.color} stroke="none" className="hover:opacity-80 transition-opacity outline-none" />)}
                </Pie>
             </PieChart>
          </ResponsiveContainer>
          <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none transition-all duration-300">
             <div className="text-4xl font-black text-slate-900 dark:text-white tracking-tighter">{hoveredData ? hoveredData.amount : '100%'}</div>
             <div className="text-[10px] font-black text-slate-400 uppercase tracking-widest mt-1">{hoveredData ? hoveredData.name : 'Total Impact'}</div>
          </div>
       </div>
       <div className="flex justify-between w-full px-4 border-t border-slate-50 dark:border-slate-800 pt-8 mt-auto">
          {chartData.map(d => (
            <div key={d.name} className="flex flex-col items-center group/leg cursor-pointer transition-all" onMouseEnter={() => setHoveredData(d)} onMouseLeave={() => setHoveredData(null)}>
               <div className="flex items-center gap-2 mb-1">
                  <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: d.color }}></div>
                  <span className="text-[10px] font-black uppercase text-slate-400 group-hover/leg:text-slate-900 dark:group-hover/leg:text-white transition-colors">{d.name}</span>
               </div>
               <span className="text-sm font-black text-slate-900 dark:text-white">{d.value}%</span>
            </div>
          ))}
       </div>
    </div>
  );
}

function PerformanceBar({ label, value, progress, icon, growth, color, badge, trendTone }) {
  const resolvedTrendTone = trendTone || (growth.startsWith('+') ? 'positive' : growth.startsWith('-') ? 'negative' : 'neutral');
  return (
    <div className="p-5 rounded-[1.5rem] border border-slate-50 dark:border-slate-800 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-all group/bar">
       <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-4">
             <div className="w-14 h-14 bg-slate-50 dark:bg-slate-800 rounded-2xl flex items-center justify-center text-slate-400 group-hover/bar:bg-white dark:group-hover/bar:bg-slate-900 group-hover/bar:text-primary transition-all shadow-sm"><span className="material-symbols-outlined text-3xl">{icon}</span></div>
             <div>
                <div className="text-base font-black flex items-center gap-2 text-slate-900 dark:text-white">
                   {label} {badge && <span className="bg-primary/10 text-primary text-[8px] px-2 py-1 rounded font-black tracking-widest leading-none">{badge}</span>}
                </div>
                <div className="text-[10px] text-slate-400 font-black uppercase tracking-widest mt-1">Cross-unit performance</div>
             </div>
          </div>
          <div className="text-right">
             <div className="text-base font-black text-slate-900 dark:text-white">{value}</div>
             <div className={`text-[10px] font-black flex items-center justify-end gap-1 ${resolvedTrendTone === 'positive' ? 'text-emerald-500' : resolvedTrendTone === 'negative' ? 'text-rose-500' : 'text-slate-400 dark:text-slate-300'}`}>
                {resolvedTrendTone === 'positive' ? <span className="material-symbols-outlined text-[10px]">north</span> : resolvedTrendTone === 'negative' ? <span className="material-symbols-outlined text-[10px]">south</span> : <span className="material-symbols-outlined text-[10px]">donut_small</span>}
                {growth}
             </div>
          </div>
       </div>
       <div className="h-2 w-full bg-slate-100 dark:bg-slate-800 rounded-full overflow-hidden">
          <div className={`h-full ${color} rounded-full transition-all duration-1000 group-hover/bar:scale-x-105 origin-left shadow-lg`} style={{ width: `${progress}%` }}></div>
       </div>
    </div>
  );
}

function FilterButton({ icon, label, count }) {
  return (
    <button className="px-4 py-2.5 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-xs font-black text-slate-600 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-700 hover:border-slate-300 transition-all flex items-center gap-3 group shadow-sm">
       <span className="material-symbols-outlined text-base group-hover:text-primary transition-colors">{icon}</span> 
       <span>{label}</span>
       {count && <span className="bg-slate-100 dark:bg-slate-700 px-1.5 py-0.5 rounded text-[8px] opacity-70">{count}</span>}
    </button>
  );
}

function ProjectCard({ project, isFavorite, onToggleFavorite, onEditProject }) {
  const navigate = useNavigate();
  const canEditProject = typeof onEditProject === 'function';

  return (
    <div onClick={() => navigate(`/advanced-analytics/${project.id}`, { state: { projectName: project.name } })} className="bg-white dark:bg-slate-900 p-8 rounded-[3rem] shadow-xl border border-slate-100 dark:border-slate-800 cursor-pointer transition-all hover:-translate-y-3 group">
      <div
        className="h-48 rounded-[2.5rem] flex items-center justify-center mb-8 transition-all relative overflow-hidden bg-slate-50 dark:bg-slate-800 group-hover:bg-primary/5"
        style={project.store_logo_url ? {
          backgroundImage: `url(${project.store_logo_url})`,
          backgroundSize: 'cover',
          backgroundPosition: 'center',
        } : undefined}
      >
        <button
          type="button"
          onClick={(event) => {
            event.stopPropagation();
            onToggleFavorite(project.id);
          }}
          className={`absolute left-4 top-4 z-10 flex h-10 w-10 items-center justify-center rounded-full border transition-all opacity-0 pointer-events-none group-hover:opacity-100 group-hover:pointer-events-auto focus-visible:opacity-100 focus-visible:pointer-events-auto ${
            isFavorite
              ? 'border-amber-300 bg-amber-100 text-amber-500'
              : 'border-white/70 bg-white/85 text-slate-500 hover:text-amber-500'
          }`}
          aria-label={isFavorite ? 'Remove from favorites' : 'Add to favorites'}
          title={isFavorite ? 'Remove from favorites' : 'Add to favorites'}
        >
          <span className="material-symbols-outlined text-[20px]">{isFavorite ? 'star' : 'star_outline'}</span>
        </button>
        {canEditProject && (
          <button
            type="button"
            onClick={(event) => {
              event.stopPropagation();
              onEditProject(project);
            }}
            className="absolute right-4 top-4 z-10 flex h-10 w-10 items-center justify-center rounded-full border border-white/70 bg-white/85 text-slate-500 transition-all opacity-0 pointer-events-none group-hover:opacity-100 group-hover:pointer-events-auto focus-visible:opacity-100 focus-visible:pointer-events-auto hover:text-indigo-600"
            aria-label="Edit project details"
            title="Edit project details"
          >
            <span className="material-symbols-outlined text-[20px]">edit</span>
          </button>
        )}
        {!project.store_logo_url && (
          <span className="material-symbols-outlined text-7xl text-slate-200 transition-all duration-500 group-hover:text-primary group-hover:scale-110">storefront</span>
        )}
        <div className="absolute bottom-4 right-4 bg-emerald-100 text-emerald-600 px-3 py-1 rounded-full text-[8px] font-black uppercase">Active</div>
      </div>
      <h3 className="text-2xl font-black tracking-tight mb-2 group-hover:text-primary transition-colors">{project.name}</h3>
      <div className="flex items-center justify-between mt-6 pt-6 border-t border-slate-50 dark:border-slate-800">
        <div className="text-[10px] font-black text-slate-400 uppercase tracking-widest">{isFavorite ? 'Favorite Store' : 'Store Entry'}</div>
        <span className="material-symbols-outlined text-slate-300 group-hover:text-primary transition-colors">arrow_forward</span>
      </div>
    </div>
  );
}

// Sub-pages
function ProjectsList() {
  const [projects, setProjects] = useState([]);
  const [isCreateOpen, setIsCreateOpen] = useState(false);
  const [isEditOpen, setIsEditOpen] = useState(false);
  const [isCreatingProject, setIsCreatingProject] = useState(false);
  const [isUpdatingProject, setIsUpdatingProject] = useState(false);
  const [editingProjectId, setEditingProjectId] = useState(null);
  const [projectLogoFileName, setProjectLogoFileName] = useState('');
  const [favoriteProjectIds, setFavoriteProjectIds] = useState(() => getStoredFavoriteProjectIds());
  const [projectForm, setProjectForm] = useState({
    name: '',
    store_type: '',
    store_segments: '',
    branch_location_id: '',
    store_logo_url: '',
    currency_code: 'INR',
    timezone: 'Asia/Kolkata',
    tax_identification_number: '',
    default_tax_rate: '',
    low_stock_threshold: '',
    opening_balances: '',
    owner_admin_email: '',
    contact_number: '',
  });
  const navigate = useNavigate();
  const token = localStorage.getItem('token');

  const toggleFavoriteProject = (projectId) => {
    setFavoriteProjectIds((currentFavorites) => {
      const normalizedId = String(projectId);
      const nextFavorites = currentFavorites.includes(normalizedId)
        ? currentFavorites.filter((favoriteId) => favoriteId !== normalizedId)
        : [...currentFavorites, normalizedId];
      setStoredFavoriteProjectIds(nextFavorites);
      return nextFavorites;
    });
  };

  useEffect(() => {
    apiRequest({
      method: 'get',
      url: '/api/projects',
      headers: { 'Authorization': `Bearer ${token}` }
    }).then(res => setProjects(res.data)).catch(err => console.error('Error fetching projects:', err));
  }, [token]);

  const handleProjectFormChange = (field, value) => {
    setProjectForm((currentForm) => ({ ...currentForm, [field]: value }));
  };

  const parseOpeningBalancesFromForm = () => {
    if (!projectForm.opening_balances.trim()) return [];
    return projectForm.opening_balances
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => {
        const [productName, balanceValue] = line.split(':');
        return {
          product: productName?.trim() || 'Unnamed Product',
          opening_balance: Number(balanceValue?.trim() || 0),
        };
      });
  };

  const normalizeStoreSegmentsInput = (segments) => {
    if (Array.isArray(segments)) return segments.map((segment) => String(segment || '').trim()).filter(Boolean);
    if (typeof segments === 'string') {
      try {
        const parsed = JSON.parse(segments);
        if (Array.isArray(parsed)) {
          return parsed.map((segment) => String(segment || '').trim()).filter(Boolean);
        }
      } catch {
      }
      return segments.split(',').map((segment) => segment.trim()).filter(Boolean);
    }
    return [];
  };

  const normalizeOpeningBalancesInput = (balances) => {
    if (Array.isArray(balances)) return balances;
    if (typeof balances === 'string') {
      try {
        const parsed = JSON.parse(balances);
        if (Array.isArray(parsed)) return parsed;
      } catch {
      }
    }
    return [];
  };

  const mapProjectToForm = (project) => {
    const openingBalances = normalizeOpeningBalancesInput(project.opening_balances);
    return {
      name: project.name || '',
      store_type: project.store_type || '',
      store_segments: normalizeStoreSegmentsInput(project.store_segments).join(', '),
      branch_location_id: project.branch_location_id || '',
      store_logo_url: project.store_logo_url || '',
      currency_code: project.currency_code || 'INR',
      timezone: project.timezone || 'Asia/Kolkata',
      tax_identification_number: project.tax_identification_number || '',
      default_tax_rate: project.default_tax_rate ?? '',
      low_stock_threshold: project.low_stock_threshold ?? '',
      opening_balances: openingBalances
        .map((balance) => `${balance.product || 'Unnamed Product'}: ${Number(balance.opening_balance || 0)}`)
        .join('\n'),
      owner_admin_email: project.owner_admin_email || '',
      contact_number: project.contact_number || '',
    };
  };

  const resetProjectForm = () => {
    setProjectLogoFileName('');
    setEditingProjectId(null);
    setProjectForm({
      name: '',
      store_type: '',
      store_segments: '',
      branch_location_id: '',
      store_logo_url: '',
      currency_code: 'INR',
      timezone: 'Asia/Kolkata',
      tax_identification_number: '',
      default_tax_rate: '',
      low_stock_threshold: '',
      opening_balances: '',
      owner_admin_email: '',
      contact_number: '',
    });
  };

  const openEditProjectModal = (project) => {
    setEditingProjectId(project.id);
    setProjectLogoFileName('');
    setProjectForm(mapProjectToForm(project));
    setIsEditOpen(true);
  };

  const handleProjectLogoSelect = (event) => {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }

    if (!file.type.startsWith('image/')) {
      alert('Please choose an image file for the store logo.');
      return;
    }

    const reader = new FileReader();
    reader.onload = () => {
      handleProjectFormChange('store_logo_url', typeof reader.result === 'string' ? reader.result : '');
      setProjectLogoFileName(file.name);
    };
    reader.readAsDataURL(file);
  };

  const handleCreateProject = async (event) => {
    event.preventDefault();

    if (!projectForm.name.trim()) {
      alert('Store name is required before creating a new project.');
      return;
    }

    let parsedOpeningBalances = [];
    try {
      parsedOpeningBalances = parseOpeningBalancesFromForm();
    } catch {
      alert('Opening balances should use one product per line in the format Product Name: Quantity');
      return;
    }

    try {
      setIsCreatingProject(true);
      const response = await apiRequest({
        method: 'post',
        url: '/api/projects',
        headers: { 'Authorization': `Bearer ${token}` },
        data: {
          ...projectForm,
          store_segments: projectForm.store_segments
            .split(',')
            .map((segment) => segment.trim())
            .filter(Boolean),
          opening_balances: parsedOpeningBalances,
        },
      });

      setProjects((currentProjects) => [...currentProjects, response.data]);
      setIsCreateOpen(false);
      resetProjectForm();
      alert('Project created successfully.');
      navigate(`/advanced-analytics/${response.data.id}`, { state: { projectName: response.data.name } });
    } catch (error) {
      alert(`Project creation failed: ${error.response?.data?.error || error.message}`);
    } finally {
      setIsCreatingProject(false);
    }
  };

  const handleUpdateProject = async (event) => {
    event.preventDefault();
    if (!editingProjectId) return;

    if (!projectForm.name.trim()) {
      alert('Store name is required before saving changes.');
      return;
    }

    let parsedOpeningBalances = [];
    try {
      parsedOpeningBalances = parseOpeningBalancesFromForm();
    } catch {
      alert('Opening balances should use one product per line in the format Product Name: Quantity');
      return;
    }

    try {
      setIsUpdatingProject(true);
      const response = await apiRequest({
        method: 'patch',
        url: `/api/projects/${editingProjectId}`,
        headers: { 'Authorization': `Bearer ${token}` },
        data: {
          ...projectForm,
          store_segments: normalizeStoreSegmentsInput(projectForm.store_segments),
          opening_balances: parsedOpeningBalances,
        },
      });

      setProjects((currentProjects) => currentProjects.map((project) => (
        String(project.id) === String(editingProjectId) ? response.data : project
      )));
      setIsEditOpen(false);
      resetProjectForm();
      alert('Project details updated successfully.');
    } catch (error) {
      alert(`Project update failed: ${error.response?.data?.error || error.message}`);
    } finally {
      setIsUpdatingProject(false);
    }
  };

  return (
    <div className="p-10 space-y-10 max-w-7xl mx-auto">
      <div className="flex items-center justify-between">
        <div><h1 className="text-4xl font-black tracking-tighter">Managed Stores</h1><p className="text-slate-400 font-bold uppercase text-xs tracking-widest mt-2">Strategic Deployment Hub</p></div>
        <button onClick={() => setIsCreateOpen(true)} className="bg-slate-900 dark:bg-indigo-600 text-white px-8 py-4 rounded-[2rem] font-black shadow-2xl shadow-indigo-600/20 active:scale-95 transition-all">CREATE NEW ENTITY</button>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-8">
        {projects.map((project) => (
          <ProjectCard
            key={project.id}
            project={project}
            isFavorite={favoriteProjectIds.includes(String(project.id))}
            onToggleFavorite={toggleFavoriteProject}
            onEditProject={openEditProjectModal}
          />
        ))}
      </div>

      {isCreateOpen && (
        <div className="fixed inset-0 z-[110] flex items-center justify-center p-6">
          <div className="absolute inset-0 bg-slate-950/60 backdrop-blur-sm" onClick={() => setIsCreateOpen(false)}></div>
          <div className="relative max-h-[90vh] w-full max-w-5xl overflow-y-auto rounded-[2.5rem] border border-slate-200 bg-white p-8 shadow-[0_32px_120px_rgba(15,23,42,0.28)] dark:border-slate-800 dark:bg-slate-900">
            <div className="mb-8 flex items-start justify-between gap-6">
              <div>
                <div className="text-[10px] font-black uppercase tracking-[0.3em] text-indigo-500">New Project</div>
                <h2 className="mt-3 text-3xl font-black tracking-tighter text-slate-900 dark:text-white">Create a new store workspace</h2>
                <p className="mt-2 text-sm font-medium text-slate-500 dark:text-slate-300">Capture the store profile, branch details, tax settings, stock defaults, and owner/admin contacts in one step.</p>
              </div>
              <button onClick={() => setIsCreateOpen(false)} className="rounded-full p-2 text-slate-400 transition-all hover:bg-slate-100 hover:text-slate-900 dark:hover:bg-slate-800 dark:hover:text-white">
                <span className="material-symbols-outlined">close</span>
              </button>
            </div>

            <form onSubmit={handleCreateProject} className="space-y-8">
              <div className="grid grid-cols-1 gap-5 md:grid-cols-2">
                <ProjectFormField label="Store Name" required>
                  <input value={projectForm.name} onChange={(event) => handleProjectFormChange('name', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="ABC Stores" />
                </ProjectFormField>
                <ProjectFormField label="Store Category / Type">
                  <input value={projectForm.store_type} onChange={(event) => handleProjectFormChange('store_type', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="General store, grocery, electronics, apparel" />
                </ProjectFormField>
                <ProjectFormField label="Store Segments">
                  <input value={projectForm.store_segments} onChange={(event) => handleProjectFormChange('store_segments', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Groceries, Electronics, Furniture" />
                </ProjectFormField>
                <ProjectFormField label="Branch / Location ID">
                  <input value={projectForm.branch_location_id} onChange={(event) => handleProjectFormChange('branch_location_id', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="CHN-OMR-001" />
                </ProjectFormField>
                <ProjectFormField label="Store Logo">
                  <div className="space-y-3">
                    <label className="flex min-h-[7rem] cursor-pointer flex-col items-center justify-center rounded-[1.75rem] border border-dashed border-slate-300 bg-slate-50 px-4 py-4 text-center transition-all hover:border-indigo-400 hover:bg-white dark:border-slate-700 dark:bg-slate-800/70 dark:hover:border-indigo-500 dark:hover:bg-slate-800">
                      <input type="file" accept="image/*" onChange={handleProjectLogoSelect} className="hidden" />
                      <span className="material-symbols-outlined text-3xl text-indigo-500">image</span>
                      <span className="mt-2 text-sm font-black text-slate-900 dark:text-white">{projectLogoFileName || 'Upload store logo'}</span>
                      <span className="mt-1 text-xs font-medium text-slate-400">PNG, JPG, WEBP and other image formats are supported</span>
                    </label>
                    {projectForm.store_logo_url && (
                      <div className="overflow-hidden rounded-[1.75rem] border border-slate-200 bg-slate-50 p-2 dark:border-slate-700 dark:bg-slate-800/70">
                        <img src={projectForm.store_logo_url} alt="Store logo preview" className="h-28 w-full rounded-[1.25rem] object-cover" />
                      </div>
                    )}
                  </div>
                </ProjectFormField>
                <ProjectFormField label="Currency">
                  <input value={projectForm.currency_code} onChange={(event) => handleProjectFormChange('currency_code', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="INR" />
                </ProjectFormField>
                <ProjectFormField label="Time Zone">
                  <input value={projectForm.timezone} onChange={(event) => handleProjectFormChange('timezone', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Asia/Kolkata" />
                </ProjectFormField>
                <ProjectFormField label="Tax Identification Number">
                  <input value={projectForm.tax_identification_number} onChange={(event) => handleProjectFormChange('tax_identification_number', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="GSTIN / VAT / TIN" />
                </ProjectFormField>
                <ProjectFormField label="Default Tax Rate (%)">
                  <input value={projectForm.default_tax_rate} onChange={(event) => handleProjectFormChange('default_tax_rate', event.target.value)} type="number" min="0" step="0.01" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="18" />
                </ProjectFormField>
                <ProjectFormField label="Low Stock Threshold">
                  <input value={projectForm.low_stock_threshold} onChange={(event) => handleProjectFormChange('low_stock_threshold', event.target.value)} type="number" min="0" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="10" />
                </ProjectFormField>
                <ProjectFormField label="Owner / Admin Email">
                  <input value={projectForm.owner_admin_email} onChange={(event) => handleProjectFormChange('owner_admin_email', event.target.value)} type="email" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="owner@abcstores.com" />
                </ProjectFormField>
                <ProjectFormField label="Contact Number">
                  <input value={projectForm.contact_number} onChange={(event) => handleProjectFormChange('contact_number', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="+91 98765 43210" />
                </ProjectFormField>
              </div>

              <ProjectFormField label="Opening Balances">
                <textarea value={projectForm.opening_balances} onChange={(event) => handleProjectFormChange('opening_balances', event.target.value)} rows={5} className="min-h-[140px] w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder={"Rice Bag: 120\nLED TV: 15\nOffice Chair: 42"} />
                <p className="mt-2 text-xs font-medium text-slate-400">Use one product per line in the format `Product Name: Quantity`.</p>
              </ProjectFormField>

              <div className="flex flex-col-reverse gap-3 sm:flex-row sm:justify-end">
                <button type="button" onClick={() => setIsCreateOpen(false)} className="rounded-2xl border border-slate-200 px-5 py-3 font-bold text-slate-600 transition-all hover:bg-slate-50 dark:border-slate-700 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
                <button type="submit" disabled={isCreatingProject} className="rounded-2xl bg-indigo-600 px-5 py-3 font-black text-white shadow-lg shadow-indigo-600/20 transition-all hover:-translate-y-0.5 hover:bg-indigo-500 disabled:cursor-not-allowed disabled:opacity-60">
                  {isCreatingProject ? 'Creating...' : 'Create Project'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {isEditOpen && (
        <div className="fixed inset-0 z-[110] flex items-center justify-center p-6">
          <div className="absolute inset-0 bg-slate-950/60 backdrop-blur-sm" onClick={() => setIsEditOpen(false)}></div>
          <div className="relative max-h-[90vh] w-full max-w-5xl overflow-y-auto rounded-[2.5rem] border border-slate-200 bg-white p-8 shadow-[0_32px_120px_rgba(15,23,42,0.28)] dark:border-slate-800 dark:bg-slate-900">
            <div className="mb-8 flex items-start justify-between gap-6">
              <div>
                <div className="text-[10px] font-black uppercase tracking-[0.3em] text-indigo-500">Edit Project</div>
                <h2 className="mt-3 text-3xl font-black tracking-tighter text-slate-900 dark:text-white">Update store details</h2>
                <p className="mt-2 text-sm font-medium text-slate-500 dark:text-slate-300">Edit your store profile, tax settings, contacts, and opening balances.</p>
              </div>
              <button onClick={() => setIsEditOpen(false)} className="rounded-full p-2 text-slate-400 transition-all hover:bg-slate-100 hover:text-slate-900 dark:hover:bg-slate-800 dark:hover:text-white">
                <span className="material-symbols-outlined">close</span>
              </button>
            </div>

            <form onSubmit={handleUpdateProject} className="space-y-8">
              <div className="grid grid-cols-1 gap-5 md:grid-cols-2">
                <ProjectFormField label="Store Name" required>
                  <input value={projectForm.name} onChange={(event) => handleProjectFormChange('name', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="ABC Stores" />
                </ProjectFormField>
                <ProjectFormField label="Store Category / Type">
                  <input value={projectForm.store_type} onChange={(event) => handleProjectFormChange('store_type', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="General store, grocery, electronics, apparel" />
                </ProjectFormField>
                <ProjectFormField label="Store Segments">
                  <input value={projectForm.store_segments} onChange={(event) => handleProjectFormChange('store_segments', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Groceries, Electronics, Furniture" />
                </ProjectFormField>
                <ProjectFormField label="Branch / Location ID">
                  <input value={projectForm.branch_location_id} onChange={(event) => handleProjectFormChange('branch_location_id', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="CHN-OMR-001" />
                </ProjectFormField>
                <ProjectFormField label="Store Logo">
                  <div className="space-y-3">
                    <label className="flex min-h-[7rem] cursor-pointer flex-col items-center justify-center rounded-[1.75rem] border border-dashed border-slate-300 bg-slate-50 px-4 py-4 text-center transition-all hover:border-indigo-400 hover:bg-white dark:border-slate-700 dark:bg-slate-800/70 dark:hover:border-indigo-500 dark:hover:bg-slate-800">
                      <input type="file" accept="image/*" onChange={handleProjectLogoSelect} className="hidden" />
                      <span className="material-symbols-outlined text-3xl text-indigo-500">image</span>
                      <span className="mt-2 text-sm font-black text-slate-900 dark:text-white">{projectLogoFileName || 'Upload store logo'}</span>
                      <span className="mt-1 text-xs font-medium text-slate-400">PNG, JPG, WEBP and other image formats are supported</span>
                    </label>
                    {projectForm.store_logo_url && (
                      <div className="overflow-hidden rounded-[1.75rem] border border-slate-200 bg-slate-50 p-2 dark:border-slate-700 dark:bg-slate-800/70">
                        <img src={projectForm.store_logo_url} alt="Store logo preview" className="h-28 w-full rounded-[1.25rem] object-cover" />
                      </div>
                    )}
                  </div>
                </ProjectFormField>
                <ProjectFormField label="Currency">
                  <input value={projectForm.currency_code} onChange={(event) => handleProjectFormChange('currency_code', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="INR" />
                </ProjectFormField>
                <ProjectFormField label="Time Zone">
                  <input value={projectForm.timezone} onChange={(event) => handleProjectFormChange('timezone', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Asia/Kolkata" />
                </ProjectFormField>
                <ProjectFormField label="Tax Identification Number">
                  <input value={projectForm.tax_identification_number} onChange={(event) => handleProjectFormChange('tax_identification_number', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="GSTIN / VAT / TIN" />
                </ProjectFormField>
                <ProjectFormField label="Default Tax Rate (%)">
                  <input value={projectForm.default_tax_rate} onChange={(event) => handleProjectFormChange('default_tax_rate', event.target.value)} type="number" min="0" step="0.01" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="18" />
                </ProjectFormField>
                <ProjectFormField label="Low Stock Threshold">
                  <input value={projectForm.low_stock_threshold} onChange={(event) => handleProjectFormChange('low_stock_threshold', event.target.value)} type="number" min="0" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="8" />
                </ProjectFormField>
                <ProjectFormField label="Owner / Admin Email">
                  <input value={projectForm.owner_admin_email} onChange={(event) => handleProjectFormChange('owner_admin_email', event.target.value)} type="email" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="owner@example.com" />
                </ProjectFormField>
                <ProjectFormField label="Contact Number">
                  <input value={projectForm.contact_number} onChange={(event) => handleProjectFormChange('contact_number', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="+91 98765 43210" />
                </ProjectFormField>
              </div>

              <ProjectFormField label="Opening Balances">
                <textarea value={projectForm.opening_balances} onChange={(event) => handleProjectFormChange('opening_balances', event.target.value)} rows={5} className="min-h-[140px] w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder={"Rice Bag: 120\nLED TV: 15\nOffice Chair: 42"} />
                <p className="mt-2 text-xs font-medium text-slate-400">Use one product per line in the format `Product Name: Quantity`.</p>
              </ProjectFormField>

              <div className="flex flex-col-reverse gap-3 sm:flex-row sm:justify-end">
                <button type="button" onClick={() => setIsEditOpen(false)} className="rounded-2xl border border-slate-200 px-5 py-3 font-bold text-slate-600 transition-all hover:bg-slate-50 dark:border-slate-700 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
                <button type="submit" disabled={isUpdatingProject} className="rounded-2xl bg-indigo-600 px-5 py-3 font-black text-white shadow-lg shadow-indigo-600/20 transition-all hover:-translate-y-0.5 hover:bg-indigo-500 disabled:cursor-not-allowed disabled:opacity-60">
                  {isUpdatingProject ? 'Saving...' : 'Save Changes'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}

function FavoritesList() {
  const [projects, setProjects] = useState([]);
  const [favoriteProjectIds, setFavoriteProjectIds] = useState(() => getStoredFavoriteProjectIds());
  const token = localStorage.getItem('token');

  useEffect(() => {
    apiRequest({
      method: 'get',
      url: '/api/projects',
      headers: { 'Authorization': `Bearer ${token}` }
    }).then(res => setProjects(res.data)).catch(err => console.error('Error fetching projects:', err));
  }, [token]);

  const toggleFavoriteProject = (projectId) => {
    setFavoriteProjectIds((currentFavorites) => {
      const normalizedId = String(projectId);
      const nextFavorites = currentFavorites.includes(normalizedId)
        ? currentFavorites.filter((favoriteId) => favoriteId !== normalizedId)
        : [...currentFavorites, normalizedId];
      setStoredFavoriteProjectIds(nextFavorites);
      return nextFavorites;
    });
  };

  const favoriteProjects = projects.filter((project) => favoriteProjectIds.includes(String(project.id)));

  return (
    <div className="p-10 space-y-10 max-w-7xl mx-auto">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-4xl font-black tracking-tighter">Favorites</h1>
          <p className="text-slate-400 font-bold uppercase text-xs tracking-widest mt-2">Pinned Store Workspaces</p>
        </div>
        <div className="rounded-[2rem] border border-amber-200 bg-amber-50 px-6 py-3 text-xs font-black uppercase tracking-[0.2em] text-amber-600 dark:border-amber-900/30 dark:bg-amber-950/20 dark:text-amber-300">
          {favoriteProjects.length} favorite {favoriteProjects.length === 1 ? 'project' : 'projects'}
        </div>
      </div>

      {favoriteProjects.length > 0 ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-8">
          {favoriteProjects.map((project) => (
            <ProjectCard
              key={project.id}
              project={project}
              isFavorite={true}
              onToggleFavorite={toggleFavoriteProject}
            />
          ))}
        </div>
      ) : (
        <div className="rounded-[3rem] border border-dashed border-slate-300 bg-white p-12 text-center shadow-sm dark:border-slate-700 dark:bg-slate-900">
          <div className="mx-auto flex h-20 w-20 items-center justify-center rounded-full bg-amber-50 text-amber-500 dark:bg-amber-950/30 dark:text-amber-300">
            <span className="material-symbols-outlined text-4xl">star</span>
          </div>
          <h2 className="mt-6 text-2xl font-black tracking-tight text-slate-900 dark:text-white">No favorite projects yet</h2>
          <p className="mt-3 text-sm font-medium text-slate-500 dark:text-slate-300">
            Open `My Projects` and click the star on any store card to pin it here.
          </p>
        </div>
      )}
    </div>
  );
}

function ReportsPage() {
  const [reports, setReports] = useState([]);
  const [isLoadingReports, setIsLoadingReports] = useState(true);
  const token = localStorage.getItem('token');

  useEffect(() => {
    const fetchReports = async () => {
      try {
        setIsLoadingReports(true);
        const projectsResponse = await apiRequest({
          method: 'get',
          url: '/api/projects',
          headers: { 'Authorization': `Bearer ${token}` }
        });

        const projects = Array.isArray(projectsResponse.data) ? projectsResponse.data : [];
        const reportRows = await Promise.all(projects.map(async (project) => {
          try {
            const analyticsResponse = await apiRequest({
              method: 'get',
              url: `/api/dashboard/${project.id}`,
              headers: { 'Authorization': `Bearer ${token}` }
            });
            return buildProjectReport(project, analyticsResponse.data);
          } catch (error) {
            console.error(`Error fetching report data for project ${project.id}:`, error);
            return buildProjectReport(project, []);
          }
        }));

        setReports(reportRows.filter((report) => Array.isArray(report.rows) && report.rows.length > 0));
      } catch (error) {
        console.error('Error preparing reports:', error);
        setReports([]);
      } finally {
        setIsLoadingReports(false);
      }
    };

    fetchReports();
  }, [token]);

  return (
    <div className="p-10 space-y-10 max-w-7xl mx-auto">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <h1 className="text-4xl font-black tracking-tighter">Reports</h1>
          <p className="mt-2 text-slate-400 font-bold uppercase text-xs tracking-widest">Past Performance, Present Conditions, Future Forecast, SWOT</p>
        </div>
        <div className="rounded-[2rem] border border-sky-200 bg-sky-50 px-6 py-3 text-xs font-black uppercase tracking-[0.2em] text-sky-600 dark:border-sky-900/30 dark:bg-sky-950/20 dark:text-sky-300">
          {reports.length} generated store {reports.length === 1 ? 'report' : 'reports'}
        </div>
      </div>

      {isLoadingReports ? (
        <div className="rounded-[3rem] border border-slate-200 bg-white p-12 text-center shadow-sm dark:border-slate-800 dark:bg-slate-900">
          <div className="text-lg font-black text-slate-900 dark:text-white">Preparing reports...</div>
          <p className="mt-2 text-sm font-medium text-slate-500 dark:text-slate-300">Analyzing advanced analytics data for every project.</p>
        </div>
      ) : reports.length > 0 ? (
        <div className="space-y-8">
          {reports.map((report) => (
            <div key={report.project.id} className="rounded-[3rem] border border-slate-100 bg-white p-8 shadow-xl dark:border-slate-800 dark:bg-slate-900">
              <div className="flex flex-col gap-5 xl:flex-row xl:items-start xl:justify-between">
                <div>
                  <div className="text-[10px] font-black uppercase tracking-[0.3em] text-indigo-500">Store Report</div>
                  <h2 className="mt-3 text-3xl font-black tracking-tighter text-slate-900 dark:text-white">{report.project.name}</h2>
                  <p className="mt-2 max-w-3xl text-sm font-medium text-slate-500 dark:text-slate-300">
                    This report summarizes the advanced analytics signals for the store and translates them into executive-friendly insights.
                  </p>
                </div>
                <button
                  onClick={() => downloadProjectReportPdf(report)}
                  className="inline-flex items-center justify-center gap-2 rounded-2xl bg-slate-900 px-5 py-3 text-sm font-black text-white shadow-lg transition-all hover:-translate-y-0.5 hover:bg-slate-800 dark:bg-indigo-600 dark:hover:bg-indigo-500"
                >
                  <span className="material-symbols-outlined text-sm">picture_as_pdf</span>
                  Download PDF
                </button>
              </div>

              <div className="mt-8 grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
                <AnalyticsStat title="Revenue" value={formatInrCompact(report.totals.revenue)} sub="historical total" />
                <AnalyticsStat title="Profit" value={formatInrCompact(report.totals.profit)} sub={`${report.margin.toFixed(1)}% margin`} />
                <AnalyticsStat title="Forecast" value={formatInrCompact(report.forecastRevenue)} sub="projected next cycle" />
                <AnalyticsStat title="Periods" value={report.rows.length} sub="uploaded reporting rows" />
              </div>

              <div className="mt-8 grid grid-cols-1 gap-8 xl:grid-cols-[1fr_1fr]">
                <AnalyticsPanel title="Executive Summary" subtitle="Past, present, and future outlook">
                  <div className="space-y-4">
                    {report.highlights.map((line) => (
                      <div key={line} className="rounded-2xl border border-slate-100 bg-slate-50 px-5 py-4 text-sm font-medium text-slate-600 dark:border-slate-800 dark:bg-slate-800/60 dark:text-slate-200">
                        {line}
                      </div>
                    ))}
                  </div>
                </AnalyticsPanel>

                <AnalyticsPanel title="SWOT Analysis" subtitle="Operational strengths, risks, and opportunities">
                  <div className="space-y-4">
                    <div className="rounded-2xl border border-emerald-100 bg-emerald-50 px-5 py-4 text-sm font-medium text-emerald-700 dark:border-emerald-900/30 dark:bg-emerald-950/20 dark:text-emerald-300">
                      <span className="block text-[10px] font-black uppercase tracking-[0.2em]">Strengths</span>
                      <span className="mt-2 block">{report.strengths.join(' ')}</span>
                    </div>
                    <div className="rounded-2xl border border-rose-100 bg-rose-50 px-5 py-4 text-sm font-medium text-rose-700 dark:border-rose-900/30 dark:bg-rose-950/20 dark:text-rose-300">
                      <span className="block text-[10px] font-black uppercase tracking-[0.2em]">Weaknesses</span>
                      <span className="mt-2 block">{report.weaknesses.join(' ')}</span>
                    </div>
                    <div className="rounded-2xl border border-sky-100 bg-sky-50 px-5 py-4 text-sm font-medium text-sky-700 dark:border-sky-900/30 dark:bg-sky-950/20 dark:text-sky-300">
                      <span className="block text-[10px] font-black uppercase tracking-[0.2em]">Opportunities</span>
                      <span className="mt-2 block">{report.opportunities.join(' ')}</span>
                    </div>
                    <div className="rounded-2xl border border-amber-100 bg-amber-50 px-5 py-4 text-sm font-medium text-amber-700 dark:border-amber-900/30 dark:bg-amber-950/20 dark:text-amber-300">
                      <span className="block text-[10px] font-black uppercase tracking-[0.2em]">Threats</span>
                      <span className="mt-2 block">{report.threats.join(' ')}</span>
                    </div>
                  </div>
                </AnalyticsPanel>
              </div>

              <div className="mt-8 grid grid-cols-1 gap-8 xl:grid-cols-[1.15fr_0.85fr]">
                <AnalyticsPanel title="Revenue & Profit Trend" subtitle="Monthly movement from uploaded report rows">
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <ComposedChart data={report.trendSeries}>
                        <CartesianGrid vertical={false} strokeDasharray="3 3" opacity={0.15} />
                        <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{ fontSize: 11, fontWeight: 700 }} />
                        <YAxis yAxisId="left" axisLine={false} tickLine={false} tick={{ fontSize: 11 }} />
                        <YAxis yAxisId="right" orientation="right" axisLine={false} tickLine={false} tick={{ fontSize: 11 }} />
                        <Tooltip formatter={(value, name) => [formatInrCompact(value), name]} />
                        <Legend />
                        <Bar yAxisId="left" dataKey="revenue" name="Revenue" fill="#38bdf8" radius={[10, 10, 0, 0]} />
                        <Line yAxisId="right" type="monotone" dataKey="profit" name="Profit" stroke="#f59e0b" strokeWidth={3} dot={{ r: 4 }} />
                      </ComposedChart>
                    </ResponsiveContainer>
                  </div>
                </AnalyticsPanel>

                <AnalyticsPanel title="Category Share" subtitle="Revenue contribution by category">
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <PieChart>
                        <Pie data={report.categorySeries} dataKey="value" nameKey="name" innerRadius={55} outerRadius={90} paddingAngle={4}>
                          {report.categorySeries.map((entry) => <Cell key={`${report.project.id}-${entry.name}`} fill={entry.color} />)}
                        </Pie>
                        <Tooltip formatter={(value, name) => [formatInrCompact(value), name]} />
                        <Legend />
                      </PieChart>
                    </ResponsiveContainer>
                  </div>
                </AnalyticsPanel>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className="rounded-[3rem] border border-dashed border-slate-300 bg-white p-12 text-center shadow-sm dark:border-slate-700 dark:bg-slate-900">
          <div className="mx-auto flex h-20 w-20 items-center justify-center rounded-full bg-sky-50 text-sky-500 dark:bg-sky-950/30 dark:text-sky-300">
            <span className="material-symbols-outlined text-4xl">description</span>
          </div>
          <h2 className="mt-6 text-2xl font-black tracking-tight text-slate-900 dark:text-white">No reports available yet</h2>
          <p className="mt-3 text-sm font-medium text-slate-500 dark:text-slate-300">
            Create a project, upload store data, and the reports page will summarize the advanced analytics into downloadable PDFs.
          </p>
        </div>
      )}
    </div>
  );
}

function SettingsPage({ user }) {
  const [settings, setSettings] = useState(() => {
    try {
      return {
        display_name: user?.name || 'Administrator',
        email: user?.email || '',
        workspace_name: 'Analytics Pro Workspace',
        default_currency: localStorage.getItem('settings_default_currency') || 'INR',
        default_timezone: localStorage.getItem('settings_default_timezone') || 'Asia/Kolkata',
        weekly_reports: localStorage.getItem('settings_weekly_reports') !== 'false',
        report_alerts: localStorage.getItem('settings_report_alerts') !== 'false',
        low_stock_alerts: localStorage.getItem('settings_low_stock_alerts') !== 'false',
        ai_summaries: localStorage.getItem('settings_ai_summaries') !== 'false',
      };
    } catch {
      return {
        display_name: user?.name || 'Administrator',
        email: user?.email || '',
        workspace_name: 'Analytics Pro Workspace',
        default_currency: 'INR',
        default_timezone: 'Asia/Kolkata',
        weekly_reports: true,
        report_alerts: true,
        low_stock_alerts: true,
        ai_summaries: true,
      };
    }
  });

  const updateSetting = (field, value) => {
    setSettings((current) => ({ ...current, [field]: value }));
  };

  const handleSaveSettings = () => {
    localStorage.setItem('settings_default_currency', settings.default_currency);
    localStorage.setItem('settings_default_timezone', settings.default_timezone);
    localStorage.setItem('settings_weekly_reports', String(settings.weekly_reports));
    localStorage.setItem('settings_report_alerts', String(settings.report_alerts));
    localStorage.setItem('settings_low_stock_alerts', String(settings.low_stock_alerts));
    localStorage.setItem('settings_ai_summaries', String(settings.ai_summaries));
    alert('Settings saved successfully.');
  };

  return (
    <div className="p-10 space-y-10 max-w-7xl mx-auto">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <h1 className="text-4xl font-black tracking-tighter">Settings</h1>
          <p className="mt-2 text-slate-400 font-bold uppercase text-xs tracking-widest">Workspace Preferences And Account Controls</p>
        </div>
        <button
          onClick={handleSaveSettings}
          className="inline-flex items-center justify-center gap-2 rounded-[2rem] bg-slate-900 px-6 py-3 text-sm font-black text-white shadow-lg transition-all hover:-translate-y-0.5 hover:bg-slate-800 dark:bg-indigo-600 dark:hover:bg-indigo-500"
        >
          <span className="material-symbols-outlined text-sm">save</span>
          Save Settings
        </button>
      </div>

      <div className="grid grid-cols-1 gap-8 xl:grid-cols-[1.05fr_0.95fr]">
        <AnalyticsPanel title="Profile" subtitle="Account and workspace identity">
          <div className="grid grid-cols-1 gap-5 md:grid-cols-2">
            <ProjectFormField label="Display Name">
              <input value={settings.display_name} onChange={(event) => updateSetting('display_name', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" />
            </ProjectFormField>
            <ProjectFormField label="Email">
              <input value={settings.email} onChange={(event) => updateSetting('email', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" />
            </ProjectFormField>
            <ProjectFormField label="Workspace Name">
              <input value={settings.workspace_name} onChange={(event) => updateSetting('workspace_name', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" />
            </ProjectFormField>
            <ProjectFormField label="Default Currency">
              <input value={settings.default_currency} onChange={(event) => updateSetting('default_currency', event.target.value.toUpperCase())} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" />
            </ProjectFormField>
            <ProjectFormField label="Default Timezone">
              <input value={settings.default_timezone} onChange={(event) => updateSetting('default_timezone', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" />
            </ProjectFormField>
          </div>
        </AnalyticsPanel>

        <AnalyticsPanel title="Notifications" subtitle="Choose which updates you want to receive">
          <div className="space-y-4">
            <SettingsToggle
              title="Weekly Reports"
              description="Receive a scheduled weekly summary for your store analytics."
              checked={settings.weekly_reports}
              onChange={(value) => updateSetting('weekly_reports', value)}
            />
            <SettingsToggle
              title="Report Alerts"
              description="Get notified when new project reports and PDF summaries are ready."
              checked={settings.report_alerts}
              onChange={(value) => updateSetting('report_alerts', value)}
            />
            <SettingsToggle
              title="Low Stock Alerts"
              description="Highlight inventory thresholds configured during project setup."
              checked={settings.low_stock_alerts}
              onChange={(value) => updateSetting('low_stock_alerts', value)}
            />
            <SettingsToggle
              title="AI Summaries"
              description="Enable AI-generated executive summaries across dashboards and reports."
              checked={settings.ai_summaries}
              onChange={(value) => updateSetting('ai_summaries', value)}
            />
          </div>
        </AnalyticsPanel>
      </div>

      <div className="grid grid-cols-1 gap-8 xl:grid-cols-3">
        <AnalyticsPanel title="Security" subtitle="Recommended protection settings">
          <div className="space-y-4 text-sm font-medium text-slate-600 dark:text-slate-300">
            <div className="rounded-2xl border border-slate-100 bg-slate-50 px-5 py-4 dark:border-slate-800 dark:bg-slate-800/60">
              <div className="font-black text-slate-900 dark:text-white">Password</div>
              <div className="mt-1">Use a strong password and rotate it periodically.</div>
            </div>
            <div className="rounded-2xl border border-slate-100 bg-slate-50 px-5 py-4 dark:border-slate-800 dark:bg-slate-800/60">
              <div className="font-black text-slate-900 dark:text-white">Session Access</div>
              <div className="mt-1">Log out of shared machines after working with reports and uploads.</div>
            </div>
          </div>
        </AnalyticsPanel>

        <AnalyticsPanel title="Workspace" subtitle="Default behavior for new projects">
          <div className="space-y-4 text-sm font-medium text-slate-600 dark:text-slate-300">
            <div className="rounded-2xl border border-slate-100 bg-slate-50 px-5 py-4 dark:border-slate-800 dark:bg-slate-800/60">
              <div className="font-black text-slate-900 dark:text-white">Project Creation</div>
              <div className="mt-1">New projects inherit your saved currency and timezone preferences.</div>
            </div>
            <div className="rounded-2xl border border-slate-100 bg-slate-50 px-5 py-4 dark:border-slate-800 dark:bg-slate-800/60">
              <div className="font-black text-slate-900 dark:text-white">Imports</div>
              <div className="mt-1">Uploads continue to use the selected project, month, and year before import.</div>
            </div>
          </div>
        </AnalyticsPanel>

        <AnalyticsPanel title="Support" subtitle="General help and maintenance">
          <div className="space-y-4 text-sm font-medium text-slate-600 dark:text-slate-300">
            <div className="rounded-2xl border border-sky-100 bg-sky-50 px-5 py-4 dark:border-sky-900/30 dark:bg-sky-950/20 dark:text-sky-300">
              <div className="font-black text-slate-900 dark:text-white">Tips</div>
              <div className="mt-1">Keep your project logos, tax defaults, and opening balances updated for cleaner reports.</div>
            </div>
            <div className="rounded-2xl border border-amber-100 bg-amber-50 px-5 py-4 dark:border-amber-900/30 dark:bg-amber-950/20 dark:text-amber-300">
              <div className="font-black text-slate-900 dark:text-white">Maintenance</div>
              <div className="mt-1">Large PDF exports can increase page load time until bundle splitting is optimized.</div>
            </div>
          </div>
        </AnalyticsPanel>
      </div>
    </div>
  );
}

function StoreDetailAnalytics() {
  const { projectId } = useParams();
  const [data, setData] = useState([]);
  const token = localStorage.getItem('token');

  useEffect(() => {
    apiRequest({
      method: 'get',
      url: `/api/dashboard/${projectId}`,
      headers: { 'Authorization': `Bearer ${token}` }
    }).then(res => setData(res.data)).catch(err => console.error('Error fetching store data:', err));
  }, [projectId, token]);

  const latest = data[data.length - 1] || {};
  const regionData = latest.region_data ? Object.entries(latest.region_data).map(([name, value]) => ({ name, value })) : [];

  const handleDeleteData = async () => {
    if (confirm("Discard all store data?")) {
      try {
        await apiRequest({
          method: 'delete',
          url: `/api/dashboard/${projectId}`,
          headers: { 'Authorization': `Bearer ${token}` }
        });
        window.location.reload();
      } catch (err) {
        alert("Failed to delete data: " + (err.response?.data?.error || err.message));
      }
    }
  };

  return (
    <div className="p-10 space-y-10 max-w-7xl mx-auto">
      <div className="flex justify-between items-center border-b pb-8 border-slate-100 dark:border-slate-800">
         <div>
            <h1 className="text-4xl font-black tracking-tighter uppercase">Store Intelligence</h1>
            <div className="flex items-center gap-2 mt-2">
               <div className="w-2 h-2 rounded-full bg-emerald-500"></div>
               <span className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Node SID-00{projectId} | Connected</span>
            </div>
         </div>
         <button onClick={handleDeleteData} className="px-6 py-3 border border-rose-100 text-rose-500 font-black text-xs rounded-2xl hover:bg-rose-50 transition-all uppercase tracking-widest">Discard Hub Records</button>
      </div>
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-10">
         <ChartCard title="Regional Velocity" icon="public" color="blue">
            <div className="h-80"><ResponsiveContainer width="100%" height="100%"><PieChart><Pie data={regionData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={120} stroke="none">{regionData.map((e, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}</Pie><Tooltip /><Legend /></PieChart></ResponsiveContainer></div>
         </ChartCard>
         <ChartCard title="Revenue Growth" icon="trending_up" color="emerald">
            <div className="h-80">
               <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={data}>
                     <XAxis dataKey="month_name" axisLine={false} tickLine={false} tick={{fontSize: 10, fontWeight: 900}} />
                     <Tooltip contentStyle={{borderRadius: '20px', border: 'none', background: '#0f172a', color: '#fff'}} />
                     <Bar dataKey="total_revenue" fill="#10b981" radius={[10, 10, 0, 0]} />
                  </BarChart>
               </ResponsiveContainer>
            </div>
         </ChartCard>
      </div>
    </div>
  );
}

function ChartCard({ title, icon, color, children }) {
  return (
    <div className="bg-white dark:bg-slate-900 p-10 rounded-[3rem] shadow-xl border border-slate-100 dark:border-slate-800 transition-all hover:shadow-2xl hover:shadow-slate-200/50 dark:hover:shadow-none relative group">
       <div className="flex items-center justify-between mb-10">
          <h3 className="font-black text-[10px] opacity-40 uppercase tracking-[4px] flex items-center gap-3">
             <span className="material-symbols-outlined text-primary group-hover:rotate-12 transition-transform">{icon}</span> {title}
          </h3>
          <span className="material-symbols-outlined text-slate-200">more_horiz</span>
       </div>
       {children}
    </div>
  );
}

function buildEntriesFromProjectData(projectRows) {
  const normalizedRows = Array.isArray(projectRows) ? projectRows : [];
  const derivedEntries = normalizedRows.flatMap((row, rowIndex) => {
    const monthName = row.month_name || 'January';
    const yearValue = Number(row.year || new Date().getFullYear());
    const recordedAt = `${yearValue}-${String(rowIndex + 1).padStart(2, '0')}-01T09:00:00`;
    const detailedEntries = Array.isArray(row.detailed_entries) ? row.detailed_entries : [];

    if (detailedEntries.length > 0) {
      return detailedEntries.map((detail, detailIndex) => ({
        id: `${row.id || `${monthName}-${yearValue}`}-detail-${detailIndex}`,
        label: String(detail.product || row.top_product || `Product ${detailIndex + 1}`).trim(),
        category: String(detail.category || 'General').trim() || 'General',
        region: normalizeMapStateName(String(detail.region || row.top_region || 'Unknown').trim() || 'Unknown'),
        month: monthName,
        year: yearValue,
        revenue: Number(detail.revenue || 0),
        cost: Number(detail.cost || 0),
        quantity: Number(detail.quantity || 0),
        recordedAt,
      }));
    }

    const regionEntries = Object.entries(row.region_data || {});
    if (regionEntries.length > 0) {
      const categoryEntries = Object.entries(row.category_data || {});
      const topCategory = categoryEntries.sort(([, leftValue], [, rightValue]) => Number(rightValue || 0) - Number(leftValue || 0))[0]?.[0] || 'General';
      const totalRevenue = regionEntries.reduce((sum, [, value]) => sum + Number(value || 0), 0) || 1;
      const totalCost = Number(row.total_cost || 0);
      const totalQuantity = Number(row.total_quantity || 0);

      return regionEntries.map(([regionName, regionRevenue], regionIndex) => {
        const revenue = Number(regionRevenue || 0);
        const share = revenue / totalRevenue;
        return {
          id: `${row.id || `${monthName}-${yearValue}`}-region-${regionIndex}`,
          label: String(row.top_product || 'General Product').trim(),
          category: topCategory,
          region: normalizeMapStateName(String(regionName || row.top_region || 'Unknown').trim() || 'Unknown'),
          month: monthName,
          year: yearValue,
          revenue,
          cost: Math.round(totalCost * share),
          quantity: Math.max(1, Math.round(totalQuantity * share)),
          recordedAt,
        };
      });
    }

    const categoryEntries = Object.entries(row.category_data || {});
    if (categoryEntries.length > 0) {
      const totalCategoryRevenue = categoryEntries.reduce((sum, [, value]) => sum + Number(value || 0), 0) || 1;
      return categoryEntries.map(([categoryName, categoryRevenue], categoryIndex) => {
        const revenue = Number(categoryRevenue || 0);
        const revenueShare = revenue / totalCategoryRevenue;
        return {
          id: `${row.id || `${monthName}-${yearValue}`}-category-${categoryIndex}`,
          label: String(row.top_product || categoryName || `Product ${categoryIndex + 1}`).trim(),
          category: String(categoryName || 'General').trim() || 'General',
          region: normalizeMapStateName(String(row.top_region || 'Unknown').trim() || 'Unknown'),
          month: monthName,
          year: yearValue,
          revenue,
          cost: Math.round(Number(row.total_cost || 0) * revenueShare),
          quantity: Math.max(1, Math.round(Number(row.total_quantity || 0) * revenueShare)),
          recordedAt,
        };
      });
    }

    return [{
      id: `${row.id || `${monthName}-${yearValue}`}-summary`,
      label: String(row.top_product || 'General Product').trim(),
      category: 'General',
      region: normalizeMapStateName(String(row.top_region || 'Unknown').trim() || 'Unknown'),
      month: monthName,
      year: yearValue,
      revenue: Number(row.total_revenue || 0),
      cost: Number(row.total_cost || 0),
      quantity: Number(row.total_quantity || 0),
      recordedAt,
    }];
  });

  return derivedEntries;
}

function getAnalyticsDimension(entry, dimension) {
  if (dimension === 'month') return entry.month || new Date(entry.recordedAt).toLocaleString('en-US', { month: 'long' });
  if (dimension === 'year') return String(entry.year || new Date(entry.recordedAt).getFullYear());
  if (dimension === 'category') return entry.category;
  if (dimension === 'region') return entry.region;
  return entry.label;
}

function getAnalyticsMetric(entry, metric) {
  if (metric === 'cost') return Number(entry.cost || 0);
  if (metric === 'quantity') return Number(entry.quantity || 0);
  if (metric === 'profit') return Number(entry.revenue || 0) - Number(entry.cost || 0);
  return Number(entry.revenue || 0);
}

function aggregateEntriesForAxis(entries, xAxis, yAxis) {
  const groupedEntries = entries.reduce((groups, entry) => {
    const dimensionValue = getAnalyticsDimension(entry, xAxis);
    groups[dimensionValue] = (groups[dimensionValue] || 0) + getAnalyticsMetric(entry, yAxis);
    return groups;
  }, {});

  return Object.entries(groupedEntries)
    .map(([name, value], index) => ({
      name,
      value,
      fill: COLORS[index % COLORS.length],
    }))
    .sort((leftEntry, rightEntry) => {
      if (xAxis === 'month') {
        return MONTH_ORDER.indexOf(leftEntry.name) - MONTH_ORDER.indexOf(rightEntry.name);
      }
      if (xAxis === 'year') {
        return Number(leftEntry.name) - Number(rightEntry.name);
      }
      return leftEntry.name.localeCompare(rightEntry.name);
    });
}

function buildHistogramData(entries, yAxis) {
  const metricValues = entries.map((entry) => getAnalyticsMetric(entry, yAxis)).filter((value) => Number.isFinite(value));
  const maxValue = metricValues.length ? Math.max(...metricValues) : 0;
  const bucketSize = maxValue > 0 ? Math.max(1, Math.ceil(maxValue / 5)) : 1;

  return Array.from({ length: 5 }, (_, index) => {
    const min = index * bucketSize;
    const max = index === 4 ? Number.POSITIVE_INFINITY : (index + 1) * bucketSize;
    const count = metricValues.filter((value) => value >= min && value < max).length;

    return {
      name: max === Number.POSITIVE_INFINITY ? `${formatInrCompact(min)}+` : `${formatInrCompact(min)}-${formatInrCompact(max)}`,
      count,
    };
  });
}

function buildProjectReport(project, projectRows) {
  const rows = Array.isArray(projectRows) ? projectRows : [];
  const totals = rows.reduce((summary, row) => ({
    revenue: summary.revenue + Number(row.total_revenue || 0),
    cost: summary.cost + Number(row.total_cost || 0),
    profit: summary.profit + Number(row.net_revenue || (Number(row.total_revenue || 0) - Number(row.total_cost || 0))),
    quantity: summary.quantity + Number(row.total_quantity || 0),
  }), { revenue: 0, cost: 0, profit: 0, quantity: 0 });

  const sortedRows = [...rows].sort((leftRow, rightRow) => {
    if (leftRow.year !== rightRow.year) return Number(leftRow.year) - Number(rightRow.year);
    return MONTH_ORDER.indexOf(leftRow.month_name) - MONTH_ORDER.indexOf(rightRow.month_name);
  });

  const previousRevenue = sortedRows.length > 1 ? Number(sortedRows[sortedRows.length - 2].total_revenue || 0) : 0;
  const currentRevenue = sortedRows.length > 0 ? Number(sortedRows[sortedRows.length - 1].total_revenue || 0) : totals.revenue;
  const growthRate = previousRevenue > 0 ? ((currentRevenue - previousRevenue) / previousRevenue) * 100 : 0;
  const forecastRevenue = Math.round(currentRevenue * (1 + (growthRate / 100 || 0.08)));

  const categoryTotals = rows.reduce((accumulator, row) => {
    Object.entries(row.category_data || {}).forEach(([name, value]) => {
      accumulator[name] = (accumulator[name] || 0) + Number(value || 0);
    });
    return accumulator;
  }, {});

  const regionTotals = rows.reduce((accumulator, row) => {
    Object.entries(row.region_data || {}).forEach(([name, value]) => {
      accumulator[name] = (accumulator[name] || 0) + Number(value || 0);
    });
    return accumulator;
  }, {});

  const topCategory = Object.entries(categoryTotals).sort((leftEntry, rightEntry) => rightEntry[1] - leftEntry[1])[0];
  const topRegion = Object.entries(regionTotals).sort((leftEntry, rightEntry) => rightEntry[1] - leftEntry[1])[0];
  const margin = totals.revenue > 0 ? (totals.profit / totals.revenue) * 100 : 0;
  const trendSeries = sortedRows.map((row) => ({
    name: `${String(row.month_name || '').slice(0, 3)} ${String(row.year || '').slice(-2)}`,
    revenue: Number(row.total_revenue || 0),
    profit: Number(row.net_revenue || (Number(row.total_revenue || 0) - Number(row.total_cost || 0))),
  }));
  const categorySeries = Object.entries(categoryTotals)
    .map(([name, value], index) => ({ name, value: Number(value || 0), color: COLORS[index % COLORS.length] }))
    .sort((leftEntry, rightEntry) => rightEntry.value - leftEntry.value);

  const strengths = [
    topCategory ? `${topCategory[0]} leads category revenue at ${formatInrCompact(topCategory[1])}.` : 'Revenue mix is available for business review.',
    topRegion ? `${topRegion[0]} is the strongest operating region.` : 'Regional footprint is ready for heat-map analysis.',
  ];
  const weaknesses = [
    margin < 15 ? `Net margin is only ${margin.toFixed(1)}%, which signals cost pressure.` : `Margin is healthy, but cost discipline should still be monitored.`,
    Object.keys(categoryTotals).length <= 1 ? 'Sales are concentrated in too few categories.' : 'Category diversification can still be improved further.',
  ];
  const opportunities = [
    `At the current trajectory, the next-period revenue forecast is ${formatInrCompact(forecastRevenue)}.`,
    topRegion ? `Replicate the ${topRegion[0]} playbook across weaker locations.` : 'Use imports to unlock stronger regional forecasting.',
  ];
  const threats = [
    growthRate < 0 ? `Revenue has declined by ${Math.abs(growthRate).toFixed(1)}% versus the previous period.` : `Growth volatility should be watched as revenue moves by ${growthRate.toFixed(1)}%.`,
    'Inventory and tax settings should be reviewed before high-volume expansion.',
  ];

  return {
    project,
    rows,
    totals,
    growthRate,
    forecastRevenue,
    margin,
    topCategory,
    topRegion,
    strengths,
    weaknesses,
    opportunities,
    threats,
    highlights: [
      `Past: ${rows.length} reporting periods have been consolidated into this store report.`,
      `Present: current cumulative revenue is ${formatInrCompact(totals.revenue)} with ${formatInrCompact(totals.profit)} net contribution.`,
      `Future: forecast revenue for the upcoming cycle is ${formatInrCompact(forecastRevenue)} based on the existing trend.`,
    ],
    trendSeries,
    categorySeries,
  };
}

function downloadProjectReportPdf(report) {
  const doc = new jsPDF();
  const pageWidth = doc.internal.pageSize.getWidth();
  const pageHeight = doc.internal.pageSize.getHeight();
  const marginX = 16;
  const contentWidth = pageWidth - (marginX * 2);
  let cursorY = 16;

  const ensureSpace = (requiredHeight) => {
    if (cursorY + requiredHeight > pageHeight - 14) {
      doc.addPage();
      cursorY = 16;
    }
  };

  const writeLine = (text, size = 11, gap = 6) => {
    doc.setFontSize(size);
    doc.setTextColor(15, 23, 42);
    const lines = doc.splitTextToSize(String(text || ''), contentWidth);
    ensureSpace((lines.length * gap) + 2);
    doc.text(lines, marginX, cursorY);
    cursorY += lines.length * gap;
  };

  const writeSectionTitle = (title) => {
    ensureSpace(10);
    doc.setFontSize(13);
    doc.setTextColor(30, 41, 59);
    doc.text(title, marginX, cursorY);
    cursorY += 7;
  };

  const drawTrendChart = (series) => {
    const chartHeight = 72;
    ensureSpace(chartHeight + 8);

    const x = marginX;
    const y = cursorY;
    const w = contentWidth;
    const h = chartHeight;

    doc.setDrawColor(226, 232, 240);
    doc.setFillColor(248, 250, 252);
    doc.roundedRect(x, y, w, h, 4, 4, 'FD');

    doc.setFontSize(11);
    doc.setTextColor(30, 41, 59);
    doc.text('Revenue & Profit Trend', x + 4, y + 7);

    const points = Array.isArray(series) ? series.slice(-8) : [];
    if (points.length === 0) {
      doc.setFontSize(10);
      doc.setTextColor(100, 116, 139);
      doc.text('No trend data available.', x + 4, y + 18);
      cursorY += chartHeight + 6;
      return;
    }

    const plotX = x + 10;
    const plotY = y + 12;
    const plotW = w - 18;
    const plotH = h - 24;
    const maxRevenue = Math.max(...points.map((point) => Number(point.revenue || 0)), 1);
    const maxProfit = Math.max(...points.map((point) => Number(point.profit || 0)), 1);
    const slotW = plotW / points.length;
    const barW = Math.max(4, Math.min(12, slotW * 0.5));

    doc.setDrawColor(203, 213, 225);
    doc.line(plotX, plotY + plotH, plotX + plotW, plotY + plotH);

    const linePoints = [];
    points.forEach((point, index) => {
      const revenue = Math.max(0, Number(point.revenue || 0));
      const profit = Math.max(0, Number(point.profit || 0));
      const centerX = plotX + (index * slotW) + (slotW / 2);
      const barHeight = (revenue / maxRevenue) * (plotH - 6);
      const barX = centerX - (barW / 2);
      const barY = plotY + plotH - barHeight;

      doc.setFillColor(56, 189, 248);
      doc.roundedRect(barX, barY, barW, barHeight, 1.5, 1.5, 'F');

      const lineY = plotY + plotH - ((profit / maxProfit) * (plotH - 6));
      linePoints.push({ x: centerX, y: lineY });

      doc.setFontSize(8);
      doc.setTextColor(71, 85, 105);
      doc.text(String(point.name || ''), centerX, plotY + plotH + 5, { align: 'center' });
    });

    doc.setDrawColor(245, 158, 11);
    doc.setLineWidth(0.8);
    linePoints.forEach((point, index) => {
      if (index > 0) {
        const previous = linePoints[index - 1];
        doc.line(previous.x, previous.y, point.x, point.y);
      }
      doc.setFillColor(245, 158, 11);
      doc.circle(point.x, point.y, 0.9, 'F');
    });

    doc.setFontSize(8);
    doc.setTextColor(56, 189, 248);
    doc.text('Revenue', x + w - 36, y + 7);
    doc.setTextColor(245, 158, 11);
    doc.text('Profit', x + w - 16, y + 7);

    cursorY += chartHeight + 6;
  };

  const drawCategoryChart = (series) => {
    const topCategories = Array.isArray(series) ? series.slice(0, 5) : [];
    const chartHeight = 62;
    ensureSpace(chartHeight + 8);

    const x = marginX;
    const y = cursorY;
    const w = contentWidth;
    const h = chartHeight;

    doc.setDrawColor(226, 232, 240);
    doc.setFillColor(248, 250, 252);
    doc.roundedRect(x, y, w, h, 4, 4, 'FD');

    doc.setFontSize(11);
    doc.setTextColor(30, 41, 59);
    doc.text('Top Category Share', x + 4, y + 7);

    if (topCategories.length === 0) {
      doc.setFontSize(10);
      doc.setTextColor(100, 116, 139);
      doc.text('No category data available.', x + 4, y + 18);
      cursorY += chartHeight + 6;
      return;
    }

    const maxValue = Math.max(...topCategories.map((row) => Number(row.value || 0)), 1);
    topCategories.forEach((row, index) => {
      const lineY = y + 14 + (index * 9);
      const value = Number(row.value || 0);
      const barMaxW = 76;
      const barW = (value / maxValue) * barMaxW;
      const barX = x + 74;

      doc.setFontSize(9);
      doc.setTextColor(51, 65, 85);
      doc.text(String(row.name || '-'), x + 4, lineY);

      doc.setFillColor(226, 232, 240);
      doc.roundedRect(barX, lineY - 3, barMaxW, 4.5, 1, 1, 'F');
      doc.setFillColor(56, 189, 248);
      doc.roundedRect(barX, lineY - 3, Math.max(1, barW), 4.5, 1, 1, 'F');

      doc.setTextColor(30, 41, 59);
      doc.text(formatInrCompact(value), x + w - 4, lineY, { align: 'right' });
    });

    cursorY += chartHeight + 6;
  };

  doc.setFontSize(18);
  doc.setTextColor(15, 23, 42);
  doc.text(`${report.project.name} Analytics Report`, marginX, cursorY);
  cursorY += 8;

  doc.setFontSize(10);
  doc.setTextColor(100, 116, 139);
  doc.text(`Generated from Advanced Analytics for ${report.project.name}`, marginX, cursorY);
  cursorY += 8;

  writeSectionTitle('Past / Present / Future');
  report.highlights.slice(0, 1).forEach((line) => writeLine(line, 11, 6));
  writeLine(report.highlights[1], 11, 6);
  writeLine(report.highlights[2], 11, 6);

  writeSectionTitle('Core Metrics');
  writeLine(`Revenue: ${formatInrCompact(report.totals.revenue)} | Profit: ${formatInrCompact(report.totals.profit)} | Margin: ${report.margin.toFixed(1)}% | Units: ${report.totals.quantity}`, 10, 6);
  writeLine(`Top Category: ${report.topCategory ? `${report.topCategory[0]} (${formatInrCompact(report.topCategory[1])})` : 'N/A'}`, 10, 6);
  writeLine(`Top Region: ${report.topRegion ? `${report.topRegion[0]} (${formatInrCompact(report.topRegion[1])})` : 'N/A'}`, 10, 6);

  writeSectionTitle('Graph Snapshot');
  drawTrendChart(report.trendSeries);
  drawCategoryChart(report.categorySeries);

  writeSectionTitle('SWOT Analysis');
  writeLine(`Strengths: ${report.strengths.join(' ')}`, 10, 6);
  writeLine(`Weaknesses: ${report.weaknesses.join(' ')}`, 10, 6);
  writeLine(`Opportunities: ${report.opportunities.join(' ')}`, 10, 6);
  writeLine(`Threats: ${report.threats.join(' ')}`, 10, 6);

  doc.save(`${report.project.name.replace(/[^a-z0-9]+/gi, '_').toLowerCase()}_report.pdf`);
}

function AdvancedAnalyticsBoard() {
  const { projectId } = useParams();
  const location = useLocation();
  const token = localStorage.getItem('token');
  const [entries, setEntries] = useState([]);
  const [projectRows, setProjectRows] = useState([]);
  const [selectedProjectName, setSelectedProjectName] = useState(location.state?.projectName || 'Advanced Analytics');
  const [isProjectLoading, setIsProjectLoading] = useState(false);
  const [selectedXAxis, setSelectedXAxis] = useState('month');
  const [selectedYAxes, setSelectedYAxes] = useState(['revenue']);
  const [selectedCategoryFilter, setSelectedCategoryFilter] = useState('all');
  const [selectedYearFilter, setSelectedYearFilter] = useState('all');
  const [selectedTimeWindow, setSelectedTimeWindow] = useState('all');
  const [projectionWindow, setProjectionWindow] = useState('3');
  const [compareProducts, setCompareProducts] = useState([]);
  const [compareMonths, setCompareMonths] = useState([]);
  const [compareRegions, setCompareRegions] = useState([]);
  const [formState, setFormState] = useState({
    label: '',
    category: '',
    region: '',
    month: '',
    year: '',
    revenue: '',
    cost: '',
    quantity: '',
  });

  useEffect(() => {
    if (!projectId) {
      setSelectedProjectName(location.state?.projectName || 'Advanced Analytics');
      setEntries([]);
      setProjectRows([]);
      return;
    }

    const fetchProjectAnalytics = async () => {
      try {
        setIsProjectLoading(true);
        const response = await apiRequest({
          method: 'get',
          url: `/api/dashboard/${projectId}`,
          headers: { Authorization: `Bearer ${token}` },
        });

        const rows = Array.isArray(response.data) ? response.data : [];
        setProjectRows(rows);
        setEntries(buildEntriesFromProjectData(rows));
        setSelectedProjectName(location.state?.projectName || `Project ${projectId}`);
      } catch (error) {
        console.error('Error fetching advanced analytics project data:', error);
        setProjectRows([]);
        setEntries([]);
        setSelectedProjectName(location.state?.projectName || `Project ${projectId}`);
      } finally {
        setIsProjectLoading(false);
      }
    };

    fetchProjectAnalytics();
  }, [location.state, projectId, token]);

  const availableCategoryOptions = [...new Set(entries.map((entry) => entry.category).filter(Boolean))].sort((left, right) => left.localeCompare(right));
  const availableRegionOptions = [...new Set(entries.map((entry) => entry.region).filter(Boolean))].sort((left, right) => left.localeCompare(right));
  const availableProductOptions = [...new Set(entries.map((entry) => entry.label).filter(Boolean))].sort((left, right) => left.localeCompare(right));
  const availableYearOptions = [...new Set(entries.map((entry) => String(entry.year)).filter(Boolean))]
    .sort((left, right) => Number(left) - Number(right));
  const availableMonthOptions = [...new Set(entries.map((entry) => entry.month).filter(Boolean))]
    .sort((left, right) => {
      const leftIndex = MONTH_ORDER.indexOf(left);
      const rightIndex = MONTH_ORDER.indexOf(right);
      if (leftIndex >= 0 && rightIndex >= 0) return leftIndex - rightIndex;
      return left.localeCompare(right);
    });

  const getEntryPeriodKey = (entry) => {
    const yearValue = Number(entry.year || new Date(entry.recordedAt).getFullYear());
    const monthIndex = MONTH_ORDER.indexOf(entry.month);
    const safeMonthIndex = monthIndex >= 0 ? monthIndex : 0;
    return yearValue * 12 + safeMonthIndex;
  };

  const filteredEntries = entries.filter((entry) => {
    const matchesCategory = selectedCategoryFilter === 'all' || entry.category === selectedCategoryFilter;
    const matchesYear = selectedYearFilter === 'all' || String(entry.year) === selectedYearFilter;
    const matchesCompareProduct = compareProducts.length === 0 || compareProducts.includes(entry.label);
    const matchesCompareMonth = compareMonths.length === 0 || compareMonths.includes(entry.month);
    const matchesCompareRegion = compareRegions.length === 0 || compareRegions.includes(entry.region);
    return matchesCategory && matchesYear && matchesCompareProduct && matchesCompareMonth && matchesCompareRegion;
  });

  const windowFilteredEntries = (() => {
    if (selectedTimeWindow === 'all' || filteredEntries.length === 0) {
      return filteredEntries;
    }

    if (selectedTimeWindow === 'year') {
      const latestYear = Math.max(...filteredEntries.map((entry) => Number(entry.year || 0)));
      return filteredEntries.filter((entry) => Number(entry.year || 0) === latestYear);
    }

    const monthsToKeep = Number(selectedTimeWindow);
    if (!Number.isFinite(monthsToKeep) || monthsToKeep <= 0) {
      return filteredEntries;
    }

    const uniquePeriods = [...new Set(filteredEntries.map((entry) => getEntryPeriodKey(entry)))].sort((left, right) => left - right);
    const recentPeriods = new Set(uniquePeriods.slice(-monthsToKeep));
    return filteredEntries.filter((entry) => recentPeriods.has(getEntryPeriodKey(entry)));
  })();

  const sortedEntries = [...windowFilteredEntries].sort((leftEntry, rightEntry) => new Date(leftEntry.recordedAt) - new Date(rightEntry.recordedAt));
  const selectedYAxis = selectedYAxes[0] || 'revenue';
  const totalRevenue = sortedEntries.reduce((sum, entry) => sum + entry.revenue, 0);
  const totalCost = sortedEntries.reduce((sum, entry) => sum + entry.cost, 0);
  const totalProfit = totalRevenue - totalCost;
  const avgRevenue = sortedEntries.length ? Math.round(totalRevenue / sortedEntries.length) : 0;
  const activeCategories = new Set(sortedEntries.map((entry) => entry.category).filter(Boolean)).size;
  const activeRegions = new Set(sortedEntries.map((entry) => entry.region).filter(Boolean)).size;
  const selectedYAxisLabel = ANALYTICS_Y_AXIS_OPTIONS.find((option) => option.value === selectedYAxis)?.label || 'Revenue';
  const selectedXAxisLabel = ANALYTICS_X_AXIS_OPTIONS.find((option) => option.value === selectedXAxis)?.label || 'Month';
  const aggregatedAxisData = aggregateEntriesForAxis(sortedEntries, selectedXAxis, selectedYAxis);
  const projectionMetricKeys = selectedYAxes.length > 0 ? selectedYAxes : ['revenue'];
  const projectionMetricLabels = projectionMetricKeys
    .map((metricKey) => ANALYTICS_Y_AXIS_OPTIONS.find((option) => option.value === metricKey)?.label || metricKey);
  const productAxisData = aggregateEntriesForAxis(sortedEntries, 'label', selectedYAxis);
  const categoryAxisData = aggregateEntriesForAxis(sortedEntries, 'category', selectedYAxis);
  const pieData = categoryAxisData.map((entry) => ({ name: entry.name, value: entry.value, color: entry.fill }));
  const histogramRanges = buildHistogramData(sortedEntries, selectedYAxis);
  const selectedMetricTotal = sortedEntries.reduce((sum, entry) => sum + getAnalyticsMetric(entry, selectedYAxis), 0);
  const projectionTrendRows = (() => {
    const grouped = sortedEntries.reduce((accumulator, entry) => {
      const monthName = entry.month || 'January';
      const yearValue = Number(entry.year || new Date(entry.recordedAt).getFullYear());
      const monthIndex = MONTH_ORDER.indexOf(monthName);
      const safeMonthIndex = monthIndex >= 0 ? monthIndex : 0;
      const key = `${yearValue}-${safeMonthIndex}`;

      if (!accumulator[key]) {
        accumulator[key] = {
          year: yearValue,
          monthIndex: safeMonthIndex,
          monthName: MONTH_ORDER[safeMonthIndex],
        };
        projectionMetricKeys.forEach((metricKey) => {
          accumulator[key][metricKey] = 0;
        });
      }

      projectionMetricKeys.forEach((metricKey) => {
        accumulator[key][metricKey] += getAnalyticsMetric(entry, metricKey);
      });
      return accumulator;
    }, {});

    const actualBuckets = Object.values(grouped).sort((leftEntry, rightEntry) => {
      if (leftEntry.year !== rightEntry.year) return leftEntry.year - rightEntry.year;
      return leftEntry.monthIndex - rightEntry.monthIndex;
    });

    const actualRows = actualBuckets.map((entry) => {
      const row = {
        name: `${entry.monthName.slice(0, 3)} ${String(entry.year).slice(-2)}`,
      };
      projectionMetricKeys.forEach((metricKey) => {
        row[`${metricKey}_actual`] = Number(entry[metricKey] || 0);
        row[`${metricKey}_projected`] = null;
      });
      return row;
    });

    if (actualRows.length === 0) {
      return [];
    }

    const lastBucket = actualBuckets[actualBuckets.length - 1];
    let monthIndex = lastBucket.monthIndex;
    let yearValue = lastBucket.year;
    const futureRows = [];

    const projectedMonths = Number(projectionWindow) || 3;
    for (let index = 1; index <= projectedMonths; index += 1) {
      monthIndex += 1;
      if (monthIndex > 11) {
        monthIndex = 0;
        yearValue += 1;
      }

      futureRows.push({
        name: `${MONTH_ORDER[monthIndex].slice(0, 3)} ${String(yearValue).slice(-2)}`,
      });
    }

    projectionMetricKeys.forEach((metricKey) => {
      const lastActual = Number(actualRows[actualRows.length - 1][`${metricKey}_actual`] || 0);
      actualRows[actualRows.length - 1][`${metricKey}_projected`] = lastActual;
      const previousActual = actualRows.length > 1
        ? Number(actualRows[actualRows.length - 2][`${metricKey}_actual`] || 0)
        : 0;
      const growthRate = previousActual > 0 ? (lastActual - previousActual) / previousActual : 0.08;
      let forecastBase = lastActual;

      futureRows.forEach((row) => {
        forecastBase = Math.max(0, Math.round(forecastBase * (1 + growthRate)));
        row[`${metricKey}_actual`] = null;
        row[`${metricKey}_projected`] = forecastBase;
      });
    });

    return [...actualRows, ...futureRows];
  })();
  const realtimeFeed = [...sortedEntries]
    .sort((leftEntry, rightEntry) => new Date(rightEntry.recordedAt) - new Date(leftEntry.recordedAt))
    .slice(0, 5);

  const stateMapData = sortedEntries.reduce((accumulator, entry) => {
    const stateName = normalizeMapStateName(entry.region || 'Unknown');
    accumulator[stateName] = (accumulator[stateName] || 0) + Number(entry.revenue || 0);
    return accumulator;
  }, {});

  const fallbackMonthlyTrendRows = Object.values(sortedEntries.reduce((accumulator, entry) => {
    const monthName = entry.month || 'January';
    const yearValue = Number(entry.year || new Date(entry.recordedAt).getFullYear());
    const monthIndex = MONTH_ORDER.indexOf(monthName);
    const safeMonthIndex = monthIndex >= 0 ? monthIndex : 0;
    const key = `${yearValue}-${safeMonthIndex}`;
    if (!accumulator[key]) {
      accumulator[key] = {
        year: yearValue,
        monthIndex: safeMonthIndex,
        month_name: MONTH_ORDER[safeMonthIndex],
        total_revenue: 0,
        total_cost: 0,
      };
    }
    accumulator[key].total_revenue += Number(entry.revenue || 0);
    accumulator[key].total_cost += Number(entry.cost || 0);
    return accumulator;
  }, {}))
    .sort((leftEntry, rightEntry) => {
      if (leftEntry.year !== rightEntry.year) return leftEntry.year - rightEntry.year;
      return leftEntry.monthIndex - rightEntry.monthIndex;
    });

  const monthlyTrendRows = (projectRows.length > 0 ? projectRows : fallbackMonthlyTrendRows)
    .map((row, index) => ({
      name: row.month_name || row.name || `Point ${index + 1}`,
      revenue: Number(row.total_revenue || row.value || 0),
      profit: Number(row.net_revenue ?? (Number(row.total_revenue || row.value || 0) - Number(row.total_cost || 0))),
      metricLabel: 'Revenue',
    }));

  const previousPeriodRevenue = monthlyTrendRows.length > 1 ? monthlyTrendRows[monthlyTrendRows.length - 2].revenue : 0;
  const latestPeriodRevenue = monthlyTrendRows.length > 0 ? monthlyTrendRows[monthlyTrendRows.length - 1].revenue : totalRevenue;
  const salesGrowth = previousPeriodRevenue > 0 ? ((latestPeriodRevenue - previousPeriodRevenue) / previousPeriodRevenue) * 100 : 0;

  const categoryOverviewRows = categoryAxisData
    .map((entry, index) => ({
      name: entry.name,
      value: entry.value,
      color: COLORS[index % COLORS.length],
      share: selectedMetricTotal > 0 ? (Number(entry.value || 0) / selectedMetricTotal) * 100 : 0,
    }))
    .sort((leftEntry, rightEntry) => rightEntry.value - leftEntry.value);

  const topProductsChartData = [...productAxisData]
    .filter((entry) => String(entry.name || '').trim().length > 0)
    .sort((leftEntry, rightEntry) => rightEntry.value - leftEntry.value)
    .map((entry) => ({ ...entry, fill: '#38bdf8' }));

  const topRegionRows = Object.entries(stateMapData)
    .map(([name, value]) => ({ name, value: Number(value || 0) }))
    .sort((leftEntry, rightEntry) => rightEntry.value - leftEntry.value)
    .slice(0, 4);

  const monthBasedRows = (monthlyTrendRows.length > 0
    ? monthlyTrendRows
      .map((entry) => ({
        month: entry.name,
        sales: Number(entry.revenue || 0),
        profit: Number(entry.profit || 0),
        margin: Number(entry.revenue || 0) > 0 ? (Number(entry.profit || 0) / Number(entry.revenue || 0)) * 100 : 0,
      }))
      .sort((leftEntry, rightEntry) => rightEntry.sales - leftEntry.sales)
      .slice(0, 5)
    : [
      {
        month: 'Current',
        sales: totalRevenue,
        profit: totalProfit,
        margin: totalRevenue > 0 ? (totalProfit / totalRevenue) * 100 : 0,
      },
    ]);

  const overviewRows = [
    ...monthBasedRows,
    ...Array.from({ length: Math.max(0, 5 - monthBasedRows.length) }, (_, index) => ({
      month: `-`,
      sales: null,
      profit: null,
      margin: null,
      id: `placeholder-${index + 1}`,
    })),
  ];

  const handleFormChange = (field, value) => {
    setFormState((currentState) => ({ ...currentState, [field]: value }));
  };

  const handleAddEntry = (event) => {
    event.preventDefault();

    const revenue = Number(formState.revenue);
    const cost = Number(formState.cost);
    const quantity = Number(formState.quantity);

    const yearValue = Number(formState.year);
    if (!formState.label.trim() || !formState.category.trim() || !formState.region.trim() || !formState.month || !Number.isFinite(yearValue) || revenue <= 0 || cost < 0 || quantity <= 0) {
      alert('Enter product, category, region, month, year, and valid numeric values before adding a dataset row.');
      return;
    }

    setEntries((currentEntries) => [
      ...currentEntries,
      {
        id: Date.now(),
        label: formState.label.trim(),
        category: formState.category.trim(),
        region: formState.region.trim(),
        month: formState.month,
        year: yearValue,
        revenue,
        cost,
        quantity,
        recordedAt: `${yearValue}-${String(Math.max(1, MONTH_ORDER.indexOf(formState.month) + 1)).padStart(2, '0')}-01T09:00:00`,
      },
    ]);

    setFormState({
      label: '',
      category: formState.category,
      region: formState.region,
      month: formState.month,
      year: formState.year,
      revenue: '',
      cost: '',
      quantity: '',
    });
  };

  const handleClearImportedData = async () => {
    const confirmed = window.confirm(
      'Clear all imported analytics data for all your projects? This keeps projects/entities but removes uploaded month data.'
    );

    if (!confirmed) {
      return;
    }

    try {
      const response = await apiRequest({
        method: 'delete',
        url: '/api/dashboard',
        headers: { Authorization: `Bearer ${token}` },
      });
      setEntries([]);
      setProjectRows([]);
      alert(response.data?.message || 'Imported data cleared successfully.');
    } catch (error) {
      alert(`Failed to clear imported data: ${error.response?.data?.error || error.message}`);
    }
  };

  return (
    <div className="mx-auto max-w-[1650px] space-y-8 p-8">
      <div className="rounded-[2.5rem] border border-slate-100 bg-white p-8 shadow-xl dark:border-slate-800 dark:bg-slate-900">
        <div className="flex flex-col gap-6 xl:flex-row xl:items-end xl:justify-between">
          <div>
            <div className="text-[10px] font-black uppercase tracking-[0.3em] text-sky-500">Advanced Analytics</div>
            <h1 className="mt-3 text-4xl font-black tracking-tighter text-slate-900 dark:text-white">Store Intelligence Board</h1>
            <p className="mt-3 max-w-3xl text-sm font-medium text-slate-500 dark:text-slate-300">
              The layout matches your requested analytics board while staying connected to backend project data, axis controls, and the live dataset editor.
            </p>
            <div className="mt-4 inline-flex items-center gap-3 rounded-full border border-sky-100 bg-sky-50 px-4 py-2 text-[10px] font-black uppercase tracking-[0.2em] text-sky-600 dark:border-sky-900/40 dark:bg-sky-950/30 dark:text-sky-300">
              <span className="material-symbols-outlined text-sm">storefront</span>
              <span>{selectedProjectName}</span>
              {projectId && <span className="text-slate-400">ID {projectId}</span>}
            </div>
          </div>
          <div className="grid grid-cols-2 gap-4 2xl:grid-cols-3">
            <AnalyticsStat title="Total Sales" value={formatInrCompact(totalRevenue)} sub={`${activeRegions} live regions`} />
            <AnalyticsStat title="Profit" value={formatInrCompact(totalProfit)} sub={`${totalRevenue > 0 ? Math.round((totalProfit / totalRevenue) * 100) : 0}% margin`} />
            <AnalyticsStat title="Sales Growth" value={`${salesGrowth >= 0 ? '+' : ''}${salesGrowth.toFixed(1)}%`} sub={monthlyTrendRows.length > 1 ? 'vs previous period' : 'baseline ready'} />
            <AnalyticsStat title="Rows" value={sortedEntries.length} sub="retrieved rows" />
            <AnalyticsStat title="Categories" value={activeCategories} sub="dynamic category mix" />
            <AnalyticsStat title="Avg Ticket" value={formatInrCompact(avgRevenue)} sub="average revenue per row/transaction entry" />
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-8 xl:grid-cols-[0.95fr_1.45fr]">
        <AnalyticsPanel title="Sales By Region" subtitle="Dynamic India heat map from uploaded state data">
          <div className="h-[420px]">
            <IndiaHeatMap stateData={stateMapData} hasData={Object.keys(stateMapData).length > 0} mapScale={0.93} />
          </div>
        </AnalyticsPanel>

        <div className="self-stretch">
        <AnalyticsPanel title="Revenue & Profit Trend" subtitle="One bar graph plus one line graph, driven by backend totals" className="h-full">
          <div className="h-[420px]">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={monthlyTrendRows}>
                <CartesianGrid vertical={false} strokeDasharray="3 3" opacity={0.15} />
                <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{ fontSize: 11, fontWeight: 700 }} />
                <YAxis yAxisId="left" axisLine={false} tickLine={false} tick={{ fontSize: 11 }} />
                <YAxis yAxisId="right" orientation="right" axisLine={false} tickLine={false} tick={{ fontSize: 11 }} />
                <Tooltip
                  formatter={(value, name, item) => {
                    const metricName = item?.dataKey === 'profit' ? 'Profit' : 'Revenue';
                    return [formatInrCompact(value), metricName];
                  }}
                />
                <Legend />
                <Bar yAxisId="left" dataKey="revenue" name="Revenue" fill="#38bdf8" radius={[10, 10, 0, 0]} />
                <Line yAxisId="right" type="monotone" dataKey="profit" name="Profit" stroke="#f59e0b" strokeWidth={3} dot={{ r: 4 }} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </AnalyticsPanel>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-8 xl:grid-cols-2 2xl:grid-cols-[1fr_1fr_0.95fr] items-start">
        <div className="space-y-8">
          <AnalyticsPanel title="Bar Chart" subtitle={`${selectedYAxisLabel} by Product`} className="min-h-[28rem]">
            <div className="max-h-[24rem] overflow-y-auto pr-1">
              <div style={{ height: `${Math.max(320, topProductsChartData.length * 34)}px`, minHeight: '320px' }}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={topProductsChartData} layout="vertical" margin={{ left: 10, right: 10 }}>
                  <CartesianGrid horizontal={false} strokeDasharray="3 3" opacity={0.12} />
                  <XAxis type="number" axisLine={false} tickLine={false} tick={{ fontSize: 11 }} />
                  <YAxis type="category" dataKey="name" interval={0} width={130} axisLine={false} tickLine={false} tick={{ fontSize: 11, fontWeight: 700 }} tickFormatter={(label) => String(label || 'Unknown')} />
                  <Tooltip
                    formatter={(value) => [formatMetricValue(selectedYAxis, value), selectedYAxisLabel]}
                    labelFormatter={(label) => `Product: ${label}`}
                  />
                  <Bar dataKey="value" name={selectedYAxisLabel} radius={[0, 10, 10, 0]}>
                    {topProductsChartData.map((entry) => <Cell key={entry.name} fill={entry.fill} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
              </div>
            </div>
          </AnalyticsPanel>

          <AnalyticsPanel title="Top Regions" subtitle={`Highest ${selectedYAxisLabel.toLowerCase()} states from active rows`} className="min-h-[24rem]">
            <div className="grid grid-cols-1 gap-3">
              {(topRegionRows.length > 0 ? topRegionRows : [{ name: 'Awaiting data', value: 0 }]).map((entry, index) => (
                <div key={entry.name} className="rounded-2xl border border-slate-100 bg-slate-50 px-4 py-4 dark:border-slate-800 dark:bg-slate-800/60">
                  <div className="flex items-center justify-between gap-4">
                    <div className="flex items-center gap-3">
                      <div className={`flex h-10 w-10 items-center justify-center rounded-2xl text-sm font-black text-white ${index === 0 ? 'bg-emerald-500' : 'bg-sky-500'}`}>
                        {index + 1}
                      </div>
                      <div>
                        <div className="text-sm font-black text-slate-900 dark:text-white">{entry.name}</div>
                        <div className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">State performance</div>
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="text-sm font-black text-slate-900 dark:text-white">{formatInrCompact(entry.value)}</div>
                      <div className="text-[10px] font-black uppercase tracking-[0.2em] text-emerald-500">
                        {selectedMetricTotal > 0 ? `${((entry.value / selectedMetricTotal) * 100).toFixed(1)}% of ${selectedYAxisLabel.toLowerCase()}` : 'No share yet'}
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </AnalyticsPanel>
        </div>

        <div>
        <AnalyticsPanel title="Sales By Category" subtitle={`${selectedYAxisLabel} share by Category`} className="min-h-[28rem]">
          <div className="h-60">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={pieData} dataKey="value" nameKey="name" innerRadius={54} outerRadius={90} paddingAngle={3}>
                  {pieData.map((entry) => <Cell key={entry.name} fill={entry.color} />)}
                </Pie>
                <Tooltip
                  formatter={(value) => [formatMetricValue(selectedYAxis, value), selectedYAxisLabel]}
                  labelFormatter={(label) => `Category: ${label}`}
                />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-4 space-y-2">
            {(categoryOverviewRows.length > 0 ? categoryOverviewRows : pieData.map((entry) => ({ ...entry, share: selectedMetricTotal > 0 ? (entry.value / selectedMetricTotal) * 100 : 0 })))
              .slice(0, 4)
              .map((entry) => (
                <div key={entry.name} className="flex items-center justify-between rounded-2xl border border-slate-100 bg-slate-50 px-4 py-3 text-sm font-medium text-slate-600 dark:border-slate-800 dark:bg-slate-800/60 dark:text-slate-200">
                  <div className="flex items-center gap-3">
                    <span className="h-3 w-3 rounded-full" style={{ backgroundColor: entry.color }} />
                    <span className="font-bold text-slate-900 dark:text-white">{entry.name}</span>
                  </div>
                  <div className="text-right">
                    <div className="font-black text-slate-900 dark:text-white">{formatMetricValue(selectedYAxis, entry.value)}</div>
                    <div className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{Number(entry.share || 0).toFixed(1)}% share</div>
                  </div>
                </div>
              ))}
          </div>
        </AnalyticsPanel>
        </div>

        <div className="space-y-8">
          <AnalyticsPanel title="Histogram" subtitle={`${selectedYAxisLabel} distribution buckets`} className="min-h-[18rem]">
            <div className="h-44">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={histogramRanges}>
                  <CartesianGrid vertical={false} strokeDasharray="3 3" opacity={0.15} />
                  <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{ fontSize: 10, fontWeight: 700 }} />
                  <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 11 }} allowDecimals={false} />
                  <Tooltip />
                  <Bar dataKey="count" fill="#f97316" radius={[10, 10, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </AnalyticsPanel>

          <AnalyticsPanel title="Overview" subtitle="Beneath summary strip" className="min-h-[34rem]">
            <div className="overflow-hidden rounded-[1.75rem] border border-slate-200 dark:border-slate-800">
              <div className="grid grid-cols-[1.4fr_1fr_1fr_0.8fr] gap-x-6 bg-slate-50 px-6 py-3 text-[10px] font-black uppercase tracking-[0.2em] text-slate-500 dark:bg-slate-800/70 dark:text-slate-300">
                <span className="block pl-2">Month</span>
                <span className="text-right">Sales</span>
                <span className="text-right">Profit</span>
                <span className="text-right">Margin</span>
              </div>
              <div className="bg-white dark:bg-slate-900">
                {overviewRows.map((entry) => (
                  <div key={entry.id || entry.month} className="grid grid-cols-[1.4fr_1fr_1fr_0.8fr] gap-x-6 border-t border-slate-100 px-6 py-4 text-sm font-medium text-slate-600 dark:border-slate-800 dark:text-slate-200">
                    <span className="block whitespace-nowrap pl-2 font-bold text-slate-900 dark:text-white">{entry.month}</span>
                    <span className="whitespace-nowrap text-right">{entry.sales == null ? '-' : formatInrCompact(entry.sales)}</span>
                    <span className="whitespace-nowrap text-right">{entry.profit == null ? '-' : formatInrCompact(entry.profit)}</span>
                    <span className="whitespace-nowrap text-right font-black text-emerald-500">{entry.margin == null ? '-' : `${Number(entry.margin || 0).toFixed(1)}%`}</span>
                  </div>
                ))}
              </div>
            </div>
            <div className="mt-5 grid grid-cols-1 gap-4">
              <RealtimeMetric title="Total Rows" value={sortedEntries.length} detail="retrieved from backend" tone="rose" />
              <RealtimeMetric title="Calculated Total Sum" value={formatMetricValue(selectedYAxis, selectedMetricTotal)} detail={`${selectedYAxisLabel.toLowerCase()} across all rows`} tone="emerald" />
              <RealtimeMetric title="Autosync" value="Active" detail={projectId ? 'live backend project feed' : 'local analytics workspace'} tone="amber" />
            </div>
          </AnalyticsPanel>
        </div>
      </div>

      <AnalyticsPanel title="Real-Time Visualization" subtitle={`${projectionMetricLabels.join(' + ')} actual vs next ${projectionWindow}-month projection`}>
        <div className="h-56">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={projectionTrendRows}>
              <CartesianGrid vertical={false} strokeDasharray="3 3" opacity={0.15} />
              <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{ fontSize: 11, fontWeight: 700 }} />
              <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 11 }} />
              <Tooltip
                formatter={(value, seriesName, item) => {
                  if (value == null) {
                    return ['-', seriesName];
                  }
                  const metricKey = String(item?.dataKey || '').split('_')[0];
                  return [formatMetricValue(metricKey, value), seriesName];
                }}
              />
              <Legend />
              {projectionMetricKeys.map((metricKey) => {
                const metricLabel = ANALYTICS_Y_AXIS_OPTIONS.find((option) => option.value === metricKey)?.label || metricKey;
                const strokeColor = '#2563eb';
                return (
                  <Line
                    key={`${metricKey}-actual`}
                    type="monotone"
                    dataKey={`${metricKey}_actual`}
                    name={`${metricLabel} (Actual)`}
                    stroke={strokeColor}
                    strokeWidth={3}
                    dot={{ r: 4 }}
                    activeDot={{ r: 6 }}
                    connectNulls
                  />
                );
              })}
              {projectionMetricKeys.map((metricKey) => {
                const metricLabel = ANALYTICS_Y_AXIS_OPTIONS.find((option) => option.value === metricKey)?.label || metricKey;
                const strokeColor = '#9333ea';
                return (
                  <Line
                    key={`${metricKey}-projected`}
                    type="monotone"
                    dataKey={`${metricKey}_projected`}
                    name={`${metricLabel} (Projected)`}
                    stroke={strokeColor}
                    strokeWidth={3}
                    strokeDasharray="6 4"
                    dot={(props) => (props?.value == null ? null : <circle cx={props.cx} cy={props.cy} r={4} fill={strokeColor} />)}
                    activeDot={{ r: 6 }}
                    connectNulls
                  />
                );
              })}
            </LineChart>
          </ResponsiveContainer>
        </div>
        <div className="mt-5 grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-5">
          {realtimeFeed.map((entry, index) => (
            <div key={entry.id} className="flex items-center justify-between rounded-2xl border border-slate-100 bg-slate-50 px-4 py-3 dark:border-slate-800 dark:bg-slate-800/50">
              <div className="flex items-center gap-3">
                <div className={`flex h-10 w-10 items-center justify-center rounded-2xl text-white ${index === 0 ? 'bg-emerald-500' : 'bg-sky-500'}`}>
                  <span className="material-symbols-outlined text-base">{index === 0 ? 'bolt' : 'monitoring'}</span>
                </div>
                <div>
                  <div className="text-sm font-black text-slate-900 dark:text-white">{entry.label}</div>
                  <div className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{entry.category} | {entry.region}</div>
                </div>
              </div>
              <div className="text-right">
                <div className="text-sm font-black text-slate-900 dark:text-white">{formatInrCompact(entry.revenue)}</div>
                <div className="text-[10px] font-black uppercase tracking-[0.2em] text-emerald-500">{entry.quantity} units</div>
              </div>
            </div>
          ))}
        </div>
      </AnalyticsPanel>

      <AnalyticsPanel title="Data Set Editor" subtitle="Choose axes, add rows, and drive the analytics board live">
        {isProjectLoading && (
          <div className="mb-5 rounded-2xl border border-sky-100 bg-sky-50 px-4 py-3 text-sm font-bold text-sky-700 dark:border-sky-900/40 dark:bg-sky-950/30 dark:text-sky-300">
            Loading selected project analytics...
          </div>
        )}
        <div className="mb-4 flex justify-end">
          <button
            type="button"
            onClick={handleClearImportedData}
            className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-2 text-xs font-black uppercase tracking-[0.2em] text-rose-600 transition-all hover:border-rose-300 hover:bg-rose-100 dark:border-rose-900/40 dark:bg-rose-950/30 dark:text-rose-300"
          >
            Clear Imported Data
          </button>
        </div>
        <div className="mb-6 rounded-[1.75rem] border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-800/60">
          <div className="grid grid-cols-1 gap-4 xl:grid-cols-6 xl:items-end">
          <EditorField label="X Axis">
            <select value={selectedXAxis} onChange={(event) => setSelectedXAxis(event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 dark:border-slate-700 dark:bg-slate-900 dark:text-white">
              {ANALYTICS_X_AXIS_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </EditorField>
          <EditorField label="Y Axis (Multi Select)">
            <MultiSelectDropdown
              options={ANALYTICS_Y_AXIS_OPTIONS.map((option) => ({ value: option.value, label: option.label }))}
              selectedValues={selectedYAxes}
              onChange={(values) => setSelectedYAxes(values.length > 0 ? values : ['revenue'])}
              placeholder="Select one or more metrics"
              compactLabel
            />
          </EditorField>
          <EditorField label="Filter Category">
            <select value={selectedCategoryFilter} onChange={(event) => setSelectedCategoryFilter(event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 dark:border-slate-700 dark:bg-slate-900 dark:text-white">
              <option value="all">All Categories</option>
              {availableCategoryOptions.map((category) => <option key={category} value={category}>{category}</option>)}
            </select>
          </EditorField>
          <EditorField label="Filter Year">
            <select value={selectedYearFilter} onChange={(event) => setSelectedYearFilter(event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 dark:border-slate-700 dark:bg-slate-900 dark:text-white">
              <option value="all">All Years</option>
              {availableYearOptions.map((year) => <option key={year} value={year}>{year}</option>)}
            </select>
          </EditorField>
          <EditorField label="Month Range">
            <select value={selectedTimeWindow} onChange={(event) => setSelectedTimeWindow(event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 dark:border-slate-700 dark:bg-slate-900 dark:text-white">
              {ANALYTICS_TIME_WINDOW_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </EditorField>
          <EditorField label="Projection Horizon">
            <select value={projectionWindow} onChange={(event) => setProjectionWindow(event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 dark:border-slate-700 dark:bg-slate-900 dark:text-white">
              {ANALYTICS_PROJECTION_WINDOW_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </EditorField>
          </div>
          <div className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-[1fr_1fr_1fr_auto] xl:items-end">
            <MultiSelectDropdown
              label="Products (Multi Select)"
              options={availableProductOptions.map((product) => ({ value: product, label: product }))}
              selectedValues={compareProducts}
              onChange={setCompareProducts}
              placeholder="All products"
            />
            <MultiSelectDropdown
              label="Filter Months (Multi Select)"
              options={availableMonthOptions.map((month) => ({ value: month, label: month }))}
              selectedValues={compareMonths}
              onChange={setCompareMonths}
              placeholder="All months"
            />
            <MultiSelectDropdown
              label="Regions (Multi Select)"
              options={availableRegionOptions.map((region) => ({ value: region, label: region }))}
              selectedValues={compareRegions}
              onChange={setCompareRegions}
              placeholder="All regions"
            />
            <div className="space-y-2">
              <div className="h-4 xl:h-5" />
              <button
                type="button"
                onClick={() => {
                  setSelectedCategoryFilter('all');
                  setSelectedYearFilter('all');
                  setSelectedTimeWindow('all');
                  setCompareProducts([]);
                  setCompareMonths([]);
                  setCompareRegions([]);
                }}
                className="h-12 rounded-2xl border border-slate-200 bg-white px-6 text-xs font-black uppercase tracking-[0.2em] text-slate-500 transition-all hover:border-sky-300 hover:text-sky-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300"
              >
                Clear
              </button>
            </div>
          </div>
        </div>
        <form onSubmit={handleAddEntry} className="grid grid-cols-1 gap-4 xl:grid-cols-8">
          <EditorField label="Product">
            <input value={formState.label} onChange={(event) => handleFormChange('label', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Product name" />
          </EditorField>
          <EditorField label="Category">
            <input value={formState.category} onChange={(event) => handleFormChange('category', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Category (e.g., Electronics)" />
          </EditorField>
          <EditorField label="Region">
            <input value={formState.region} onChange={(event) => handleFormChange('region', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Region (e.g., Tamil Nadu)" />
          </EditorField>
          <EditorField label="Month">
            <select value={formState.month} onChange={(event) => handleFormChange('month', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white">
              <option value="">Select month</option>
              {MONTH_ORDER.map((month) => <option key={month} value={month}>{month}</option>)}
            </select>
          </EditorField>
          <EditorField label="Year">
            <input value={formState.year} onChange={(event) => handleFormChange('year', event.target.value)} type="number" min="2000" max="2100" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Year (e.g., 2026)" />
          </EditorField>
          <EditorField label="Revenue">
            <input value={formState.revenue} onChange={(event) => handleFormChange('revenue', event.target.value)} type="number" min="0" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Revenue value" />
          </EditorField>
          <EditorField label="Cost">
            <input value={formState.cost} onChange={(event) => handleFormChange('cost', event.target.value)} type="number" min="0" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Cost value" />
          </EditorField>
          <EditorField label="Quantity">
            <div className="flex gap-3">
              <input value={formState.quantity} onChange={(event) => handleFormChange('quantity', event.target.value)} type="number" min="1" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Quantity" />
              <button type="submit" className="rounded-2xl bg-sky-600 px-5 text-sm font-black text-white shadow-lg shadow-sky-600/20 transition-all hover:-translate-y-0.5 hover:bg-sky-500">Add</button>
            </div>
          </EditorField>
        </form>

        <div className="mt-6 overflow-hidden rounded-[1.75rem] border border-slate-200 dark:border-slate-800">
          <div className="grid grid-cols-[1.4fr_1fr_1fr_0.8fr_0.8fr_0.8fr] bg-slate-50 px-5 py-3 text-[10px] font-black uppercase tracking-[0.2em] text-slate-500 dark:bg-slate-800/70 dark:text-slate-300">
            <span>Product</span>
            <span>Category</span>
            <span>Region</span>
            <span>Revenue</span>
            <span>Cost</span>
            <span>Units</span>
          </div>
          <div className="max-h-72 overflow-y-auto bg-white dark:bg-slate-900">
            {sortedEntries.slice().reverse().map((entry) => (
              <div key={entry.id} className="grid grid-cols-[1.4fr_1fr_1fr_0.8fr_0.8fr_0.8fr] border-t border-slate-100 px-5 py-4 text-sm font-medium text-slate-600 dark:border-slate-800 dark:text-slate-200">
                <span className="font-bold text-slate-900 dark:text-white">{entry.label}</span>
                <span>{entry.category}</span>
                <span>{entry.region}</span>
                <span>{formatInrCompact(entry.revenue)}</span>
                <span>{formatInrCompact(entry.cost)}</span>
                <span>{entry.quantity}</span>
              </div>
            ))}
          </div>
        </div>
      </AnalyticsPanel>
    </div>
  );
}

function AdvancedAnalyticsBase() {
  const { projectId } = useParams();
  const location = useLocation();
  const token = localStorage.getItem('token');
  const [entries, setEntries] = useState([]);
  const [selectedProjectName, setSelectedProjectName] = useState(location.state?.projectName || 'Advanced Analytics');
  const [isProjectLoading, setIsProjectLoading] = useState(false);
  const [selectedXAxis, setSelectedXAxis] = useState('month');
  const [selectedYAxis, setSelectedYAxis] = useState('revenue');
  const [formState, setFormState] = useState({
    label: '',
    category: 'Electronics',
    region: 'Tamil Nadu',
    revenue: '',
    cost: '',
    quantity: '',
  });

  useEffect(() => {
    if (!projectId) {
      setSelectedProjectName(location.state?.projectName || 'Advanced Analytics');
      setEntries([]);
      return;
    }

    const fetchProjectAnalytics = async () => {
      try {
        setIsProjectLoading(true);
        const response = await apiRequest({
          method: 'get',
          url: `/api/dashboard/${projectId}`,
          headers: { 'Authorization': `Bearer ${token}` },
        });

        setEntries(buildEntriesFromProjectData(response.data));
        if (location.state?.projectName) {
          setSelectedProjectName(location.state.projectName);
        } else {
          setSelectedProjectName(`Project ${projectId}`);
        }
      } catch (error) {
        console.error('Error fetching advanced analytics project data:', error);
        setEntries([]);
        setSelectedProjectName(location.state?.projectName || `Project ${projectId}`);
      } finally {
        setIsProjectLoading(false);
      }
    };

    fetchProjectAnalytics();
  }, [location.state, projectId, token]);

  const sortedEntries = [...entries].sort((leftEntry, rightEntry) => new Date(leftEntry.recordedAt) - new Date(rightEntry.recordedAt));
  const totalRevenue = sortedEntries.reduce((sum, entry) => sum + entry.revenue, 0);
  const totalCost = sortedEntries.reduce((sum, entry) => sum + entry.cost, 0);
  const totalUnits = sortedEntries.reduce((sum, entry) => sum + entry.quantity, 0);
  const avgRevenue = sortedEntries.length ? Math.round(totalRevenue / sortedEntries.length) : 0;
  const selectedYAxisLabel = ANALYTICS_Y_AXIS_OPTIONS.find((option) => option.value === selectedYAxis)?.label || 'Revenue';
  const selectedXAxisLabel = ANALYTICS_X_AXIS_OPTIONS.find((option) => option.value === selectedXAxis)?.label || 'Month';
  const aggregatedAxisData = aggregateEntriesForAxis(sortedEntries, selectedXAxis, selectedYAxis);
  const barChartData = aggregatedAxisData.map((entry) => ({ ...entry, metric: entry.value }));
  const lineData = aggregatedAxisData.map((entry) => ({ ...entry, metric: entry.value }));
  const pieData = aggregatedAxisData.map((entry) => ({ name: entry.name, value: entry.value, color: entry.fill }));
  const histogramRanges = buildHistogramData(sortedEntries, selectedYAxis);
  const selectedMetricTotal = sortedEntries.reduce((sum, entry) => sum + getAnalyticsMetric(entry, selectedYAxis), 0);

  const realtimeFeed = [...sortedEntries]
    .sort((leftEntry, rightEntry) => new Date(rightEntry.recordedAt) - new Date(leftEntry.recordedAt))
    .slice(0, 5);

  const handleFormChange = (field, value) => {
    setFormState((currentState) => ({ ...currentState, [field]: value }));
  };

  const handleAddEntry = (event) => {
    event.preventDefault();

    const revenue = Number(formState.revenue);
    const cost = Number(formState.cost);
    const quantity = Number(formState.quantity);

    if (!formState.label.trim() || !formState.category.trim() || !formState.region.trim() || revenue <= 0 || cost < 0 || quantity <= 0) {
      alert('Enter a label, category, region, and valid numeric values before adding a dataset row.');
      return;
    }

    const normalizedYear = Number(formState.year);
    if (!formState.month || !Number.isFinite(normalizedYear) || normalizedYear < 2000 || normalizedYear > 2100) {
      alert('Select a valid month and year before adding a dataset row.');
      return;
    }

    setEntries((currentEntries) => [
      ...currentEntries,
      {
        id: Date.now(),
        label: formState.label.trim(),
        category: formState.category.trim(),
        region: formState.region.trim(),
        month: formState.month,
        year: normalizedYear,
        revenue,
        cost,
        quantity,
        recordedAt: new Date().toISOString(),
      },
    ]);

    setFormState({
      label: '',
      category: '',
      region: '',
      month: '',
      year: '',
      revenue: '',
      cost: '',
      quantity: '',
    });
  };

  return (
    <div className="p-8 space-y-8 max-w-[1600px] mx-auto">
      <div className="rounded-[2.5rem] border border-slate-100 bg-white p-8 shadow-xl dark:border-slate-800 dark:bg-slate-900">
        <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <div className="text-[10px] font-black uppercase tracking-[0.3em] text-sky-500">Advanced Analytics</div>
            <h1 className="mt-3 text-4xl font-black tracking-tighter text-slate-900 dark:text-white">Legacy Intelligence Studio</h1>
            <p className="mt-3 max-w-3xl text-sm font-medium text-slate-500 dark:text-slate-300">
              Interactive bar chart, line graph, pie chart, histogram, real-time visualization, and a dataset editor for adding new rows directly into the live analytics canvas.
            </p>
            <div className="mt-4 inline-flex items-center gap-3 rounded-full border border-sky-100 bg-sky-50 px-4 py-2 text-[10px] font-black uppercase tracking-[0.2em] text-sky-600 dark:border-sky-900/40 dark:bg-sky-950/30 dark:text-sky-300">
              <span className="material-symbols-outlined text-sm">storefront</span>
              <span>{selectedProjectName}</span>
              {projectId && <span className="text-slate-400">ID {projectId}</span>}
            </div>
          </div>
          <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
            <AnalyticsStat title="Rows" value={sortedEntries.length} sub="active entries" />
            <AnalyticsStat title="Revenue" value={formatInrCompact(totalRevenue)} sub="gross sales" />
            <AnalyticsStat title="Margin" value={`${totalRevenue > 0 ? Math.round(((totalRevenue - totalCost) / totalRevenue) * 100) : 0}%`} sub="profit rate" />
            <AnalyticsStat title="Avg Ticket" value={formatInrCompact(avgRevenue)} sub="average revenue per row/transaction entry" />
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-8 2xl:grid-cols-[1.5fr_1fr]">
        <div className="space-y-8">
          <div className="grid grid-cols-1 gap-8 xl:grid-cols-2">
            <AnalyticsPanel title="Bar Chart" subtitle={`${selectedYAxisLabel} by Product`}>
              <div className="h-72">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={barChartData}>
                    <CartesianGrid vertical={false} strokeDasharray="3 3" opacity={0.15} />
                    <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{ fontSize: 11, fontWeight: 700 }} />
                    <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 11 }} />
                    <Tooltip formatter={(value) => formatMetricValue(selectedYAxis, value)} />
                    <Bar dataKey="metric" radius={[10, 10, 0, 0]}>
                      {barChartData.map((entry) => <Cell key={entry.name} fill={entry.fill} />)}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </AnalyticsPanel>

            <AnalyticsPanel title="Line Graph" subtitle={`${selectedYAxisLabel} trend by ${selectedXAxisLabel}`}>
              <div className="h-72">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={lineData}>
                    <CartesianGrid vertical={false} strokeDasharray="3 3" opacity={0.15} />
                    <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{ fontSize: 11, fontWeight: 700 }} />
                    <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 11 }} />
                    <Tooltip formatter={(value) => formatMetricValue(selectedYAxis, value)} />
                    <Line type="monotone" dataKey="metric" stroke="#38bdf8" strokeWidth={3} dot={{ r: 4 }} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </AnalyticsPanel>
          </div>

          <div className="grid grid-cols-1 gap-8 xl:grid-cols-2">
            <AnalyticsPanel title="Pie Chart" subtitle={`${selectedYAxisLabel} share by ${selectedXAxisLabel}`}>
              <div className="h-72">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie data={pieData} dataKey="value" nameKey="name" innerRadius={68} outerRadius={108} paddingAngle={4}>
                      {pieData.map((entry) => <Cell key={entry.name} fill={entry.color} />)}
                    </Pie>
                    <Tooltip formatter={(value) => formatMetricValue(selectedYAxis, value)} />
                    <Legend />
                  </PieChart>
                </ResponsiveContainer>
              </div>
            </AnalyticsPanel>

            <AnalyticsPanel title="Histogram" subtitle={`${selectedYAxisLabel} distribution buckets`}>
              <div className="h-72">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={histogramRanges}>
                    <CartesianGrid vertical={false} strokeDasharray="3 3" opacity={0.15} />
                    <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{ fontSize: 11, fontWeight: 700 }} />
                    <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 11 }} allowDecimals={false} />
                    <Tooltip />
                    <Bar dataKey="count" fill="#14b8a6" radius={[10, 10, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </AnalyticsPanel>
          </div>

          <AnalyticsPanel title="Data Set Editor" subtitle="Add new entries into the live analytics workspace">
            {isProjectLoading && (
              <div className="mb-5 rounded-2xl border border-sky-100 bg-sky-50 px-4 py-3 text-sm font-bold text-sky-700 dark:border-sky-900/40 dark:bg-sky-950/30 dark:text-sky-300">
                Loading selected project analytics...
              </div>
            )}
            <div className="mb-6 grid grid-cols-1 gap-4 rounded-[1.75rem] border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-800/60 xl:grid-cols-2">
              <EditorField label="X Axis">
                <select value={selectedXAxis} onChange={(event) => setSelectedXAxis(event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 dark:border-slate-700 dark:bg-slate-900 dark:text-white">
                  {ANALYTICS_X_AXIS_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
                </select>
              </EditorField>
              <EditorField label="Y Axis">
                <select value={selectedYAxis} onChange={(event) => setSelectedYAxis(event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 dark:border-slate-700 dark:bg-slate-900 dark:text-white">
                  {ANALYTICS_Y_AXIS_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
                </select>
              </EditorField>
            </div>
            <form onSubmit={handleAddEntry} className="grid grid-cols-1 gap-4 xl:grid-cols-6">
              <EditorField label="Label">
                <input value={formState.label} onChange={(event) => handleFormChange('label', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Product name" />
              </EditorField>
              <EditorField label="Category">
                <input value={formState.category} onChange={(event) => handleFormChange('category', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Electronics" />
              </EditorField>
              <EditorField label="Region">
                <input value={formState.region} onChange={(event) => handleFormChange('region', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Tamil Nadu" />
              </EditorField>
              <EditorField label="Revenue">
                <input value={formState.revenue} onChange={(event) => handleFormChange('revenue', event.target.value)} type="number" min="0" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="125000" />
              </EditorField>
              <EditorField label="Cost">
                <input value={formState.cost} onChange={(event) => handleFormChange('cost', event.target.value)} type="number" min="0" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="76000" />
              </EditorField>
              <EditorField label="Quantity">
                <div className="flex gap-3">
                  <input value={formState.quantity} onChange={(event) => handleFormChange('quantity', event.target.value)} type="number" min="1" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="14" />
                  <button type="submit" className="rounded-2xl bg-sky-600 px-5 text-sm font-black text-white shadow-lg shadow-sky-600/20 transition-all hover:-translate-y-0.5 hover:bg-sky-500">Add</button>
                </div>
              </EditorField>
            </form>

            <div className="mt-6 overflow-hidden rounded-[1.75rem] border border-slate-200 dark:border-slate-800">
              <div className="grid grid-cols-[1.4fr_1fr_1fr_0.8fr_0.8fr_0.8fr] bg-slate-50 px-5 py-3 text-[10px] font-black uppercase tracking-[0.2em] text-slate-500 dark:bg-slate-800/70 dark:text-slate-300">
                <span>Product</span>
                <span>Category</span>
                <span>Region</span>
                <span>Revenue</span>
                <span>Cost</span>
                <span>Units</span>
              </div>
              <div className="max-h-72 overflow-y-auto bg-white dark:bg-slate-900">
                {sortedEntries.slice().reverse().map((entry) => (
                  <div key={entry.id} className="grid grid-cols-[1.4fr_1fr_1fr_0.8fr_0.8fr_0.8fr] border-t border-slate-100 px-5 py-4 text-sm font-medium text-slate-600 dark:border-slate-800 dark:text-slate-200">
                    <span className="font-bold text-slate-900 dark:text-white">{entry.label}</span>
                    <span>{entry.category}</span>
                    <span>{entry.region}</span>
                    <span>{formatInrCompact(entry.revenue)}</span>
                    <span>{formatInrCompact(entry.cost)}</span>
                    <span>{entry.quantity}</span>
                  </div>
                ))}
              </div>
            </div>
          </AnalyticsPanel>
        </div>

        <div className="space-y-8">
          <AnalyticsPanel title="Real-Time Visualization" subtitle={`${selectedYAxisLabel} projected by ${selectedXAxisLabel}`}>
            <div className="h-60">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={lineData}>
                  <CartesianGrid vertical={false} strokeDasharray="3 3" opacity={0.15} />
                  <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{ fontSize: 11, fontWeight: 700 }} />
                  <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 11 }} />
                  <Tooltip formatter={(value) => formatMetricValue(selectedYAxis, value)} />
                  <Line type="monotone" dataKey="metric" stroke="#0ea5e9" strokeWidth={3} dot={{ r: 4 }} activeDot={{ r: 6 }} />
                </LineChart>
              </ResponsiveContainer>
            </div>
            <div className="mt-5 space-y-3">
              {realtimeFeed.map((entry, index) => (
                <div key={entry.id} className="flex items-center justify-between rounded-2xl border border-slate-100 bg-slate-50 px-4 py-3 dark:border-slate-800 dark:bg-slate-800/50">
                  <div className="flex items-center gap-3">
                    <div className={`flex h-10 w-10 items-center justify-center rounded-2xl text-white ${index === 0 ? 'bg-emerald-500' : 'bg-sky-500'}`}>
                      <span className="material-symbols-outlined text-base">{index === 0 ? 'bolt' : 'monitoring'}</span>
                    </div>
                    <div>
                      <div className="text-sm font-black text-slate-900 dark:text-white">{entry.label}</div>
                      <div className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{entry.category} | {entry.region}</div>
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="text-sm font-black text-slate-900 dark:text-white">{formatInrCompact(entry.revenue)}</div>
                    <div className="text-[10px] font-black uppercase tracking-[0.2em] text-emerald-500">{entry.quantity} units</div>
                  </div>
                </div>
              ))}
            </div>
          </AnalyticsPanel>

          <AnalyticsPanel title="Beneath Feed" subtitle="Operational summary strip">
            <div className="grid grid-cols-1 gap-4">
              <RealtimeMetric title="Total Revenue" value={formatInrCompact(totalRevenue)} detail="all active rows" tone="sky" />
              <RealtimeMetric title="Total Rows" value={sortedEntries.length} detail="retrieved from backend" tone="rose" />
              <RealtimeMetric title="Calculated Total Sum" value={formatMetricValue(selectedYAxis, selectedMetricTotal)} detail={`${selectedYAxisLabel.toLowerCase()} across all rows`} tone="emerald" />
              <RealtimeMetric title="Autosync" value="Active" detail={projectId ? 'live backend project feed' : 'local analytics workspace'} tone="amber" />
            </div>
          </AnalyticsPanel>
        </div>
      </div>
    </div>
  );
}

function AnalyticsPanel({ title, subtitle, children, className = '' }) {
  return (
    <div className={`rounded-[2.5rem] border border-slate-100 bg-white p-8 shadow-xl dark:border-slate-800 dark:bg-slate-900 ${className}`}>
      <div className="mb-6">
        <div className="text-[10px] font-black uppercase tracking-[0.3em] text-slate-400">{title}</div>
        <h3 className="mt-2 text-2xl font-black tracking-tighter text-slate-900 dark:text-white">{subtitle}</h3>
      </div>
      {children}
    </div>
  );
}

function AnalyticsStat({ title, value, sub }) {
  return (
    <div className="rounded-[1.75rem] border border-slate-100 bg-slate-50 px-5 py-4 dark:border-slate-800 dark:bg-slate-800/60">
      <div className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{title}</div>
      <div className="mt-2 text-2xl font-black tracking-tighter text-slate-900 dark:text-white">{value}</div>
      <div className="mt-1 text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{sub}</div>
    </div>
  );
}

function ProjectFormField({ label, required, children }) {
  return (
    <label className="block">
      <div className="mb-2 text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">
        {label} {required && <span className="text-rose-500">*</span>}
      </div>
      {children}
    </label>
  );
}

function EditorField({ label, children }) {
  return (
    <label className="block">
      <div className="mb-2 text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{label}</div>
      {children}
    </label>
  );
}

function RealtimeMetric({ title, value, detail, tone }) {
  const toneClasses = {
    sky: 'bg-sky-50 text-sky-600 dark:bg-sky-950/30 dark:text-sky-300',
    rose: 'bg-rose-50 text-rose-600 dark:bg-rose-950/30 dark:text-rose-300',
    emerald: 'bg-emerald-50 text-emerald-600 dark:bg-emerald-950/30 dark:text-emerald-300',
    amber: 'bg-amber-50 text-amber-600 dark:bg-amber-950/30 dark:text-amber-300',
  };

  return (
    <div className="flex items-center justify-between rounded-[1.75rem] border border-slate-100 bg-slate-50 px-5 py-4 dark:border-slate-800 dark:bg-slate-800/50">
      <div>
        <div className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{title}</div>
        <div className="mt-1 text-xs font-black uppercase tracking-[0.2em] text-slate-400">{detail}</div>
      </div>
      <div className={`rounded-2xl px-4 py-2 text-sm font-black ${toneClasses[tone]}`}>{value}</div>
    </div>
  );
}

function SettingsToggle({ title, description, checked, onChange }) {
  return (
    <div className="flex items-center justify-between gap-4 rounded-[1.75rem] border border-slate-100 bg-slate-50 px-5 py-4 dark:border-slate-800 dark:bg-slate-800/50">
      <div>
        <div className="text-sm font-black text-slate-900 dark:text-white">{title}</div>
        <div className="mt-1 text-xs font-medium text-slate-500 dark:text-slate-300">{description}</div>
      </div>
      <button
        type="button"
        onClick={() => onChange(!checked)}
        className={`relative h-8 w-14 rounded-full transition-all ${checked ? 'bg-indigo-500' : 'bg-slate-300 dark:bg-slate-700'}`}
      >
        <span className={`absolute top-1 h-6 w-6 rounded-full bg-white shadow transition-all ${checked ? 'left-7' : 'left-1'}`} />
      </button>
    </div>
  );
}

export default App;
