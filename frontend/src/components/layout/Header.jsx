import { useState, useEffect, useRef } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';
import { useTheme } from '../../context/ThemeContext';
import { useOutsideClick } from '../../hooks/useOutsideClick';
import { useNotifications } from '../../hooks/useNotifications';
import { useProfile } from '../../hooks/useProfile';
import { getProjects } from '../../services/projectService';
import { uploadPreview } from '../../services/uploadService';
import { apiRequest } from '../../services/api';
import { jsPDF } from 'jspdf';
import { months } from '../../constants/months';
import { years } from '../../constants/analyticsConstants';
import { buildBatchItems, getSequentialBatchPeriod } from '../../utils/batchHelpers';
import { ImportDropdown } from '../common/ImportDropdown';

export function Header() {
  const location = useLocation();
  const navigate = useNavigate();
  const { user, token, logout } = useAuth();
  const { darkMode, setDarkMode } = useTheme();

  const [isExportOpen, setIsExportOpen] = useState(false);
  const [isImportDialogOpen, setIsImportDialogOpen] = useState(false);
  const [isNotifOpen, setIsNotifOpen] = useState(false);
  const [isProfileOpen, setIsProfileOpen] = useState(false);
  const [availableProjects, setAvailableProjects] = useState([]);
  const [selectedImportProjectId, setSelectedImportProjectId] = useState('');
  const [selectedMonth, setSelectedMonth] = useState('January');
  const [selectedYear, setSelectedYear] = useState('2024');
  const [selectedFile, setSelectedFile] = useState(null);
  const [importMode, setImportMode] = useState('single');
  const [batchFiles, setBatchFiles] = useState([]);
  const [isUploading, setIsUploading] = useState(false);
  const [openImportMenu, setOpenImportMenu] = useState(null);
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

  const { notifications } = useNotifications(token);
  const { profileData, fetchProfile } = useProfile(token);

  useEffect(() => {
    getProjects(token)
      .then((response) => {
        const projects = Array.isArray(response.data) ? response.data : [];
        setAvailableProjects(projects);
        if (activeProjectId) setSelectedImportProjectId(activeProjectId);
        else if (projects.length > 0) setSelectedImportProjectId(String(projects[0].id));
      })
      .catch((error) => console.error('Error fetching projects for import:', error));
  }, [activeProjectId, token]);

  useEffect(() => {
    if (isProfileOpen && !profileData) {
      fetchProfile();
    }
  }, [isProfileOpen, profileData]);

  useOutsideClick(notifRef, () => setIsNotifOpen(false));
  useOutsideClick(profileRef, () => setIsProfileOpen(false));
  useOutsideClick(exportRef, () => setIsExportOpen(false));
  useOutsideClick(searchRef, () => setIsSearchOpen(false));

  const handleFileSelect = (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setSelectedFile(file);
  };

  const openImportDialog = () => setIsImportDialogOpen(true);

  const resetImportDialog = () => {
    setIsImportDialogOpen(false);
    setSelectedFile(null);
    setBatchFiles([]);
    setImportMode('single');
    setIsUploading(false);
    setOpenImportMenu(null);
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

    setBatchFiles((currentFiles) =>
      currentFiles.map((item, index) => ({
        ...item,
        ...getSequentialBatchPeriod(selectedMonth, selectedYear, index),
      }))
    );
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
    if (!selectedImportProjectId) {
      alert('Select a project before importing data.');
      return;
    }

    if (importMode === 'single' && !selectedFile) return;
    if (importMode === 'batch' && batchFiles.length === 0) return;

    try {
      setIsUploading(true);
      const formData = new FormData();
      const batchPeriods = importMode === 'batch'
        ? batchFiles.map((item) => ({ month: item.month, year: item.year }))
        : [{ month: selectedMonth, year: selectedYear }];

      if (importMode === 'single') {
        formData.append('files', selectedFile);
      } else {
        batchFiles.forEach((batchItem) => formData.append('files', batchItem.file));
      }

      formData.append('month', selectedMonth);
      formData.append('year', selectedYear);
      formData.append('batch_periods', JSON.stringify(batchPeriods));

      const response = await uploadPreview(formData, token);

      const previewItems = Array.isArray(response.data?.preview_items) ? response.data.preview_items : [];
      if (previewItems.length === 0) {
        throw new Error('No preview data returned from the uploaded file.');
      }

      const targetProject = availableProjects.find((p) => String(p.id) === String(selectedImportProjectId));
      resetImportDialog();
      navigate(`/import/mapping/${previewItems[0].preview_id}`, {
        state: {
          importMode,
          projectId: selectedImportProjectId,
          projectName: targetProject?.name || `Project ${selectedImportProjectId}`,
          previewItems,
          schemaMismatch: Boolean(response.data?.schema_mismatch),
        },
      });
    } catch (err) {
      setIsUploading(false);
      alert('Upload failed: ' + (err.response?.data?.error || err.message));
    }
  };

  const handleExportPdf = () => {
    const doc = new jsPDF();
    doc.setFontSize(20);
    doc.text('Civora - Dashboard Report', 20, 20);
    doc.setFontSize(12);
    doc.text(`Generated: ${new Date().toLocaleDateString()}`, 20, 35);
    doc.text(`User: ${user?.name || 'Administrator'}`, 20, 45);
    doc.text('Export your project reports from the Reports page for full detail.', 20, 60);
    doc.save('civora_dashboard_report.pdf');
    setIsExportOpen(false);
  };

  const handleExportCsv = async () => {
    try {
      const res = await apiRequest({
        method: 'get',
        url: '/api/projects',
        headers: { Authorization: `Bearer ${token}` },
      });
      const projects = Array.isArray(res.data) ? res.data : [];
      const rows = [['Project Name', 'Store Type', 'Currency', 'Timezone', 'Contact']];
      projects.forEach((p) =>
        rows.push([p.name, p.store_type || '', p.currency_code || '', p.timezone || '', p.contact_number || ''])
      );
      const csv = rows.map((r) => r.map((v) => `"${String(v).replace(/"/g, '""')}"`).join(',')).join('\n');
      const blob = new Blob([csv], { type: 'text/csv' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'projects_export.csv';
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      alert('Export failed: ' + e.message);
    }
    setIsExportOpen(false);
  };

  const unreadCount = notifications.length;
  const notifSeverityIcon = {
    error: 'error',
    warning: 'warning',
    info: 'info',
    low_stock: 'inventory_2',
    declining_sales: 'trending_down',
    market_threat: 'shield',
    business_news: 'newspaper',
  };
  const notifSeverityColor = {
    error: 'text-rose-500 bg-rose-50',
    warning: 'text-amber-500 bg-amber-50',
    info: 'text-sky-500 bg-sky-50',
  };

  return (
    <>
      <header className="h-20 bg-white/80 dark:bg-slate-900/80 backdrop-blur-md border-b border-slate-200 dark:border-slate-800 flex items-center justify-between px-8 sticky top-0 z-30">
        <div className="flex-1 max-w-xl relative" ref={searchRef}>
          <span className="material-symbols-outlined absolute left-4 top-1/2 -translate-y-1/2 text-indigo-500 text-lg">
            auto_awesome
          </span>
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
                <div className="px-4 py-3 text-sm font-medium text-slate-500 dark:text-slate-300">
                  No matching project found.
                </div>
              )}
            </div>
          )}
        </div>
        <div className="flex items-center gap-4">
          <button
            onClick={() => setDarkMode(!darkMode)}
            className="p-2.5 text-gray-400 hover:bg-gray-100 dark:hover:bg-slate-800 rounded-xl transition-all"
          >
            <span className="material-symbols-outlined">{darkMode ? 'light_mode' : 'dark_mode'}</span>
          </button>

          {/* Notifications */}
          <div className="relative" ref={notifRef}>
            <button
              onClick={() => {
                setIsNotifOpen(!isNotifOpen);
                setIsProfileOpen(false);
                setIsExportOpen(false);
              }}
              className="p-2.5 text-gray-400 hover:bg-gray-100 dark:hover:bg-slate-800 rounded-xl transition-all relative"
            >
              <span className="material-symbols-outlined">notifications</span>
              {unreadCount > 0 && (
                <span className="absolute top-1.5 right-1.5 min-w-[18px] h-[18px] bg-rose-500 rounded-full border-2 border-white dark:border-slate-900 text-white text-[9px] font-black flex items-center justify-center px-0.5">
                  {unreadCount > 9 ? '9+' : unreadCount}
                </span>
              )}
            </button>
            {isNotifOpen && (
              <div className="absolute right-0 top-full mt-3 w-96 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-3xl shadow-2xl z-50 overflow-hidden">
                <div className="px-6 py-4 border-b border-slate-100 dark:border-slate-800 flex items-center justify-between">
                  <div>
                    <div className="font-black text-slate-900 dark:text-white">Alerts & Notifications</div>
                    <div className="text-[10px] font-black text-slate-400 uppercase tracking-widest mt-0.5">
                      {unreadCount} active alerts
                    </div>
                  </div>
                  <span className="material-symbols-outlined text-slate-300">notifications_active</span>
                </div>
                <div className="max-h-[420px] overflow-y-auto divide-y divide-slate-50 dark:divide-slate-800">
                  {notifications.length === 0 ? (
                    <div className="px-6 py-10 text-center text-slate-400 text-sm font-bold">No alerts right now</div>
                  ) : (
                    notifications.map((n) => {
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
                              <div className="text-xs font-medium text-slate-500 dark:text-slate-400 mt-0.5 leading-relaxed">
                                {n.message}
                              </div>
                              <div className="text-[10px] font-black text-slate-300 uppercase tracking-widest mt-1">
                                {new Date(n.timestamp).toLocaleDateString()}
                              </div>
                            </div>
                          </div>
                        </div>
                      );
                    })
                  )}
                </div>
              </div>
            )}
          </div>

          <button
            onClick={openImportDialog}
            className="bg-primary text-white px-6 py-2.5 rounded-xl font-bold flex items-center gap-2 hover:shadow-lg hover:shadow-primary/30 transition-all ui-hover"
          >
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
            <button
              onClick={() => {
                setIsExportOpen(!isExportOpen);
                setIsNotifOpen(false);
                setIsProfileOpen(false);
              }}
              className="text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-200 px-4 py-2.5 rounded-xl font-bold flex items-center gap-2 hover:bg-slate-100 dark:hover:bg-slate-800 transition-all"
            >
              <span className="material-symbols-outlined text-sm">download</span> Export
            </button>
            {isExportOpen && (
              <div className="absolute right-0 top-full mt-2 w-64 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-2xl shadow-2xl z-50 overflow-hidden ring-1 ring-black/5">
                <button
                  onClick={handleExportPdf}
                  className="w-full text-left px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-700 flex items-center gap-4 transition-colors"
                >
                  <div className="p-2 rounded-lg bg-slate-50 dark:bg-slate-800 text-rose-500">
                    <span className="material-symbols-outlined text-base leading-none">picture_as_pdf</span>
                  </div>
                  <div>
                    <div className="text-sm font-bold text-slate-900 dark:text-white">PDF Report</div>
                    <div className="text-[10px] text-slate-400 font-medium">Dashboard summary</div>
                  </div>
                </button>
                <button
                  onClick={handleExportCsv}
                  className="w-full text-left px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-700 flex items-center gap-4 transition-colors"
                >
                  <div className="p-2 rounded-lg bg-slate-50 dark:bg-slate-800 text-emerald-500">
                    <span className="material-symbols-outlined text-base leading-none">table_chart</span>
                  </div>
                  <div>
                    <div className="text-sm font-bold text-slate-900 dark:text-white">Excel CSV</div>
                    <div className="text-[10px] text-slate-400 font-medium">Projects raw data</div>
                  </div>
                </button>
                <button
                  onClick={() => {
                    navigate('/reports');
                    setIsExportOpen(false);
                  }}
                  className="w-full text-left px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-700 flex items-center gap-4 transition-colors bg-indigo-50/30 dark:bg-indigo-950/20"
                >
                  <div className="p-2 rounded-lg bg-slate-50 dark:bg-slate-800 text-indigo-600">
                    <span className="material-symbols-outlined text-base leading-none">auto_awesome</span>
                  </div>
                  <div>
                    <div className="text-sm font-bold text-slate-900 dark:text-white flex items-center gap-2">
                      Full Reports <span className="bg-indigo-100 text-indigo-600 text-[8px] px-1.5 py-0.5 rounded font-black">PDF</span>
                    </div>
                    <div className="text-[10px] text-slate-400 font-medium">Per-store SWOT analysis</div>
                  </div>
                </button>
              </div>
            )}
          </div>

          {/* User Profile */}
          <div className="relative border-l border-slate-200 dark:border-slate-800 pl-4" ref={profileRef}>
            <button
              onClick={() => {
                setIsProfileOpen(!isProfileOpen);
                setIsNotifOpen(false);
                setIsExportOpen(false);
              }}
              className="flex items-center gap-3 hover:bg-slate-50 dark:hover:bg-slate-800 rounded-2xl px-3 py-2 transition-all"
            >
              <div className="text-right hidden xl:block">
                <div className="text-sm font-black text-slate-900 dark:text-white leading-none">
                  {user?.name || 'User'}
                </div>
                <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mt-1">
                  {user?.position || 'Administrator'}
                </div>
              </div>
              <div className="w-10 h-10 rounded-full bg-primary/10 flex items-center justify-center text-primary font-black overflow-hidden border-2 border-primary/20">
                {user?.profile_picture ? (
                  <img src={user.profile_picture} alt="avatar" className="w-full h-full object-cover" />
                ) : (
                  user?.name?.charAt(0) || 'U'
                )}
              </div>
              <span className="material-symbols-outlined text-slate-400 text-base hidden xl:block">
                {isProfileOpen ? 'expand_less' : 'expand_more'}
              </span>
            </button>

            {isProfileOpen && (
              <div className="absolute right-0 top-full mt-3 w-80 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-3xl shadow-2xl z-50 overflow-hidden">
                {/* Profile header */}
                <div className="px-6 py-5 bg-gradient-to-br from-primary/10 to-indigo-50 dark:from-primary/20 dark:to-slate-800 border-b border-slate-100 dark:border-slate-800">
                  <div className="flex items-center gap-4">
                    <div className="w-14 h-14 rounded-2xl bg-primary/10 border-2 border-primary/20 flex items-center justify-center text-primary font-black text-xl overflow-hidden">
                      {(profileData?.user || user)?.profile_picture ? (
                        <img
                          src={(profileData?.user || user).profile_picture}
                          alt="avatar"
                          className="w-full h-full object-cover"
                        />
                      ) : (
                        (profileData?.user?.name || user?.name || 'U').charAt(0)
                      )}
                    </div>
                    <div>
                      <div className="font-black text-slate-900 dark:text-white text-base">
                        {profileData?.user?.name || user?.name || 'User'}
                      </div>
                      <div className="text-xs font-bold text-slate-500 dark:text-slate-400">
                        {profileData?.user?.email || user?.email || ''}
                      </div>
                      <div className="mt-1 inline-flex items-center gap-1 bg-primary/10 text-primary text-[9px] font-black uppercase tracking-widest px-2 py-0.5 rounded-full">
                        {profileData?.user?.position || user?.position || 'Administrator'}
                      </div>
                    </div>
                  </div>
                </div>

                {/* Stores owned */}
                <div className="px-6 py-4">
                  <div className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400 mb-3">Stores Owned</div>
                  {profileData?.stores?.length > 0 ? (
                    <div className="space-y-2 max-h-40 overflow-y-auto">
                      {profileData.stores.map((store) => (
                        <button
                          key={store.id}
                          onClick={() => {
                            navigate(`/advanced-analytics/${store.id}`, { state: { projectName: store.name } });
                            setIsProfileOpen(false);
                          }}
                          className="w-full flex items-center gap-3 p-3 rounded-2xl hover:bg-slate-50 dark:hover:bg-slate-800 transition-colors text-left"
                        >
                          <div className="w-8 h-8 rounded-xl bg-primary/10 flex items-center justify-center text-primary shrink-0">
                            <span className="material-symbols-outlined text-base">storefront</span>
                          </div>
                          <div className="min-w-0">
                            <div className="text-sm font-black text-slate-900 dark:text-white truncate">{store.name}</div>
                            <div className="text-[10px] font-bold text-slate-400 uppercase">
                              {store.store_type || 'General'}
                            </div>
                          </div>
                          <span className="material-symbols-outlined text-slate-300 text-base ml-auto shrink-0">
                            arrow_forward
                          </span>
                        </button>
                      ))}
                    </div>
                  ) : (
                    <div className="text-sm text-slate-400 font-medium py-2">No stores yet</div>
                  )}
                </div>

                <div className="px-6 pb-4 border-t border-slate-100 dark:border-slate-800 pt-3">
                  <button
                    onClick={() => {
                      navigate('/settings');
                      setIsProfileOpen(false);
                    }}
                    className="w-full flex items-center gap-3 p-3 rounded-2xl hover:bg-slate-50 dark:hover:bg-slate-800 transition-colors text-left"
                  >
                    <span className="material-symbols-outlined text-slate-400 text-base">settings</span>
                    <span className="text-sm font-bold text-slate-700 dark:text-slate-200">Account Settings</span>
                  </button>
                  <button
                    onClick={() => {
                      setIsProfileOpen(false);
                      logout();
                    }}
                    className="mt-2 w-full flex items-center gap-3 p-3 rounded-2xl text-left text-red-500 hover:bg-red-50 dark:hover:bg-red-950/30 transition-colors"
                  >
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
          <div
            className="absolute inset-0 bg-slate-950/55 backdrop-blur-sm transition-opacity duration-300 animate-in fade-in"
            onClick={resetImportDialog}
          ></div>
          <div className="relative max-h-[calc(100vh-4rem)] overflow-y-auto p-6 md:max-h-[min(85vh,900px)] md:p-10 bg-white dark:bg-slate-900 rounded-3xl w-full max-w-5xl shadow-2xl">
            <div className="mb-8 flex items-center gap-4">
              <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-white text-indigo-600 shadow-sm ring-1 ring-indigo-100 dark:bg-slate-800 dark:ring-slate-700">
                <span className="material-symbols-outlined text-3xl">upload_file</span>
              </div>
              <div>
                <h3 className="text-2xl font-black tracking-tighter">Import Data</h3>
                <p className="text-sm text-slate-500 dark:text-slate-400">
                  Import one file or multiple month files in one run.
                </p>
              </div>
            </div>

            <div className="mb-6 inline-flex rounded-2xl border border-slate-200 bg-slate-50 p-1 dark:border-slate-700 dark:bg-slate-800">
              <button
                type="button"
                onClick={() => setImportMode('single')}
                className={`rounded-xl px-4 py-2 text-xs font-black uppercase tracking-[0.2em] transition-all ${
                  importMode === 'single' ? 'bg-white text-primary shadow-sm dark:bg-slate-900' : 'text-slate-500'
                }`}
              >
                Single Month
              </button>
              <button
                type="button"
                onClick={() => setImportMode('batch')}
                className={`rounded-xl px-4 py-2 text-xs font-black uppercase tracking-[0.2em] transition-all ${
                  importMode === 'batch' ? 'bg-white text-primary shadow-sm dark:bg-slate-900' : 'text-slate-500'
                }`}
              >
                Multi-Month Batch
              </button>
            </div>

            <div
              className={`grid gap-5 ${
                importMode === 'single' ? 'md:grid-cols-2 xl:grid-cols-3' : 'md:grid-cols-1 xl:grid-cols-1'
              }`}
            >
              <ImportDropdown
                label="Project"
                value={
                  availableProjects.find((project) => String(project.id) === String(selectedImportProjectId))?.name ||
                  'Select project'
                }
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
                      ? selectedFile
                        ? selectedFile.name
                        : 'Upload a CSV or Excel file for the selected reporting period.'
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
                  {importMode === 'single'
                    ? selectedFile
                      ? 'Change File'
                      : 'Choose File'
                    : batchFiles.length > 0
                    ? 'Add / Change Files'
                    : 'Choose Files'}
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
                    <div
                      key={batchItem.id}
                      className="grid grid-cols-[1.6fr_1fr_1fr] items-center gap-3 border-t border-slate-100 px-4 py-3 dark:border-slate-800"
                    >
                      <div className="min-w-0">
                        <span className="truncate text-sm font-bold text-slate-700 dark:text-slate-200">
                          {batchItem.name}
                        </span>
                        <div className="mt-1 text-[11px] font-medium text-slate-400 dark:text-slate-500">
                          File {index + 1}
                        </div>
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
                    ? `${
                        availableProjects.find((project) => String(project.id) === String(selectedImportProjectId))?.name ||
                        'Select project'
                      } | ${selectedMonth} ${selectedYear}`
                    : `${
                        availableProjects.find((project) => String(project.id) === String(selectedImportProjectId))?.name ||
                        'Select project'
                      } | ${batchFiles.length} month file(s) from ${selectedMonth} ${selectedYear}`}
                </p>
              </div>
              <span className="rounded-full bg-primary/10 px-3 py-1 text-xs font-black uppercase tracking-[0.2em] text-primary">
                Ready
              </span>
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
                {isUploading ? 'Preparing...' : importMode === 'single' ? 'Continue To Mapping' : 'Review Column Mapping'}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
export default Header;
