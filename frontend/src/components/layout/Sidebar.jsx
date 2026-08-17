import NavLink from './NavLink';
import { useAIAssistant } from '../../context/AIAssistantContext';

export function Sidebar({ isSidebarCollapsed, setIsSidebarCollapsed }) {
  const { openAssistant } = useAIAssistant();

  return (
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
          {!isSidebarCollapsed && (
            <span className="text-xl font-bold tracking-tight text-primary font-display uppercase whitespace-nowrap">
              Civora
            </span>
          )}
        </div>
      </div>

      <nav className="flex-1 px-4 py-4 space-y-6 overflow-y-auto no-scrollbar">
        <button
          type="button"
          onClick={() => openAssistant()}
          className={`bg-indigo-50 dark:bg-indigo-900/20 text-indigo-600 dark:text-indigo-400 rounded-xl font-semibold border border-indigo-100 dark:border-indigo-800 transition-all ui-hover shadow-sm ${isSidebarCollapsed ? 'mx-auto flex h-12 w-12 items-center justify-center' : 'w-full flex items-center gap-3 p-3'}`}
          aria-label="Open Ask Yua AI chat"
          title="Open Ask Yua AI chat"
        >
          <span className="material-symbols-outlined text-indigo-500">auto_awesome</span>
          {!isSidebarCollapsed && <span>Ask Yua AI</span>}
        </button>

        <div className="space-y-1">
          <NavLink to="/" icon="dashboard" label="Dashboard" isCollapsed={isSidebarCollapsed} />
          <NavLink to="/favorites" icon="star" label="Favorites" isCollapsed={isSidebarCollapsed} />
        </div>

        <div className="space-y-1">
          {!isSidebarCollapsed && (
            <h3 className="px-3 text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-[2px] mb-2 mt-4">
              MY DATA
            </h3>
          )}
          <NavLink to="/projects" icon="folder_open" label="My Projects" isCollapsed={isSidebarCollapsed} />
        </div>

        <div className="space-y-1">
          {!isSidebarCollapsed && (
            <h3 className="px-3 text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-[2px] mb-2 mt-4">
              ANALYSIS
            </h3>
          )}
          <NavLink to="/advanced-analytics" icon="insights" label="Advanced Analytics" isCollapsed={isSidebarCollapsed} />
          <NavLink to="/reports" icon="description" label="Reports" isCollapsed={isSidebarCollapsed} />
        </div>
      </nav>

      <div className="p-4 border-t border-slate-200 dark:border-slate-800">
        <NavLink to="/settings" icon="settings" label="Settings" isCollapsed={isSidebarCollapsed} />
      </div>
    </aside>
  );
}
export default Sidebar;
