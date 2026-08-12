import { Link, useLocation } from 'react-router-dom';

export function NavLink({ to, icon, label, isCollapsed }) {
  const { pathname } = useLocation();
  const isActive = (to === '/' ? pathname === '/' : pathname.startsWith(to)) && to !== '#';

  return (
    <Link
      to={to}
      className={`flex items-center gap-3 p-3 rounded-xl transition-all ${
        isActive
          ? 'bg-primary/10 text-primary font-bold shadow-sm'
          : 'text-slate-500 dark:text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-800'
      } ${isCollapsed ? 'justify-center mx-2' : ''}`}
    >
      <span className="material-symbols-outlined text-[22px]">{icon}</span>
      {!isCollapsed && <span className="text-sm tracking-tight">{label}</span>}
    </Link>
  );
}
export default NavLink;
