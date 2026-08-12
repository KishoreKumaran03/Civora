import { useAuth } from '../../context/AuthContext';
import { useProjects } from '../../hooks/useProjects';
import { ProjectCard } from './ProjectsList';

export function FavoritesList() {
  const { token } = useAuth();
  const { projects, favoriteProjectIds, toggleFavorite } = useProjects(token);

  const favoriteProjects = projects.filter((project) => favoriteProjectIds.includes(String(project.id)));

  return (
    <div className="p-10 space-y-10 max-w-7xl mx-auto">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-4xl font-black tracking-tighter">Favorites</h1>
          <p className="text-slate-400 font-bold uppercase text-xs tracking-widest mt-2">
            Pinned Store Workspaces
          </p>
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
              onToggleFavorite={toggleFavorite}
            />
          ))}
        </div>
      ) : (
        <div className="rounded-[3rem] border border-dashed border-slate-300 bg-white p-12 text-center shadow-sm dark:border-slate-700 dark:bg-slate-900">
          <div className="mx-auto flex h-20 w-20 items-center justify-center rounded-full bg-amber-50 text-amber-500 dark:bg-amber-950/30 dark:text-amber-300">
            <span className="material-symbols-outlined text-4xl">star</span>
          </div>
          <h2 className="mt-6 text-2xl font-black tracking-tight text-slate-900 dark:text-white">
            No favorite projects yet
          </h2>
          <p className="mt-3 text-sm font-medium text-slate-500 dark:text-slate-300">
            Open `My Projects` and click the star on any store card to pin it here.
          </p>
        </div>
      )}
    </div>
  );
}

export default FavoritesList;
