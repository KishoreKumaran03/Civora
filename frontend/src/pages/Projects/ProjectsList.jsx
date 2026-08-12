import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';
import { useProjects } from '../../hooks/useProjects';

export function ProjectCard({ project, isFavorite, onToggleFavorite, onEditProject }) {
  const navigate = useNavigate();
  const canEditProject = typeof onEditProject === 'function';

  return (
    <div
      onClick={() => navigate(`/advanced-analytics/${project.id}`, { state: { projectName: project.name } })}
      className="bg-white dark:bg-slate-900 p-8 rounded-[3rem] shadow-xl border border-slate-100 dark:border-slate-800 cursor-pointer transition-all hover:-translate-y-3 group"
    >
      <div
        className="h-48 rounded-[2.5rem] flex items-center justify-center mb-8 transition-all relative overflow-hidden bg-slate-50 dark:bg-slate-800 group-hover:bg-primary/5"
        style={
          project.store_logo_url
            ? {
                backgroundImage: `url(${project.store_logo_url})`,
                backgroundSize: 'cover',
                backgroundPosition: 'center',
              }
            : undefined
        }
      >
        <button
          type="button"
          onClick={(event) => {
            event.stopPropagation();
            onToggleFavorite(project.id);
          }}
          className={`absolute left-4 top-4 z-10 flex h-10 w-10 items-center justify-center rounded-full border transition-all opacity-0 pointer-events-none group-hover:opacity-100 group-hover:pointer-events-auto focus-visible:opacity-100 focus-visible:pointer-events-auto ${
            isFavorite ? 'border-amber-300 bg-amber-100 text-amber-500' : 'border-white/70 bg-white/85 text-slate-500 hover:text-amber-500'
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
          <span className="material-symbols-outlined text-7xl text-slate-200 transition-all duration-500 group-hover:text-primary group-hover:scale-110">
            storefront
          </span>
        )}
        <div className="absolute bottom-4 right-4 bg-emerald-100 text-emerald-600 px-3 py-1 rounded-full text-[8px] font-black uppercase">
          Active
        </div>
      </div>
      <h3 className="text-2xl font-black tracking-tight mb-2 group-hover:text-primary transition-colors">
        {project.name}
      </h3>
      <div className="flex items-center justify-between mt-6 pt-6 border-t border-slate-50 dark:border-slate-800">
        <div className="text-[10px] font-black text-slate-400 uppercase tracking-widest">
          {isFavorite ? 'Favorite Store' : 'Store Entry'}
        </div>
        <span className="material-symbols-outlined text-slate-300 group-hover:text-primary transition-colors">
          arrow_forward
        </span>
      </div>
    </div>
  );
}

export function ProjectsList() {
  const { token } = useAuth();
  const navigate = useNavigate();
  const {
    projects,
    favoriteProjectIds,
    toggleFavorite,
    createProject,
    updateProject
  } = useProjects(token);

  const [isCreateOpen, setIsCreateOpen] = useState(false);
  const [isEditOpen, setIsEditOpen] = useState(false);
  const [isCreatingProject, setIsCreatingProject] = useState(false);
  const [isUpdatingProject, setIsUpdatingProject] = useState(false);
  const [editingProjectId, setEditingProjectId] = useState(null);
  const [projectLogoFileName, setProjectLogoFileName] = useState('');

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
      } catch {}
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
      } catch {}
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
    if (!file) return;

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
      const created = await createProject({
        ...projectForm,
        store_segments: projectForm.store_segments
          .split(',')
          .map((segment) => segment.trim())
          .filter(Boolean),
        opening_balances: parsedOpeningBalances,
      });

      setIsCreateOpen(false);
      resetProjectForm();
      alert('Project created successfully.');
      navigate(`/advanced-analytics/${created.id}`, { state: { projectName: created.name } });
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
      await updateProject(editingProjectId, {
        ...projectForm,
        store_segments: normalizeStoreSegmentsInput(projectForm.store_segments),
        opening_balances: parsedOpeningBalances,
      });

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
        <div>
          <h1 className="text-4xl font-black tracking-tighter">Managed Stores</h1>
          <p className="text-slate-400 font-bold uppercase text-xs tracking-widest mt-2">Strategic Deployment Hub</p>
        </div>
        <button
          onClick={() => setIsCreateOpen(true)}
          className="bg-slate-900 dark:bg-indigo-600 text-white px-8 py-4 rounded-[2rem] font-black shadow-2xl shadow-indigo-600/20 active:scale-95 transition-all"
        >
          CREATE NEW ENTITY
        </button>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-8">
        {projects.map((project) => (
          <ProjectCard
            key={project.id}
            project={project}
            isFavorite={favoriteProjectIds.includes(String(project.id))}
            onToggleFavorite={toggleFavorite}
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
                <h2 className="mt-3 text-3xl font-black tracking-tighter text-slate-900 dark:text-white">
                  Create a new store workspace
                </h2>
                <p className="mt-2 text-sm font-medium text-slate-500 dark:text-slate-300">
                  Capture the store profile, branch details, tax settings, stock defaults, and owner/admin contacts in
                  one step.
                </p>
              </div>
              <button
                onClick={() => setIsCreateOpen(false)}
                className="rounded-full p-2 text-slate-400 transition-all hover:bg-slate-100 hover:text-slate-900 dark:hover:bg-slate-800 dark:hover:text-white"
              >
                <span className="material-symbols-outlined">close</span>
              </button>
            </div>

            <form onSubmit={handleCreateProject} className="space-y-8">
              <div className="grid grid-cols-1 gap-5 md:grid-cols-2">
                <ProjectFormField label="Store Name" required>
                  <input
                    value={projectForm.name}
                    onChange={(event) => handleProjectFormChange('name', event.target.value)}
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="ABC Stores"
                  />
                </ProjectFormField>
                <ProjectFormField label="Store Category / Type">
                  <input
                    value={projectForm.store_type}
                    onChange={(event) => handleProjectFormChange('store_type', event.target.value)}
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="General store, grocery, electronics, apparel"
                  />
                </ProjectFormField>
                <ProjectFormField label="Store Segments">
                  <input
                    value={projectForm.store_segments}
                    onChange={(event) => handleProjectFormChange('store_segments', event.target.value)}
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="Groceries, Electronics, Furniture"
                  />
                </ProjectFormField>
                <ProjectFormField label="Branch / Location ID">
                  <input
                    value={projectForm.branch_location_id}
                    onChange={(event) => handleProjectFormChange('branch_location_id', event.target.value)}
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="CHN-OMR-001"
                  />
                </ProjectFormField>
                <ProjectFormField label="Store Logo">
                  <div className="space-y-3">
                    <label className="flex min-h-[7rem] cursor-pointer flex-col items-center justify-center rounded-[1.75rem] border border-dashed border-slate-300 bg-slate-50 px-4 py-4 text-center transition-all hover:border-indigo-400 hover:bg-white dark:border-slate-700 dark:bg-slate-800/70 dark:hover:border-indigo-500 dark:hover:bg-slate-800">
                      <input type="file" accept="image/*" onChange={handleProjectLogoSelect} className="hidden" />
                      <span className="material-symbols-outlined text-3xl text-indigo-500">image</span>
                      <span className="mt-2 text-sm font-black text-slate-900 dark:text-white">
                        {projectLogoFileName || 'Upload store logo'}
                      </span>
                      <span className="mt-1 text-xs font-medium text-slate-400">
                        PNG, JPG, WEBP and other image formats are supported
                      </span>
                    </label>
                    {projectForm.store_logo_url && (
                      <div className="overflow-hidden rounded-[1.75rem] border border-slate-200 bg-slate-50 p-2 dark:border-slate-700 dark:bg-slate-800/70">
                        <img
                          src={projectForm.store_logo_url}
                          alt="Store logo preview"
                          className="h-28 w-full rounded-[1.25rem] object-cover"
                        />
                      </div>
                    )}
                  </div>
                </ProjectFormField>
                <ProjectFormField label="Currency">
                  <input
                    value={projectForm.currency_code}
                    onChange={(event) => handleProjectFormChange('currency_code', event.target.value)}
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="INR"
                  />
                </ProjectFormField>
                <ProjectFormField label="Time Zone">
                  <input
                    value={projectForm.timezone}
                    onChange={(event) => handleProjectFormChange('timezone', event.target.value)}
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="Asia/Kolkata"
                  />
                </ProjectFormField>
                <ProjectFormField label="Tax Identification Number">
                  <input
                    value={projectForm.tax_identification_number}
                    onChange={(event) => handleProjectFormChange('tax_identification_number', event.target.value)}
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="GSTIN / VAT / TIN"
                  />
                </ProjectFormField>
                <ProjectFormField label="Default Tax Rate (%)">
                  <input
                    value={projectForm.default_tax_rate}
                    onChange={(event) => handleProjectFormChange('default_tax_rate', event.target.value)}
                    type="number"
                    min="0"
                    step="0.01"
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="18"
                  />
                </ProjectFormField>
                <ProjectFormField label="Low Stock Threshold">
                  <input
                    value={projectForm.low_stock_threshold}
                    onChange={(event) => handleProjectFormChange('low_stock_threshold', event.target.value)}
                    type="number"
                    min="0"
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="10"
                  />
                </ProjectFormField>
                <ProjectFormField label="Owner / Admin Email">
                  <input
                    value={projectForm.owner_admin_email}
                    onChange={(event) => handleProjectFormChange('owner_admin_email', event.target.value)}
                    type="email"
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="owner@abcstores.com"
                  />
                </ProjectFormField>
                <ProjectFormField label="Contact Number">
                  <input
                    value={projectForm.contact_number}
                    onChange={(event) => handleProjectFormChange('contact_number', event.target.value)}
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="+91 98765 43210"
                  />
                </ProjectFormField>
              </div>

              <ProjectFormField label="Opening Balances">
                <textarea
                  value={projectForm.opening_balances}
                  onChange={(event) => handleProjectFormChange('opening_balances', event.target.value)}
                  rows={5}
                  className="min-h-[140px] w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                  placeholder={'Rice Bag: 120\nLED TV: 15\nOffice Chair: 42'}
                />
                <p className="mt-2 text-xs font-medium text-slate-400">
                  Use one product per line in the format `Product Name: Quantity`.
                </p>
              </ProjectFormField>

              <div className="flex flex-col-reverse gap-3 sm:flex-row sm:justify-end">
                <button
                  type="button"
                  onClick={() => setIsCreateOpen(false)}
                  className="rounded-2xl border border-slate-200 px-5 py-3 font-bold text-slate-600 transition-all hover:bg-slate-50 dark:border-slate-700 dark:text-slate-200 dark:hover:bg-slate-800"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={isCreatingProject}
                  className="rounded-2xl bg-indigo-600 px-5 py-3 font-black text-white shadow-lg shadow-indigo-600/20 transition-all hover:-translate-y-0.5 hover:bg-indigo-500 disabled:cursor-not-allowed disabled:opacity-60"
                >
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
                <h2 className="mt-3 text-3xl font-black tracking-tighter text-slate-900 dark:text-white">
                  Update store details
                </h2>
                <p className="mt-2 text-sm font-medium text-slate-500 dark:text-slate-300">
                  Edit your store profile, tax settings, contacts, and opening balances.
                </p>
              </div>
              <button
                onClick={() => setIsEditOpen(false)}
                className="rounded-full p-2 text-slate-400 transition-all hover:bg-slate-100 hover:text-slate-900 dark:hover:bg-slate-800 dark:hover:text-white"
              >
                <span className="material-symbols-outlined">close</span>
              </button>
            </div>

            <form onSubmit={handleUpdateProject} className="space-y-8">
              <div className="grid grid-cols-1 gap-5 md:grid-cols-2">
                <ProjectFormField label="Store Name" required>
                  <input
                    value={projectForm.name}
                    onChange={(event) => handleProjectFormChange('name', event.target.value)}
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="ABC Stores"
                  />
                </ProjectFormField>
                <ProjectFormField label="Store Category / Type">
                  <input
                    value={projectForm.store_type}
                    onChange={(event) => handleProjectFormChange('store_type', event.target.value)}
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="General store, grocery, electronics, apparel"
                  />
                </ProjectFormField>
                <ProjectFormField label="Store Segments">
                  <input
                    value={projectForm.store_segments}
                    onChange={(event) => handleProjectFormChange('store_segments', event.target.value)}
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="Groceries, Electronics, Furniture"
                  />
                </ProjectFormField>
                <ProjectFormField label="Branch / Location ID">
                  <input
                    value={projectForm.branch_location_id}
                    onChange={(event) => handleProjectFormChange('branch_location_id', event.target.value)}
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="CHN-OMR-001"
                  />
                </ProjectFormField>
                <ProjectFormField label="Store Logo">
                  <div className="space-y-3">
                    <label className="flex min-h-[7rem] cursor-pointer flex-col items-center justify-center rounded-[1.75rem] border border-dashed border-slate-300 bg-slate-50 px-4 py-4 text-center transition-all hover:border-indigo-400 hover:bg-white dark:border-slate-700 dark:bg-slate-800/70 dark:hover:border-indigo-500 dark:hover:bg-slate-800">
                      <input type="file" accept="image/*" onChange={handleProjectLogoSelect} className="hidden" />
                      <span className="material-symbols-outlined text-3xl text-indigo-500">image</span>
                      <span className="mt-2 text-sm font-black text-slate-900 dark:text-white">
                        {projectLogoFileName || 'Upload store logo'}
                      </span>
                      <span className="mt-1 text-xs font-medium text-slate-400">
                        PNG, JPG, WEBP and other image formats are supported
                      </span>
                    </label>
                    {projectForm.store_logo_url && (
                      <div className="overflow-hidden rounded-[1.75rem] border border-slate-200 bg-slate-50 p-2 dark:border-slate-700 dark:bg-slate-800/70">
                        <img
                          src={projectForm.store_logo_url}
                          alt="Store logo preview"
                          className="h-28 w-full rounded-[1.25rem] object-cover"
                        />
                      </div>
                    )}
                  </div>
                </ProjectFormField>
                <ProjectFormField label="Currency">
                  <input
                    value={projectForm.currency_code}
                    onChange={(event) => handleProjectFormChange('currency_code', event.target.value)}
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="INR"
                  />
                </ProjectFormField>
                <ProjectFormField label="Time Zone">
                  <input
                    value={projectForm.timezone}
                    onChange={(event) => handleProjectFormChange('timezone', event.target.value)}
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="Asia/Kolkata"
                  />
                </ProjectFormField>
                <ProjectFormField label="Tax Identification Number">
                  <input
                    value={projectForm.tax_identification_number}
                    onChange={(event) => handleProjectFormChange('tax_identification_number', event.target.value)}
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="GSTIN / VAT / TIN"
                  />
                </ProjectFormField>
                <ProjectFormField label="Default Tax Rate (%)">
                  <input
                    value={projectForm.default_tax_rate}
                    onChange={(event) => handleProjectFormChange('default_tax_rate', event.target.value)}
                    type="number"
                    min="0"
                    step="0.01"
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="18"
                  />
                </ProjectFormField>
                <ProjectFormField label="Low Stock Threshold">
                  <input
                    value={projectForm.low_stock_threshold}
                    onChange={(event) => handleProjectFormChange('low_stock_threshold', event.target.value)}
                    type="number"
                    min="0"
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="8"
                  />
                </ProjectFormField>
                <ProjectFormField label="Owner / Admin Email">
                  <input
                    value={projectForm.owner_admin_email}
                    onChange={(event) => handleProjectFormChange('owner_admin_email', event.target.value)}
                    type="email"
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="owner@example.com"
                  />
                </ProjectFormField>
                <ProjectFormField label="Contact Number">
                  <input
                    value={projectForm.contact_number}
                    onChange={(event) => handleProjectFormChange('contact_number', event.target.value)}
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                    placeholder="+91 98765 43210"
                  />
                </ProjectFormField>
              </div>

              <ProjectFormField label="Opening Balances">
                <textarea
                  value={projectForm.opening_balances}
                  onChange={(event) => handleProjectFormChange('opening_balances', event.target.value)}
                  rows={5}
                  className="min-h-[140px] w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm font-medium text-slate-900 outline-none transition-all focus:border-indigo-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                  placeholder={'Rice Bag: 120\nLED TV: 15\nOffice Chair: 42'}
                />
                <p className="mt-2 text-xs font-medium text-slate-400">
                  Use one product per line in the format `Product Name: Quantity`.
                </p>
              </ProjectFormField>

              <div className="flex flex-col-reverse gap-3 sm:flex-row sm:justify-end">
                <button
                  type="button"
                  onClick={() => setIsEditOpen(false)}
                  className="rounded-2xl border border-slate-200 px-5 py-3 font-bold text-slate-600 transition-all hover:bg-slate-50 dark:border-slate-700 dark:text-slate-200 dark:hover:bg-slate-800"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={isUpdatingProject}
                  className="rounded-2xl bg-indigo-600 px-5 py-3 font-black text-white shadow-lg shadow-indigo-600/20 transition-all hover:-translate-y-0.5 hover:bg-indigo-500 disabled:cursor-not-allowed disabled:opacity-60"
                >
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

export default ProjectsList;
