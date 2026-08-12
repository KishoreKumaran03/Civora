import { useState } from 'react';
import { useAuth } from '../../context/AuthContext';

export function SettingsPage() {
  const { user } = useAuth();
  const [settings, setSettings] = useState(() => {
    try {
      return {
        display_name: user?.name || 'Administrator',
        email: user?.email || '',
        workspace_name: 'Civora Workspace',
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
        workspace_name: 'Civora Workspace',
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

// Local helpers to keep structure visually exact
function AnalyticsPanel({ title, subtitle, children }) {
  return (
    <div className="relative rounded-[2.5rem] border border-slate-100 bg-white p-8 shadow-xl dark:border-slate-800 dark:bg-slate-900">
      <div className="mb-6">
        <div className="text-[10px] font-black uppercase tracking-[0.3em] text-slate-400">{title}</div>
        <h3 className="mt-2 text-2xl font-black tracking-tighter text-slate-900 dark:text-white">{subtitle}</h3>
      </div>
      {children}
    </div>
  );
}

function ProjectFormField({ label, children }) {
  return (
    <label className="block">
      <div className="mb-2 text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{label}</div>
      {children}
    </label>
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

export default SettingsPage;
