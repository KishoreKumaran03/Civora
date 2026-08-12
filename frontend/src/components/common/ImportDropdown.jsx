export function ImportDropdown({ label, value, options, isOpen, onToggle, onSelect }) {
  return (
    <div className="relative space-y-2">
      <label className="text-xs font-black uppercase tracking-[0.2em] text-slate-400">{label}</label>
      <button
        type="button"
        onClick={onToggle}
        className={`flex w-full items-center justify-between rounded-2xl border px-4 py-3.5 text-left font-semibold transition-all ${isOpen
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
                className={`flex w-full items-center justify-between rounded-xl px-3 py-2.5 text-sm font-semibold transition-colors ${option.label === value
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
export default ImportDropdown;
