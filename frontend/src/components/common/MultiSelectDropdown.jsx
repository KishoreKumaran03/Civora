import { useState, useEffect, useRef } from 'react';

export function MultiSelectDropdown({ label, options, selectedValues, onChange, placeholder = 'Select values', compactLabel = false }) {
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
        className={`flex h-12 w-full items-center justify-between rounded-2xl border px-4 text-left text-sm font-medium transition-all ${isOpen
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
                  className={`flex w-full items-center gap-3 rounded-xl px-3 py-2.5 text-left text-sm font-semibold transition-colors ${checked
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
export default MultiSelectDropdown;
