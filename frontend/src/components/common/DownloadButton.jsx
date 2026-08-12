import { downloadChart } from '../../utils/exportUtils';

export function DownloadButton({ chartRef, filename, className = '' }) {
  return (
    <button
      onClick={() => downloadChart(chartRef, filename)}
      data-export-hidden="true"
      className={`absolute right-6 top-6 z-10 flex h-11 w-11 items-center justify-center rounded-2xl bg-white text-blue-600 shadow-lg shadow-slate-900/10 ring-1 ring-slate-200 transition-all hover:-translate-y-0.5 hover:bg-blue-50 hover:text-blue-700 dark:bg-slate-900 dark:text-sky-300 dark:ring-slate-700 dark:hover:bg-slate-800 ${className}`}
      title="Download as PNG"
      aria-label="Download as PNG"
    >
      <span className="material-symbols-outlined text-[22px] leading-none">download</span>
    </button>
  );
}
export default DownloadButton;
