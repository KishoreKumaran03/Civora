import { useState, useEffect } from 'react';
import { useParams, useLocation, useNavigate } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';
import { apiRequest } from '../../services/api';
import { IMPORT_MAPPING_FIELDS } from '../../constants/analyticsConstants';

export function UploadColumnMappingPage() {
  const { previewId } = useParams();
  const location = useLocation();
  const navigate = useNavigate();
  const { token } = useAuth();
  const statePayload = location.state || {};
  const [previewItems, setPreviewItems] = useState(() => Array.isArray(statePayload.previewItems) ? statePayload.previewItems : []);
  const [projectId] = useState(() => statePayload.projectId || '');
  const [projectName] = useState(() => statePayload.projectName || 'Selected Project');
  const [importMode] = useState(() => statePayload.importMode || 'single');
  const [schemaMismatch] = useState(() => Boolean(statePayload.schemaMismatch));
  const [mapping, setMapping] = useState(() => ({
    Product: '',
    Category: '__skip__',
    Region: '',
    Revenue: '',
    Cost: '',
    Quantity: '',
  }));
  const [error, setError] = useState('');
  const [isLoading, setIsLoading] = useState(previewItems.length === 0);
  const [isSubmitting, setIsSubmitting] = useState(false);

  useEffect(() => {
    if (previewItems.length > 0 || !previewId || !token) return;

    apiRequest({
      method: 'get',
      url: `/api/upload/preview/${previewId}`,
      headers: { Authorization: `Bearer ${token}` }
    }).then((response) => {
      setPreviewItems([response.data]);
      setIsLoading(false);
    }).catch((requestError) => {
      setError(requestError.response?.data?.error || 'Unable to load the uploaded file preview.');
      setIsLoading(false);
    });
  }, [previewId, previewItems.length, token]);

  const activePreview = previewItems[0];
  const availableColumns = Array.isArray(activePreview?.columns) ? activePreview.columns : [];

  const handleSubmit = async () => {
    const missingRequired = IMPORT_MAPPING_FIELDS
      .filter((field) => field.required)
      .filter((field) => !mapping[field.key]);

    if (missingRequired.length > 0) {
      setError(`Select columns for: ${missingRequired.map((field) => field.key).join(', ')}.`);
      return;
    }

    const duplicateSelections = Object.entries(mapping)
      .filter(([, value]) => value && value !== '__skip__')
      .reduce((accumulator, [fieldKey, value]) => {
        accumulator[value] = accumulator[value] || [];
        accumulator[value].push(fieldKey);
        return accumulator;
      }, {});
    const duplicateColumns = Object.entries(duplicateSelections).filter(([, fieldKeys]) => fieldKeys.length > 1);

    if (duplicateColumns.length > 0) {
      setError(`Each parameter should use a different source column. Duplicate: ${duplicateColumns.map(([column]) => column).join(', ')}.`);
      return;
    }

    try {
      setIsSubmitting(true);
      setError('');
      await apiRequest({
        method: 'post',
        url: '/api/upload/complete',
        headers: { Authorization: `Bearer ${token}` },
        data: {
          project_id: projectId,
          preview_id: previewItems.length === 1 ? activePreview?.preview_id : undefined,
          preview_items: previewItems.length > 1
            ? previewItems.map((item) => ({ preview_id: item.preview_id, month: item.month, year: item.year }))
            : undefined,
          column_mapping: mapping,
        }
      });

      alert(importMode === 'batch'
        ? `Imported ${previewItems.length} file(s) with your selected column mapping.`
        : 'Imported the file with your selected column mapping.');
      navigate(`/advanced-analytics/${projectId}`, { state: { projectName } });
      window.location.reload();
    } catch (requestError) {
      setError(requestError.response?.data?.error || 'Import failed while processing the selected columns.');
      setIsSubmitting(false);
    }
  };

  if (isLoading) {
    return (
      <div className="min-h-screen bg-slate-50 px-6 py-10 dark:bg-slate-950">
        <div className="mx-auto max-w-5xl rounded-[2rem] border border-slate-200 bg-white p-8 shadow-xl dark:border-slate-800 dark:bg-slate-900">
          <p className="text-sm font-bold text-slate-500 dark:text-slate-400">Loading uploaded file preview...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_top_left,_rgba(14,165,233,0.14),_transparent_35%),linear-gradient(180deg,_#f8fafc_0%,_#eef6ff_100%)] px-4 py-8 dark:bg-slate-950 md:px-8">
      <div className="mx-auto max-w-6xl space-y-6">
        <div className="rounded-[2rem] border border-slate-200 bg-white/95 p-6 shadow-2xl backdrop-blur dark:border-slate-800 dark:bg-slate-900/95 md:p-8">
          <div className="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
            <div>
              <div className="text-[11px] font-black uppercase tracking-[0.28em] text-sky-600">Column Mapping</div>
              <h1 className="mt-3 text-3xl font-black tracking-tight text-slate-900 dark:text-white">Match your file columns before analysis</h1>
              <p className="mt-3 max-w-3xl text-sm font-medium leading-6 text-slate-500 dark:text-slate-300">
                Choose which uploaded columns should drive each analytics parameter. This replaces the old hard-coded auto-detection step.
              </p>
            </div>
            <div className="rounded-[1.5rem] border border-slate-200 bg-slate-50 px-5 py-4 text-sm font-semibold text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200">
              <div>{projectName}</div>
              <div className="mt-1 text-xs font-bold uppercase tracking-[0.18em] text-slate-400">
                {importMode === 'batch' ? `${previewItems.length} files queued` : activePreview?.file_name}
              </div>
            </div>
          </div>

          {schemaMismatch && (
            <div className="mt-6 rounded-[1.5rem] border border-amber-200 bg-amber-50 px-5 py-4 text-sm font-semibold text-amber-700 dark:border-amber-900/40 dark:bg-amber-950/20 dark:text-amber-300">
              Your batch files do not all share the exact same column list. The mapping below uses the first file, so keep the batch schema consistent before importing.
            </div>
          )}

          {error && (
            <div className="mt-6 rounded-[1.5rem] border border-rose-200 bg-rose-50 px-5 py-4 text-sm font-semibold text-rose-700 dark:border-rose-900/40 dark:bg-rose-950/20 dark:text-rose-300">
              {error}
            </div>
          )}
        </div>

        <div className="space-y-6">
          <div className="rounded-[2rem] border border-slate-200 bg-white p-6 shadow-xl dark:border-slate-800 dark:bg-slate-900 md:p-8">
            <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
              <div>
                <div className="text-[11px] font-black uppercase tracking-[0.24em] text-slate-400">Detected Columns</div>
                <h2 className="mt-2 text-2xl font-black tracking-tight text-slate-900 dark:text-white">Columns found in the uploaded file</h2>
              </div>
              <div className="rounded-full bg-slate-100 px-3 py-1 text-xs font-black uppercase tracking-[0.18em] text-slate-500 dark:bg-slate-800 dark:text-slate-300">
                {activePreview?.row_count || 0} rows detected
              </div>
            </div>

            <div className="mt-5 flex flex-wrap gap-3">
              {availableColumns.map((columnName) => (
                <span key={columnName} className="rounded-full border border-slate-200 bg-slate-50 px-4 py-2 text-sm font-bold text-slate-700 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200">
                  {columnName}
                </span>
              ))}
            </div>
          </div>

          <div className="rounded-[2rem] border border-slate-200 bg-white p-6 shadow-xl dark:border-slate-800 dark:bg-slate-900 md:p-8">
            <div className="mb-6">
              <div className="text-[11px] font-black uppercase tracking-[0.24em] text-slate-400">Parameters</div>
              <h2 className="mt-2 text-2xl font-black tracking-tight text-slate-900 dark:text-white">Select the source column for each metric</h2>
            </div>

            <div className="grid gap-4 lg:grid-cols-2">
              {IMPORT_MAPPING_FIELDS.map((field) => (
                <label key={field.key} className="block rounded-[1.5rem] border border-slate-200 bg-slate-50 p-4 dark:border-slate-700 dark:bg-slate-800/70">
                  <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                    <div>
                      <div className="text-xs font-black uppercase tracking-[0.2em] text-slate-400">{field.label}</div>
                      <div className="mt-2 text-sm font-semibold text-slate-500 dark:text-slate-300">{field.helper}</div>
                    </div>
                    {field.required && <span className="w-fit rounded-full bg-sky-100 px-2 py-1 text-[10px] font-black uppercase tracking-[0.18em] text-sky-700 dark:bg-sky-950/40 dark:text-sky-300">Required</span>}
                  </div>
                  <select
                    value={mapping[field.key] || ''}
                    onChange={(event) => setMapping((current) => ({ ...current, [field.key]: event.target.value }))}
                    className="mt-4 h-12 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm font-semibold text-slate-800 outline-none transition-all focus:border-sky-400 dark:border-slate-700 dark:bg-slate-900 dark:text-white"
                  >
                    <option value="">{field.required ? `Select ${field.key}` : `Optional ${field.key}`}</option>
                    {!field.required && <option value="__skip__">Not available</option>}
                    {availableColumns.map((columnName) => (
                      <option key={`${field.key}-${columnName}`} value={columnName}>{columnName}</option>
                    ))}
                  </select>
                </label>
              ))}
            </div>

            <div className="mt-6 rounded-[1.5rem] border border-slate-200 bg-slate-50 px-5 py-4 text-sm font-medium text-slate-500 dark:border-slate-700 dark:bg-slate-800/60 dark:text-slate-300">
              {importMode === 'batch'
                ? 'This mapping will be applied to every file in the batch.'
                : `Reporting period: ${activePreview?.month || '-'} ${activePreview?.year || '-'}`}
            </div>

            <div className="mt-8 flex flex-col-reverse gap-3 sm:flex-row sm:justify-end">
              <button
                type="button"
                onClick={() => navigate(-1)}
                className="rounded-2xl border border-slate-200 px-5 py-3 font-bold text-slate-700 transition-all hover:bg-slate-50 dark:border-slate-700 dark:text-slate-200 dark:hover:bg-slate-800"
              >
                Back
              </button>
              <button
                type="button"
                onClick={handleSubmit}
                disabled={isSubmitting || !activePreview}
                className="rounded-2xl bg-sky-600 px-5 py-3 font-bold text-white shadow-lg shadow-sky-600/20 transition-all hover:-translate-y-0.5 hover:bg-sky-500 disabled:cursor-not-allowed disabled:opacity-60 disabled:hover:translate-y-0"
              >
                {isSubmitting ? 'Importing...' : 'Import With Selected Columns'}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default UploadColumnMappingPage;
