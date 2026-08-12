import { useState, useEffect, useRef } from 'react';
import { useAuth } from '../../context/AuthContext';
import { apiRequest } from '../../services/api';
import { COLORS } from '../../constants/colors';
import { MONTH_ORDER } from '../../constants/months';
import { formatInrCompact } from '../../utils/formatters';
import { downloadProjectReportPdf } from '../../utils/exportUtils';
import { DownloadButton } from '../../components/common/DownloadButton';
import {
  ResponsiveContainer,
  ComposedChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  Bar,
  Line,
  PieChart,
  Pie,
  Cell,
} from 'recharts';

function buildProjectReport(project, projectRows) {
  const rows = Array.isArray(projectRows) ? projectRows : [];
  const totals = rows.reduce((summary, row) => ({
    revenue: summary.revenue + Number(row.total_revenue || 0),
    cost: summary.cost + Number(row.total_cost || 0),
    profit: summary.profit + Number(row.net_revenue || (Number(row.total_revenue || 0) - Number(row.total_cost || 0))),
    quantity: summary.quantity + Number(row.total_quantity || 0),
  }), { revenue: 0, cost: 0, profit: 0, quantity: 0 });

  const sortedRows = [...rows].sort((leftRow, rightRow) => {
    if (leftRow.year !== rightRow.year) return Number(leftRow.year) - Number(rightRow.year);
    return MONTH_ORDER.indexOf(leftRow.month_name) - MONTH_ORDER.indexOf(rightRow.month_name);
  });

  const previousRevenue = sortedRows.length > 1 ? Number(sortedRows[sortedRows.length - 2].total_revenue || 0) : 0;
  const currentRevenue = sortedRows.length > 0 ? Number(sortedRows[sortedRows.length - 1].total_revenue || 0) : totals.revenue;
  const growthRate = previousRevenue > 0 ? ((currentRevenue - previousRevenue) / previousRevenue) * 100 : 0;
  const forecastRevenue = Math.round(currentRevenue * (1 + (growthRate / 100 || 0.08)));

  const categoryTotals = rows.reduce((accumulator, row) => {
    Object.entries(row.category_data || {}).forEach(([name, value]) => {
      accumulator[name] = (accumulator[name] || 0) + Number(value || 0);
    });
    return accumulator;
  }, {});

  const regionTotals = rows.reduce((accumulator, row) => {
    Object.entries(row.region_data || {}).forEach(([name, value]) => {
      accumulator[name] = (accumulator[name] || 0) + Number(value || 0);
    });
    return accumulator;
  }, {});

  const topCategory = Object.entries(categoryTotals).sort((leftEntry, rightEntry) => rightEntry[1] - leftEntry[1])[0];
  const topRegion = Object.entries(regionTotals).sort((leftEntry, rightEntry) => rightEntry[1] - leftEntry[1])[0];
  const margin = totals.revenue > 0 ? (totals.profit / totals.revenue) * 100 : 0;
  const trendSeries = sortedRows.map((row) => ({
    name: `${String(row.month_name || '').slice(0, 3)} ${String(row.year || '').slice(-2)}`,
    revenue: Number(row.total_revenue || 0),
    profit: Number(row.net_revenue || (Number(row.total_revenue || 0) - Number(row.total_cost || 0))),
  }));
  const categorySeries = Object.entries(categoryTotals)
    .map(([name, value], index) => ({ name, value: Number(value || 0), color: COLORS[index % COLORS.length] }))
    .sort((leftEntry, rightEntry) => rightEntry.value - leftEntry.value);

  const strengths = [
    topCategory ? `${topCategory[0]} leads category revenue at ${formatInrCompact(topCategory[1])}.` : 'Revenue mix is available for business review.',
    topRegion ? `${topRegion[0]} is the strongest operating region.` : 'Regional footprint is ready for heat-map analysis.',
  ];
  const weaknesses = [
    margin < 15 ? `Net margin is only ${margin.toFixed(1)}%, which signals cost pressure.` : `Margin is healthy, but cost discipline should still be monitored.`,
    Object.keys(categoryTotals).length <= 1 ? 'Sales are concentrated in too few categories.' : 'Category diversification can still be improved further.',
  ];
  const opportunities = [
    `At the current trajectory, the next-period revenue forecast is ${formatInrCompact(forecastRevenue)}.`,
    topRegion ? `Replicate the ${topRegion[0]} playbook across weaker locations.` : 'Use imports to unlock stronger regional forecasting.',
  ];
  const threats = [
    growthRate < 0 ? `Revenue has declined by ${Math.abs(growthRate).toFixed(1)}% versus the previous period.` : `Growth volatility should be watched as revenue moves by ${growthRate.toFixed(1)}%.`,
    'Inventory and tax settings should be reviewed before high-volume expansion.',
  ];

  return {
    project,
    rows,
    totals,
    growthRate,
    forecastRevenue,
    margin,
    topCategory,
    topRegion,
    strengths,
    weaknesses,
    opportunities,
    threats,
    highlights: [
      `Past: ${rows.length} reporting periods have been consolidated into this store report.`,
      `Present: current cumulative revenue is ${formatInrCompact(totals.revenue)} with ${formatInrCompact(totals.profit)} net contribution.`,
      `Future: forecast revenue for the upcoming cycle is ${formatInrCompact(forecastRevenue)} based on the existing trend.`,
    ],
    trendSeries,
    categorySeries,
  };
}

export function ReportsPage() {
  const monthlyTrendChartRef = useRef(null);
  const categoryChartRef = useRef(null);
  const reportTrendPanelRef = useRef(null);
  const reportCategoryPanelRef = useRef(null);
  const [reports, setReports] = useState([]);
  const [isLoadingReports, setIsLoadingReports] = useState(true);
  const { token } = useAuth();

  useEffect(() => {
    const fetchReports = async () => {
      try {
        setIsLoadingReports(true);
        const projectsResponse = await apiRequest({
          method: 'get',
          url: '/api/projects',
          headers: { Authorization: `Bearer ${token}` }
        });

        const projects = Array.isArray(projectsResponse.data) ? projectsResponse.data : [];
        const reportRows = await Promise.all(projects.map(async (project) => {
          try {
            const analyticsResponse = await apiRequest({
              method: 'get',
              url: `/api/dashboard/${project.id}`,
              headers: { Authorization: `Bearer ${token}` }
            });
            return buildProjectReport(project, analyticsResponse.data);
          } catch (error) {
            console.error(`Error fetching report data for project ${project.id}:`, error);
            return buildProjectReport(project, []);
          }
        }));

        setReports(reportRows.filter((report) => Array.isArray(report.rows) && report.rows.length > 0));
      } catch (error) {
        console.error('Error preparing reports:', error);
        setReports([]);
      } finally {
        setIsLoadingReports(false);
      }
    };

    fetchReports();
  }, [token]);

  return (
    <div className="p-10 space-y-10 max-w-7xl mx-auto">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <h1 className="text-4xl font-black tracking-tighter">Reports</h1>
          <p className="mt-2 text-slate-400 font-bold uppercase text-xs tracking-widest">
            Past Performance, Present Conditions, Future Forecast, SWOT
          </p>
        </div>
        <div className="rounded-[2rem] border border-sky-200 bg-sky-50 px-6 py-3 text-xs font-black uppercase tracking-[0.2em] text-sky-600 dark:border-sky-900/30 dark:bg-sky-950/20 dark:text-sky-300">
          {reports.length} generated store {reports.length === 1 ? 'report' : 'reports'}
        </div>
      </div>

      {isLoadingReports ? (
        <div className="rounded-[3rem] border border-slate-200 bg-white p-12 text-center shadow-sm dark:border-slate-800 dark:bg-slate-900">
          <div className="text-lg font-black text-slate-900 dark:text-white">Preparing reports...</div>
          <p className="mt-2 text-sm font-medium text-slate-500 dark:text-slate-400">
            Analyzing advanced analytics data for every project.
          </p>
        </div>
      ) : reports.length > 0 ? (
        <div className="space-y-8">
          {reports.map((report) => (
            <div key={report.project.id} className="rounded-[3rem] border border-slate-100 bg-white p-8 shadow-xl dark:border-slate-800 dark:bg-slate-900">
              <div className="flex flex-col gap-5 xl:flex-row xl:items-start xl:justify-between">
                <div>
                  <div className="text-[10px] font-black uppercase tracking-[0.3em] text-indigo-500">Store Report</div>
                  <h2 className="mt-3 text-3xl font-black tracking-tighter text-slate-900 dark:text-white">
                    {report.project.name}
                  </h2>
                  <p className="mt-2 max-w-3xl text-sm font-medium text-slate-500 dark:text-slate-400">
                    This report summarizes the advanced analytics signals for the store and translates them into executive-friendly insights.
                  </p>
                </div>
                <button
                  onClick={() => downloadProjectReportPdf(report)}
                  className="inline-flex items-center justify-center gap-2 rounded-2xl bg-slate-900 px-5 py-3 text-sm font-black text-white shadow-lg transition-all hover:-translate-y-0.5 hover:bg-slate-800 dark:bg-indigo-600 dark:hover:bg-indigo-500"
                >
                  <span className="material-symbols-outlined text-sm">picture_as_pdf</span>
                  Download PDF
                </button>
              </div>

              <div className="mt-8 grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
                <AnalyticsStat title="Revenue" value={formatInrCompact(report.totals.revenue)} sub="historical total" />
                <AnalyticsStat title="Profit" value={formatInrCompact(report.totals.profit)} sub={`${report.margin.toFixed(1)}% margin`} />
                <AnalyticsStat title="Forecast" value={formatInrCompact(report.forecastRevenue)} sub="projected next cycle" />
                <AnalyticsStat title="Periods" value={report.rows.length} sub="uploaded reporting rows" />
              </div>

              <div className="mt-8 grid grid-cols-1 gap-8 xl:grid-cols-[1fr_1fr]">
                <AnalyticsPanel title="Executive Summary" subtitle="Past, present, and future outlook">
                  <div className="space-y-4">
                    {report.highlights.map((line) => (
                      <div key={line} className="rounded-2xl border border-slate-100 bg-slate-50 px-5 py-4 text-sm font-medium text-slate-600 dark:border-slate-800 dark:bg-slate-800/60 dark:text-slate-200">
                        {line}
                      </div>
                    ))}
                  </div>
                </AnalyticsPanel>

                <AnalyticsPanel title="SWOT Analysis" subtitle="Operational strengths, risks, and opportunities">
                  <div className="space-y-4">
                    <div className="rounded-2xl border border-emerald-100 bg-emerald-50 px-5 py-4 text-sm font-medium text-emerald-700 dark:border-emerald-900/30 dark:bg-emerald-950/20 dark:text-emerald-300">
                      <span className="block text-[10px] font-black uppercase tracking-[0.2em]">Strengths</span>
                      <span className="mt-2 block">{report.strengths.join(' ')}</span>
                    </div>
                    <div className="rounded-2xl border border-rose-100 bg-rose-50 px-5 py-4 text-sm font-medium text-rose-700 dark:border-rose-900/30 dark:bg-rose-950/20 dark:text-rose-300">
                      <span className="block text-[10px] font-black uppercase tracking-[0.2em]">Weaknesses</span>
                      <span className="mt-2 block">{report.weaknesses.join(' ')}</span>
                    </div>
                    <div className="rounded-2xl border border-sky-100 bg-sky-50 px-5 py-4 text-sm font-medium text-sky-700 dark:border-sky-900/30 dark:bg-sky-950/20 dark:text-sky-300">
                      <span className="block text-[10px] font-black uppercase tracking-[0.2em]">Opportunities</span>
                      <span className="mt-2 block">{report.opportunities.join(' ')}</span>
                    </div>
                    <div className="rounded-2xl border border-amber-100 bg-amber-50 px-5 py-4 text-sm font-medium text-amber-700 dark:border-amber-900/30 dark:bg-amber-950/20 dark:text-amber-300">
                      <span className="block text-[10px] font-black uppercase tracking-[0.2em]">Threats</span>
                      <span className="mt-2 block">{report.threats.join(' ')}</span>
                    </div>
                  </div>
                </AnalyticsPanel>
              </div>

              <div className="mt-8 grid grid-cols-1 gap-8 xl:grid-cols-[1.15fr_0.85fr]">
                <AnalyticsPanel title="Revenue & Profit Trend" subtitle="Monthly movement from uploaded report rows" downloadRef={reportTrendPanelRef} downloadFilename="report-trend-panel" panelRef={reportTrendPanelRef}>
                  <div className="relative h-64">
                    <div ref={monthlyTrendChartRef} className="w-full h-full">
                      <ResponsiveContainer width="100%" height="100%">
                        <ComposedChart data={report.trendSeries}>
                          <CartesianGrid vertical={false} strokeDasharray="3 3" opacity={0.15} />
                          <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{ fontSize: 11, fontWeight: 700 }} />
                          <YAxis yAxisId="left" axisLine={false} tickLine={false} tick={{ fontSize: 11 }} />
                          <YAxis yAxisId="right" orientation="right" axisLine={false} tickLine={false} tick={{ fontSize: 11 }} />
                          <Tooltip formatter={(value, name) => [formatInrCompact(value), name]} />
                          <Legend />
                          <Bar yAxisId="left" dataKey="revenue" name="Revenue" fill="#38bdf8" radius={[10, 10, 0, 0]} />
                          <Line yAxisId="right" type="monotone" dataKey="profit" name="Profit" stroke="#f59e0b" strokeWidth={3} dot={{ r: 4 }} />
                        </ComposedChart>
                      </ResponsiveContainer>
                    </div>
                  </div>
                </AnalyticsPanel>

                <AnalyticsPanel title="Category Share" subtitle="Revenue contribution by category" downloadRef={reportCategoryPanelRef} downloadFilename="report-category-share-panel" panelRef={reportCategoryPanelRef}>
                  <div className="relative h-64">
                    <div ref={categoryChartRef} className="w-full h-full">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie data={report.categorySeries} dataKey="value" nameKey="name" innerRadius={55} outerRadius={90} paddingAngle={4}>
                            {report.categorySeries.map((entry) => <Cell key={`${report.project.id}-${entry.name}`} fill={entry.color} />)}
                          </Pie>
                          <Tooltip formatter={(value, name) => [formatInrCompact(value), name]} />
                          <Legend />
                        </PieChart>
                      </ResponsiveContainer>
                    </div>
                  </div>
                </AnalyticsPanel>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className="rounded-[3rem] border border-dashed border-slate-300 bg-white p-12 text-center shadow-sm dark:border-slate-700 dark:bg-slate-900">
          <div className="mx-auto flex h-20 w-20 items-center justify-center rounded-full bg-sky-50 text-sky-500 dark:bg-sky-950/30 dark:text-sky-300">
            <span className="material-symbols-outlined text-4xl">description</span>
          </div>
          <h2 className="mt-6 text-2xl font-black tracking-tight text-slate-900 dark:text-white">No reports available yet</h2>
          <p className="mt-3 text-sm font-medium text-slate-500 dark:text-slate-400">
            Create a project, upload store data, and the reports page will summarize the advanced analytics into downloadable PDFs.
          </p>
        </div>
      )}
    </div>
  );
}

// Layout components locally defined for Reports
function AnalyticsStat({ title, value, sub }) {
  return (
    <div className="rounded-[1.75rem] border border-slate-100 bg-slate-50 px-5 py-4 dark:border-slate-800 dark:bg-slate-800/60">
      <div className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{title}</div>
      <div className="mt-2 text-2xl font-black tracking-tighter text-slate-900 dark:text-white">{value}</div>
      <div className="mt-1 text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{sub}</div>
    </div>
  );
}

function AnalyticsPanel({ title, subtitle, children, className = '', downloadRef, downloadFilename, panelRef }) {
  const containerRef = useRef(null);

  return (
    <div
      ref={(node) => {
        containerRef.current = node;
        if (panelRef) {
          panelRef.current = node;
        }
      }}
      className={`relative rounded-[2.5rem] border border-slate-100 bg-white p-8 shadow-xl dark:border-slate-800 dark:bg-slate-900 ${className}`}
    >
      {downloadRef && <DownloadButton chartRef={containerRef} filename={downloadFilename || title.toLowerCase().replace(/\s+/g, '-')} />}
      <div className="mb-6 pr-16">
        <div className="text-[10px] font-black uppercase tracking-[0.3em] text-slate-400">{title}</div>
        <h3 className="mt-2 text-2xl font-black tracking-tighter text-slate-900 dark:text-white">{subtitle}</h3>
      </div>
      {children}
    </div>
  );
}

export default ReportsPage;
