import { useState, useEffect, useRef } from 'react';
import { useAuth } from '../../context/AuthContext';
import { getDashboardSummary } from '../../services/projectService';
import { COLORS } from '../../constants/colors';
import { formatInrCompact } from '../../utils/formatters';
import { DownloadButton } from '../../components/common/DownloadButton';
import { RevenueChart } from '../../components/charts/RevenueChart';
import { PieAnalytics } from '../../components/charts/PieAnalytics';
import { StateMap } from '../../components/charts/StateMap';

export function DashboardSummary() {
  const trendPanelRef = useRef(null);
  const heatmapPanelRef = useRef(null);
  const [summary, setSummary] = useState(null);
  const [viewYear, setViewYear] = useState('');
  const [availableYears, setAvailableYears] = useState([]);
  const [showExplain, setShowExplain] = useState(false);
  const [drillMonth, setDrillMonth] = useState(null);
  const [hasData, setHasData] = useState(true);
  const { token } = useAuth();

  useEffect(() => {
    const fetchData = async () => {
      try {
        const requestedYear = viewYear || '2024';
        const res = await getDashboardSummary(requestedYear, token);
        const nextSummary = res.data || {};
        const nextAvailableYears = Array.isArray(nextSummary.available_years) ? nextSummary.available_years : [];

        setSummary(nextSummary);
        setAvailableYears(nextAvailableYears);
        setHasData(nextSummary && Object.keys(nextSummary).length > 0 && nextSummary.stats && nextSummary.stats.project_count > 0);

        if (nextAvailableYears.length > 0 && !nextAvailableYears.includes(viewYear)) {
          setViewYear(nextAvailableYears[nextAvailableYears.length - 1]);
        }
      } catch (err) {
        console.error('Error fetching dashboard:', err);
        setHasData(false);
      }
    };
    fetchData();
  }, [viewYear, token]);

  const stats = summary?.stats || {};
  const rawRegionData = summary?.state_data || summary?.region_data || {};
  const rawCategoryData = summary?.category_data || {};
  const regionData = rawRegionData;
  const categoryData = rawCategoryData;
  const trendData = summary?.trend || [];

  const topStateEntry = Object.entries(regionData).sort(([, a], [, b]) => b - a)[0];
  const activeStatesCount = Object.keys(regionData).length;
  const totalQuantity = Number(stats.total_quantity || 0);
  const trendSeries = (trendData || []).map((row) => Number(row.total_revenue || 0)).filter((value) => Number.isFinite(value));
  const growthIndex = trendSeries.length > 1 && trendSeries[0] > 0
    ? ((trendSeries[trendSeries.length - 1] - trendSeries[0]) / trendSeries[0]) * 100
    : 0;
  const categoryArray = Object.entries(categoryData).map(([name, value]) => {
    const total = Object.values(categoryData).reduce((a, b) => a + b, 0);
    return {
      name,
      value: Math.round((value / total) * 100),
      amount: formatInrCompact(value).replace('Rs ', '₹'),
      color: COLORS[Object.keys(rawCategoryData).indexOf(name) % COLORS.length]
    };
  });

  const revenueDisplay = formatInrCompact(stats.total_revenue || 0).replace('Rs ', '₹');
  const forecastDisplay = formatInrCompact(Math.round(Number(stats.total_revenue || 0) * 1.1)).replace('Rs ', '₹');
  const yearOptions = availableYears;

  return (
    <div className="p-8 space-y-8 max-w-[1600px] mx-auto">
      {/* Header Info */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-black tracking-tighter">Global Dashboard</h1>
          <p className="text-slate-400 font-bold uppercase text-[10px] tracking-[2px] mt-1">
            Cross-Functional Intelligence Hub{viewYear ? ` | ${viewYear}` : ''}
          </p>
        </div>
        {yearOptions.length > 0 && (
          <div className="flex bg-slate-100 dark:bg-slate-800 p-1 rounded-xl">
            {yearOptions.map((y) => (
              <button key={y} onClick={() => setViewYear(y)} className={`px-5 py-1.5 rounded-lg text-sm font-bold transition-all ${viewYear === y ? 'bg-white dark:bg-slate-700 text-primary shadow-sm' : 'text-slate-400'}`}>{y}</button>
            ))}
          </div>
        )}
      </div>

      {/* KPI Stats */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <StatCard title="Revenue" value={revenueDisplay} change="+12.5%" icon="payments" color="indigo" forecast={`Forecast: ${forecastDisplay} next quarter`} />
        <StatCard title="Market Hubs" value={stats.project_count || 0} change="+3" icon="public" color="emerald" forecast="Expansion: 2 new nodes pending" />
        <StatCard title="Global Usage" value={totalQuantity.toLocaleString()} change={`${activeStatesCount} states`} icon="person_add" color="blue" forecast="total units across uploaded rows" />
        <StatCard title="Growth Index" value={`${growthIndex >= 0 ? '+' : ''}${growthIndex.toFixed(1)}%`} change={`${trendSeries.length} months`} icon="trending_up" color="amber" forecast="revenue trend movement in selected year" />
      </div>

      {/* Main Charts Section */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-8">
        {/* Trend Chart Card */}
        <div ref={trendPanelRef} className="bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 p-8 rounded-[2.5rem] shadow-xl shadow-slate-200/50 dark:shadow-none flex flex-col h-[500px] relative">
          <DownloadButton chartRef={trendPanelRef} filename="dashboard-trend-panel" />
          <div className="mb-8 flex items-start justify-between gap-4 pr-20">
            <div>
              <h4 className="text-2xl font-black text-slate-900 dark:text-white tracking-tighter">Revenue & Profit Trend</h4>
              <div className="flex items-center gap-2 mt-1">
                <span className="w-2 h-2 rounded-full bg-primary animate-pulse"></span>
                <span className="text-[10px] text-slate-400 font-black uppercase tracking-widest">Real-time analytical sync</span>
              </div>
            </div>
            <div className="flex flex-wrap gap-2">
              <button onClick={() => setShowExplain(true)} className="px-4 py-2 bg-indigo-50 dark:bg-indigo-900/20 text-indigo-600 text-[10px] font-black rounded-xl border border-indigo-100 dark:border-indigo-800 hover:scale-105 active:scale-95 transition-all">EXPLAIN AI</button>
              <button className="px-4 py-2 bg-slate-50 dark:bg-slate-800 text-slate-500 text-[10px] font-black rounded-xl border border-slate-100 dark:border-slate-700">COMPARE</button>
            </div>
          </div>

          <div className="relative flex-1 min-h-0">
            <RevenueChart data={trendData} onClick={(e) => e && setDrillMonth(e.activeLabel)} height="100%" />
          </div>

          <div className="flex justify-center gap-10 mt-8 pt-6 border-t border-slate-50 dark:border-slate-800">
            <div className="flex items-center gap-3">
              <div className="w-8 h-2 bg-primary rounded-full"></div>
              <span className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Revenue</span>
            </div>
            <div className="flex items-center gap-3">
              <div className="w-8 h-2 bg-orange-500 rounded-full"></div>
              <span className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Profit</span>
            </div>
          </div>
        </div>

        {/* India Map Card */}
        <div ref={heatmapPanelRef} className="bg-white dark:bg-slate-900 p-8 rounded-[2.5rem] shadow-xl shadow-slate-200/50 dark:shadow-none border border-slate-100 dark:border-slate-800 flex flex-col h-[500px] relative overflow-hidden group">
          <DownloadButton chartRef={heatmapPanelRef} filename="dashboard-heatmap-panel" />
          <div className="relative z-10 flex w-full items-start justify-between gap-4 pr-20">
            <div>
              <h2 className="text-2xl font-black tracking-tighter">India Market Hubs</h2>
              <p className="text-xs text-slate-400 font-bold mt-1 uppercase">Strategic Regional distribution</p>
            </div>
            {hasData ? (
              <div className="flex items-center gap-2 bg-rose-50 text-rose-500 px-3 py-1 rounded-full border border-rose-100">
                <div className="w-2 h-2 bg-rose-500 rounded-full animate-ping"></div>
                <span className="text-[10px] font-black uppercase">Active Nodes</span>
              </div>
            ) : (
              <div className="flex items-center gap-2 bg-slate-50 text-slate-500 px-3 py-1 rounded-full border border-slate-100">
                <span className="text-[10px] font-black uppercase">No Data</span>
              </div>
            )}
          </div>

          <div className="relative flex-1 -mt-4">
            <StateMap stateData={regionData} hasData={hasData} showHeatScale={false} />
          </div>

          <div className="grid grid-cols-2 gap-4 mt-auto relative z-10">
            <div className="bg-slate-50 dark:bg-slate-800/50 p-4 rounded-2xl flex items-center gap-4 border border-slate-100 dark:border-slate-800 group/item hover:bg-white dark:hover:bg-slate-800 transition-all cursor-default">
              <div className="w-12 h-12 bg-white dark:bg-slate-900 rounded-2xl flex items-center justify-center text-primary shadow-sm group-hover/item:rotate-12 transition-all"><span className="material-symbols-outlined">location_on</span></div>
              <div><div className="text-[10px] font-black text-slate-400 uppercase tracking-[2px]">Top State</div><div className="text-sm font-black text-slate-900 dark:text-white">{hasData && topStateEntry ? topStateEntry[0] : 'No Data'}</div></div>
            </div>
            <div className="bg-slate-50 dark:bg-slate-800/50 p-4 rounded-2xl flex items-center gap-4 border border-slate-100 dark:border-slate-800 group/item hover:bg-white dark:hover:bg-slate-800 transition-all cursor-default">
              <div className="w-12 h-12 bg-white dark:bg-slate-900 rounded-2xl flex items-center justify-center text-rose-500 shadow-sm group-hover/item:rotate-12 transition-all"><span className="material-symbols-outlined">local_fire_department</span></div>
              <div><div className="text-[10px] font-black text-slate-400 uppercase tracking-[2px]">Active States</div><div className="text-sm font-black text-slate-900 dark:text-white">{hasData ? activeStatesCount : 'No Data'}</div></div>
            </div>
          </div>
        </div>
      </div>

      {/* Categories & Performance Area */}
      {hasData ? (
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-8 pb-12">
          <DonutChartCard data={categoryArray} />
          <div className="bg-white dark:bg-slate-900 p-8 rounded-[2.5rem] shadow-xl border border-slate-100 dark:border-slate-800 flex flex-col h-full ring-1 ring-black/5">
            <div className="flex justify-between items-start mb-10">
              <div>
                <h2 className="text-2xl font-black tracking-tighter uppercase">Sales by Category</h2>
                <p className="text-xs text-slate-400 font-bold mt-1">Imported category totals from your latest data</p>
              </div>
              <div className="text-primary text-[10px] font-black uppercase tracking-widest px-4 py-2 bg-primary/5 rounded-xl">{Object.keys(categoryData).length} categories</div>
            </div>
            <div className="space-y-4">
              {Object.entries(categoryData)
                .sort(([, leftValue], [, rightValue]) => rightValue - leftValue)
                .map(([name, value], index, entries) => {
                  const totalCategoryRevenue = entries.reduce((sum, [, categoryValue]) => sum + Number(categoryValue || 0), 0);
                  const progress = totalCategoryRevenue > 0 ? Math.round((Number(value) / totalCategoryRevenue) * 100) : 0;
                  const colorOptions = ['bg-indigo-600', 'bg-emerald-500', 'bg-amber-500', 'bg-rose-500', 'bg-sky-500', 'bg-fuchsia-500'];
                  const iconOptions = ['devices', 'weekend', 'category', 'shopping_basket', 'star', 'work'];

                  return (
                    <PerformanceBar
                      key={name}
                      label={name}
                      value={formatInrCompact(value)}
                      progress={progress}
                      icon={iconOptions[index % iconOptions.length]}
                      growth={`${progress}% of sales`}
                      trendTone="neutral"
                      color={colorOptions[index % colorOptions.length]}
                      badge={index === 0 ? 'TOP' : undefined}
                    />
                  );
                })}
            </div>
          </div>
        </div>
      ) : (
        <div className="pb-12 bg-white dark:bg-slate-900 rounded-[2.5rem] border border-slate-100 dark:border-slate-800 p-12 text-center">
          <div className="text-5xl mb-4">📊</div>
          <h3 className="text-xl font-black text-slate-900 dark:text-white mb-2">No Data Available</h3>
          <p className="text-slate-500 dark:text-slate-400">Import data to see market distribution and performance metrics</p>
        </div>
      )}

      {/* Modal Overlays */}
      {showExplain && <ExplainModal onClose={() => setShowExplain(false)} />}
      {drillMonth && <DrilldownModal month={drillMonth} onClose={() => setDrillMonth(null)} />}
    </div>
  );
}

// Local helper widgets for Dashboard
function StatCard({ title, value, change, icon, color, forecast }) {
  const colorClasses = {
    indigo: 'bg-indigo-500 text-white shadow-indigo-500/20',
    emerald: 'bg-emerald-500 text-white shadow-emerald-500/20',
    blue: 'bg-blue-500 text-white shadow-blue-500/20',
    amber: 'bg-amber-500 text-white shadow-amber-500/20'
  };
  const isPositive = change.startsWith('+');
  const isNegative = change.startsWith('-');
  const trendClasses = isPositive ? 'text-emerald-500' : isNegative ? 'text-rose-500' : 'text-slate-400';
  const trendIcon = isPositive ? 'trending_up' : isNegative ? 'trending_down' : 'remove';

  return (
    <div className="bg-white dark:bg-slate-900 p-8 rounded-[2rem] shadow-lg border border-slate-100 dark:border-slate-800 transition-all hover:-translate-y-2 group">
      <div className="flex justify-between items-start mb-6">
        <div className={`p-3 rounded-2xl ${colorClasses[color]} group-hover:scale-110 transition-transform`}><span className="material-symbols-outlined text-2xl leading-none">{icon}</span></div>
        <div className={`flex items-center gap-1 ${trendClasses} font-black text-xs`}>
          <span className="material-symbols-outlined text-sm">{trendIcon}</span> {change}
        </div>
      </div>
      <div className="text-xs font-black text-slate-400 uppercase tracking-widest mb-1">{title}</div>
      <div className="text-4xl font-black tracking-tighter mb-4 text-slate-900 dark:text-white uppercase">{value}</div>
      <div className="pt-4 border-t border-slate-50 dark:border-slate-800 flex items-center gap-2">
        <span className="w-1.5 h-1.5 bg-slate-300 rounded-full"></span>
        <p className="text-[10px] text-slate-400 font-bold italic truncate uppercase">{forecast}</p>
      </div>
    </div>
  );
}

function DonutChartCard({ data }) {
  const productPanelRef = useRef(null);
  const [hoveredData, setHoveredData] = useState(null);

  const chartData = data && data.length > 0 ? data : [
    { name: 'No Data', value: 100, color: '#e2e8f0', amount: '₹0L' }
  ];

  return (
    <div ref={productPanelRef} className="bg-white dark:bg-slate-900 p-8 rounded-[2.5rem] shadow-xl border border-slate-100 dark:border-slate-800 flex flex-col items-center group relative w-full">
      <DownloadButton chartRef={productPanelRef} filename="dashboard-category-split-panel" />
      <div className="w-full flex justify-between items-start mb-4">
        <div><h2 className="text-2xl font-black tracking-tighter uppercase">Category Split</h2><p className="text-xs text-slate-400 font-bold uppercase mt-1">Market distribution analysis</p></div>
        <span className="material-symbols-outlined text-slate-300">info</span>
      </div>
      <div className="relative w-64 h-64 my-6">
        <div className="w-full h-full">
          <PieAnalytics data={chartData} innerRadius={80} outerRadius={110} onMouseEnter={(e) => setHoveredData(e)} onMouseLeave={() => setHoveredData(null)} />
        </div>
        <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none transition-all duration-300">
          <div className="text-4xl font-black text-slate-900 dark:text-white tracking-tighter">{hoveredData ? hoveredData.amount : '100%'}</div>
          <div className="text-[10px] font-black text-slate-400 uppercase tracking-widest mt-1">{hoveredData ? hoveredData.name : 'Total Impact'}</div>
        </div>
      </div>
      <div className="flex justify-between w-full px-4 border-t border-slate-50 dark:border-slate-800 pt-8 mt-auto">
        {chartData.map(d => (
          <div key={d.name} className="flex flex-col items-center group/leg cursor-pointer transition-all" onMouseEnter={() => setHoveredData(d)} onMouseLeave={() => setHoveredData(null)}>
            <div className="flex items-center gap-2 mb-1">
              <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: d.color }}></div>
              <span className="text-[10px] font-black uppercase text-slate-400 group-hover/leg:text-slate-900 dark:group-hover/leg:text-white transition-colors">{d.name}</span>
            </div>
            <span className="text-sm font-black text-slate-900 dark:text-white">{d.value}%</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function PerformanceBar({ label, value, progress, icon, growth, color, badge, trendTone }) {
  const resolvedTrendTone = trendTone || (growth.startsWith('+') ? 'positive' : growth.startsWith('-') ? 'negative' : 'neutral');
  return (
    <div className="p-5 rounded-[1.5rem] border border-slate-50 dark:border-slate-800 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-all group/bar">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-4">
          <div className="w-14 h-14 bg-slate-50 dark:bg-slate-800 rounded-2xl flex items-center justify-center text-slate-400 group-hover/bar:bg-white dark:group-hover/bar:bg-slate-900 group-hover/bar:text-primary transition-all shadow-sm"><span className="material-symbols-outlined text-3xl">{icon}</span></div>
          <div>
            <div className="text-base font-black flex items-center gap-2 text-slate-900 dark:text-white">
              {label} {badge && <span className="bg-primary/10 text-primary text-[8px] px-2 py-1 rounded font-black tracking-widest leading-none">{badge}</span>}
            </div>
            <div className="text-[10px] text-slate-400 font-black uppercase tracking-widest mt-1">Cross-unit performance</div>
          </div>
        </div>
        <div className="text-right">
          <div className="text-base font-black text-slate-900 dark:text-white">{value}</div>
          <div className={`text-[10px] font-black flex items-center justify-end gap-1 ${resolvedTrendTone === 'positive' ? 'text-emerald-500' : resolvedTrendTone === 'negative' ? 'text-rose-500' : 'text-slate-400 dark:text-slate-300'}`}>
            {resolvedTrendTone === 'positive' ? <span className="material-symbols-outlined text-[10px]">north</span> : resolvedTrendTone === 'negative' ? <span className="material-symbols-outlined text-[10px]">south</span> : <span className="material-symbols-outlined text-[10px]">donut_small</span>}
            {growth}
          </div>
        </div>
      </div>
      <div className="h-2 w-full bg-slate-100 dark:bg-slate-800 rounded-full overflow-hidden">
        <div className={`h-full ${color} rounded-full transition-all duration-1000 group-hover/bar:scale-x-105 origin-left shadow-lg`} style={{ width: `${progress}%` }}></div>
      </div>
    </div>
  );
}

function ExplainModal({ onClose }) {
  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center p-6 animate-in fade-in duration-300">
      <div className="absolute inset-0 bg-slate-900/60 backdrop-blur-md" onClick={onClose}></div>
      <div className="relative w-full max-w-xl bg-white dark:bg-slate-900 rounded-[3rem] shadow-2xl p-10 border border-slate-200 dark:border-slate-800 group">
        <button onClick={onClose} className="absolute top-8 right-8 text-slate-400 hover:text-slate-900 dark:hover:text-white transition-all"><span className="material-symbols-outlined">close</span></button>
        <div className="flex items-center gap-4 mb-8">
          <div className="w-14 h-14 bg-indigo-50 dark:bg-indigo-900/40 rounded-2xl flex items-center justify-center text-indigo-600 animate-bounce"><span className="material-symbols-outlined text-3xl">auto_awesome</span></div>
          <div>
            <h3 className="text-2xl font-black tracking-tighter">AI Explainability</h3>
            <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Powered by Yua AI Intelligence</p>
          </div>
        </div>
        <div className="space-y-6 text-slate-600 dark:text-slate-400 font-medium leading-relaxed">
          <div className="bg-slate-50 dark:bg-slate-800/50 p-6 rounded-3xl border-l-4 border-indigo-500">
            <p className="text-sm font-bold text-slate-900 dark:text-white mb-2">Trend Analysis Summary</p>
            <p className="text-sm">Revenue peaked in June (₹6L) due to Q2 fiscal closures. Festive spending uplift of 18% noted in Q4. April dip is a recurring seasonal procurement lull.</p>
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div className="p-4 bg-emerald-50 dark:bg-emerald-950/20 rounded-2xl border border-emerald-100 dark:border-emerald-800">
              <div className="text-[10px] font-black text-emerald-600 uppercase mb-1">Growth Driver</div>
              <p className="text-sm font-bold text-slate-900 dark:text-white">Cloud Systems (+42%)</p>
            </div>
            <div className="p-4 bg-rose-50 dark:bg-rose-950/20 rounded-2xl border border-rose-100 dark:border-rose-800">
              <div className="text-[10px] font-black text-rose-600 uppercase mb-1">Alert Factor</div>
              <p className="text-sm font-bold text-slate-900 dark:text-white">Retail Attrition (-5%)</p>
            </div>
          </div>
        </div>
        <button onClick={onClose} className="w-full mt-10 bg-indigo-600 text-white font-black py-4 rounded-2xl shadow-xl shadow-indigo-600/20 hover:scale-[1.02] active:scale-95 transition-all">GENERATE FULL REPORT</button>
      </div>
    </div>
  );
}

function DrilldownModal({ month, onClose }) {
  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center p-6 animate-in fade-in duration-300">
      <div className="absolute inset-0 bg-slate-900/80 backdrop-blur-sm" onClick={onClose}></div>
      <div className="relative w-full max-w-lg bg-card-light dark:bg-slate-800 rounded-[2.5rem] shadow-2xl p-8 border border-slate-200 dark:border-slate-700">
        <div className="flex justify-between items-center mb-8">
          <h3 className="text-2xl font-black tracking-tighter uppercase">{month} Breakdown</h3>
          <button onClick={onClose} className="text-slate-400 hover:text-white"><span className="material-symbols-outlined">close</span></button>
        </div>
        <div className="space-y-4">
          <div className="p-4 bg-slate-100 dark:bg-slate-700/50 rounded-2xl flex justify-between items-center">
            <span className="text-sm font-bold text-slate-500">Peak Performance Day</span>
            <span className="text-sm font-black">21st (₹45k)</span>
          </div>
          <div className="p-4 bg-slate-100 dark:bg-slate-700/50 rounded-2xl flex justify-between items-center">
            <span className="text-sm font-bold text-slate-500">Active Clients</span>
            <span className="text-sm font-black">42 Entities</span>
          </div>
          <div className="p-4 bg-indigo-50 dark:bg-indigo-900/30 rounded-2xl border border-indigo-100 dark:border-indigo-800">
            <div className="text-xs font-black text-indigo-600 uppercase mb-2">Category Split</div>
            <div className="space-y-1">
              <div className="flex justify-between text-xs font-bold"><span>Electronics</span><span>55%</span></div>
              <div className="w-full h-1 bg-indigo-200 dark:bg-indigo-800 rounded-full overflow-hidden"><div className="h-full bg-indigo-600 w-[55%]"></div></div>
            </div>
          </div>
        </div>
        <button onClick={onClose} className="w-full mt-8 py-3 bg-slate-900 text-white rounded-xl font-black">CLOSE DATA VIEW</button>
      </div>
    </div>
  );
}
