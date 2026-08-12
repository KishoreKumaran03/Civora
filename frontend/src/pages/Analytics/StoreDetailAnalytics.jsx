import { useState, useEffect, useRef } from 'react';
import { useParams } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';
import { apiRequest } from '../../services/api';
import { COLORS } from '../../constants/colors';
import { DownloadButton } from '../../components/common/DownloadButton';
import {
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
  BarChart,
  Bar,
  XAxis,
} from 'recharts';

export function StoreDetailAnalytics() {
  const regionChartRef = useRef(null);
  const barChartRef1 = useRef(null);
  const { projectId } = useParams();
  const [data, setData] = useState([]);
  const { token } = useAuth();

  useEffect(() => {
    apiRequest({
      method: 'get',
      url: `/api/dashboard/${projectId}`,
      headers: { Authorization: `Bearer ${token}` }
    })
      .then(res => setData(res.data))
      .catch(err => console.error('Error fetching store data:', err));
  }, [projectId, token]);

  const latest = data[data.length - 1] || {};
  const regionData = latest.region_data ? Object.entries(latest.region_data).map(([name, value]) => ({ name, value })) : [];

  const handleDeleteData = async () => {
    if (confirm("Discard all store data?")) {
      try {
        await apiRequest({
          method: 'delete',
          url: `/api/dashboard/${projectId}`,
          headers: { Authorization: `Bearer ${token}` }
        });
        window.location.reload();
      } catch (err) {
        alert("Failed to delete data: " + (err.response?.data?.error || err.message));
      }
    }
  };

  return (
    <div className="p-10 space-y-10 max-w-7xl mx-auto">
      <div className="flex justify-between items-center border-b pb-8 border-slate-100 dark:border-slate-800">
        <div>
          <h1 className="text-4xl font-black tracking-tighter uppercase">Store Intelligence</h1>
          <div className="flex items-center gap-2 mt-2">
            <div className="w-2 h-2 rounded-full bg-emerald-500"></div>
            <span className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Node SID-00{projectId} | Connected</span>
          </div>
        </div>
        <button onClick={handleDeleteData} className="px-6 py-3 border border-rose-100 text-rose-500 font-black text-xs rounded-2xl hover:bg-rose-50 transition-all uppercase tracking-widest">Discard Hub Records</button>
      </div>
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-10">
        <ChartCard title="Regional Velocity" icon="public" color="blue" downloadRef={regionChartRef} downloadFilename="region-velocity-chart">
          <div className="relative">
            <div ref={regionChartRef} className="h-80">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie data={regionData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={120} stroke="none">
                    {regionData.map((e, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                  </Pie>
                  <Tooltip />
                  <Legend />
                </PieChart>
              </ResponsiveContainer>
            </div>
          </div>
        </ChartCard>
        <ChartCard title="Revenue Growth" icon="trending_up" color="emerald" downloadRef={barChartRef1} downloadFilename="revenue-growth-chart">
          <div className="relative h-80">
            <div ref={barChartRef1} className="w-full h-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={data}>
                  <XAxis dataKey="month_name" axisLine={false} tickLine={false} tick={{ fontSize: 10, fontWeight: 900 }} />
                  <Tooltip contentStyle={{ borderRadius: '20px', border: 'none', background: '#0f172a', color: '#fff' }} />
                  <Bar dataKey="total_revenue" fill="#10b981" radius={[10, 10, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        </ChartCard>
      </div>
    </div>
  );
}

function ChartCard({ title, icon, children, downloadRef, downloadFilename }) {
  const cardRef = useRef(null);

  return (
    <div ref={cardRef} className="bg-white dark:bg-slate-900 p-10 rounded-[3rem] shadow-xl border border-slate-100 dark:border-slate-800 transition-all hover:shadow-2xl hover:shadow-slate-200/50 dark:hover:shadow-none relative group">
      {downloadRef && <DownloadButton chartRef={cardRef} filename={downloadFilename || title.toLowerCase().replace(/\s+/g, '-')} />}
      <div className="flex items-center justify-between mb-10">
        <h3 className="font-black text-[10px] opacity-40 uppercase tracking-[4px] flex items-center gap-3">
          <span className="material-symbols-outlined text-primary group-hover:rotate-12 transition-transform">{icon}</span> {title}
        </h3>
        <span className="material-symbols-outlined text-slate-200">more_horiz</span>
      </div>
      {children}
    </div>
  );
}

export default StoreDetailAnalytics;
