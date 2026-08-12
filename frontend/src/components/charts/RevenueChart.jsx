import { ResponsiveContainer, ComposedChart, XAxis, YAxis, Tooltip, Bar, Line, CartesianGrid, Legend } from 'recharts';

export function RevenueChart({
  data,
  onClick,
  height = '100%',
  showGrid = false,
  barKey = 'total_revenue',
  lineKey = 'net_revenue',
  barName = 'Revenue',
  lineName = 'Profit'
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <ComposedChart data={data} onClick={onClick}>
        {showGrid && <CartesianGrid vertical={false} strokeDasharray="3 3" opacity={0.15} />}
        <defs>
          <linearGradient id="barGrad" x1="0" y1="0" x2="0" y2="1">
             <stop offset="0%" stopColor="#4f46e5" stopOpacity={1} />
             <stop offset="100%" stopColor="#818cf8" stopOpacity={0.8} />
          </linearGradient>
        </defs>
        <XAxis
          dataKey="month_name"
          axisLine={false}
          tickLine={false}
          tick={{ fontSize: 10, fontWeight: 900, fill: '#94a3b8' }}
          dy={10}
        />
        <YAxis hide domain={[0, 'auto']} />
        <Tooltip
          cursor={{ fill: 'rgba(79, 70, 229, 0.05)' }}
          contentStyle={{ borderRadius: '20px', border: 'none', background: '#0f172a', color: '#fff', padding: '15px' }}
        />
        <Bar dataKey={barKey} name={barName} fill="url(#barGrad)" radius={[10, 10, 0, 0]} barSize={35} />
        <Line
          type="monotone"
          dataKey={lineKey}
          name={lineName}
          stroke="#f97316"
          strokeWidth={4}
          dot={{ r: 6, fill: '#f97316', strokeWidth: 3, stroke: '#fff' }}
          activeDot={{ r: 8 }}
        />
      </ComposedChart>
    </ResponsiveContainer>
  );
}
export default RevenueChart;
