import { ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';

export function PieAnalytics({
  data,
  innerRadius = 80,
  outerRadius = 110,
  onMouseEnter,
  onMouseLeave
}) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <PieChart>
        <Pie
          data={data}
          dataKey="value"
          nameKey="name"
          cx="50%"
          cy="50%"
          innerRadius={innerRadius}
          outerRadius={outerRadius}
          paddingAngle={4}
          onMouseEnter={onMouseEnter}
          onMouseLeave={onMouseLeave}
        >
          {data.map((entry, index) => (
            <Cell
              key={index}
              fill={entry.color || '#e2e8f0'}
              stroke="none"
              className="hover:opacity-80 transition-opacity outline-none"
            />
          ))}
        </Pie>
      </PieChart>
    </ResponsiveContainer>
  );
}
export default PieAnalytics;
