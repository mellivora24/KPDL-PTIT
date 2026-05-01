import React from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid, ResponsiveContainer, Cell } from 'recharts'

const palette = ['#4f46e5', '#0f766e', '#2563eb', '#7c3aed', '#d97706', '#dc2626']

export default function OlapChart({ data = { items: [] }, onBarClick = () => {}, unit = '', measureCaption = '', measureDescription = '' }) {
  const chartData = (data.items || []).map((item, index) => ({
    ...item,
    index,
  }))

  const formatValue = (value) => Number(value || 0).toLocaleString('vi-VN')

  return (
    <div className="chart-wrap">
      <ResponsiveContainer width="100%" height={360}>
        <BarChart data={chartData} margin={{ top: 20, right: 16, left: 0, bottom: 8 }}>
          <CartesianGrid stroke="rgba(148,163,184,0.18)" vertical={false} />
          <XAxis dataKey="memberCaption" tick={{ fill: '#94a3b8', fontSize: 12 }} axisLine={false} tickLine={false} />
          <YAxis tickFormatter={formatValue} tick={{ fill: '#94a3b8', fontSize: 12 }} axisLine={false} tickLine={false} />
          <Tooltip
            cursor={{ fill: 'rgba(15,23,42,0.08)' }}
            contentStyle={{
              background: 'rgba(15, 23, 42, 0.96)',
              border: '1px solid rgba(148,163,184,0.2)',
              borderRadius: '16px',
              color: '#e2e8f0',
            }}
            formatter={(value) => [`${formatValue(value)} ${unit}`, 'Giá trị']}
            labelFormatter={(label) => `Phần tử: ${label}`}
          >
            {/* Custom tooltip content to include measure info */}
          </Tooltip>
          <Bar
            dataKey="value"
            radius={[12, 12, 0, 0]}
            onClick={(entry) => onBarClick(entry?.payload || entry)}
          >
            {chartData.map((entry, index) => (
              <Cell key={entry.memberUniqueName || index} fill={palette[index % palette.length]} cursor="pointer" />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <div className="chart-footer">Nhấp vào một cột để xem chi tiết cấp tiếp theo.</div>
    </div>
  )
}
