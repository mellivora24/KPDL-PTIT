import React from 'react'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid,
  ResponsiveContainer, Cell, ReferenceLine,
} from 'recharts'

// Monochrome palette: shades of white/grey only
const palette = [
  '#ffffff', '#d0d0d0', '#a8a8a8', '#888888',
  '#c8c8c8', '#e8e8e8', '#b0b0b0', '#989898',
]

function CustomTooltip({ active, payload, label, unit }) {
  if (!active || !payload?.length) return null
  const value = payload[0]?.value ?? 0
  return (
    <div style={{
      background: '#111111',
      border: '1px solid rgba(255,255,255,0.14)',
      borderRadius: 12,
      padding: '12px 18px',
      boxShadow: '0 8px 32px rgba(0,0,0,0.8)',
      minWidth: 190,
    }}>
      <div style={{
        fontSize: '0.68rem',
        color: '#707070',
        marginBottom: 6,
        textTransform: 'uppercase',
        letterSpacing: '0.14em',
        fontFamily: "'Be Vietnam Pro', sans-serif",
      }}>
        {label}
      </div>
      <div style={{
        fontSize: '1.3rem',
        fontWeight: 700,
        color: '#ffffff',
        fontFamily: "'Be Vietnam Pro', sans-serif",
      }}>
        {Number(value).toLocaleString('vi-VN')}
        {unit && (
          <span style={{ fontSize: '0.82rem', color: '#707070', marginLeft: 6, fontWeight: 400 }}>
            {unit}
          </span>
        )}
      </div>
      <div style={{
        fontSize: '0.73rem',
        color: '#555',
        marginTop: 8,
        paddingTop: 8,
        borderTop: '1px solid rgba(255,255,255,0.06)',
        fontFamily: "'Be Vietnam Pro', sans-serif",
      }}>
        Nhấn để xem chi tiết →
      </div>
    </div>
  )
}

function CustomXAxisTick({ x, y, payload }) {
  const label = String(payload?.value || '')
  const maxLen = 11
  const display = label.length > maxLen ? label.slice(0, maxLen) + '…' : label
  return (
    <g transform={`translate(${x},${y})`}>
      <text
        x={0} y={0} dy={14}
        textAnchor="middle"
        fill="#606060"
        fontSize={11}
        fontFamily="'Be Vietnam Pro', sans-serif"
      >
        {display}
      </text>
    </g>
  )
}

export default function OlapChart({
  data = { items: [] },
  onBarClick = () => {},
  unit = '',
  measureCaption = '',
}) {
  const chartData = (data.items || []).map((item, i) => ({ ...item, index: i }))
  const average = data.average || 0

  const formatYAxis = (value) => {
    if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}T`
    if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}Tr`
    if (value >= 1_000) return `${(value / 1_000).toFixed(0)}K`
    return Number(value).toLocaleString('vi-VN')
  }

  if (chartData.length === 0) {
    return (
      <div style={{
        height: 340,
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        gap: 10,
        color: '#555',
        fontSize: '0.88rem',
        fontFamily: "'Be Vietnam Pro', sans-serif",
      }}>
        <span style={{ fontSize: '2rem', opacity: 0.4 }}>📭</span>
        <span>Không có dữ liệu để hiển thị</span>
      </div>
    )
  }

  return (
    <div className="chart-wrap">
      <ResponsiveContainer width="100%" height={340}>
        <BarChart
          data={chartData}
          margin={{ top: 12, right: 12, left: 4, bottom: 24 }}
          barCategoryGap="30%"
        >
          <CartesianGrid
            stroke="rgba(255,255,255,0.04)"
            vertical={false}
            strokeDasharray="4 4"
          />
          <XAxis
            dataKey="memberCaption"
            tick={<CustomXAxisTick />}
            axisLine={false}
            tickLine={false}
            interval={0}
          />
          <YAxis
            tickFormatter={formatYAxis}
            tick={{ fill: '#555', fontSize: 11, fontFamily: "'Be Vietnam Pro', sans-serif" }}
            axisLine={false}
            tickLine={false}
            width={52}
          />
          {average > 0 && (
            <ReferenceLine
              y={average}
              stroke="rgba(255,255,255,0.2)"
              strokeDasharray="6 4"
              label={{
                value: `TB: ${formatYAxis(average)}`,
                position: 'insideTopRight',
                fill: '#666',
                fontSize: 11,
                fontFamily: "'Be Vietnam Pro', sans-serif",
              }}
            />
          )}
          <Tooltip
            content={<CustomTooltip unit={unit} />}
            cursor={{ fill: 'rgba(255,255,255,0.03)' }}
          />
          <Bar
            dataKey="value"
            radius={[6, 6, 0, 0]}
            onClick={(entry) => onBarClick(entry?.payload || entry)}
            maxBarSize={64}
          >
            {chartData.map((entry, i) => (
              <Cell
                key={entry.memberUniqueName || i}
                fill={palette[i % palette.length]}
                cursor="pointer"
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>

      <div className="chart-footer">
        {measureCaption && (
          <span style={{ marginRight: 14 }}>
            Chỉ số: <strong style={{ color: '#c0c0c0' }}>{measureCaption}</strong>
          </span>
        )}
        <span>{chartData.length} mục · Nhấn vào cột để xem chi tiết tiếp theo</span>
      </div>
    </div>
  )
}