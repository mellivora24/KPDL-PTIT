import React, { useEffect, useState } from 'react'
import axios from 'axios'
import OlapChart from './components/OlapChart'

const api = axios.create({ baseURL: 'http://localhost:8000/api/olap' })

function normalizeResult(result) {
  const columns = result?.columns || []
  const rows = result?.rows || []
  const uniqueIndexes = columns
    .map((col, i) => (col.toUpperCase().includes('MEMBER_UNIQUE_NAME') ? i : -1))
    .filter((i) => i >= 0)
  const captionIndexes = columns
    .map((col, i) => (col.toUpperCase().includes('MEMBER_CAPTION') ? i : -1))
    .filter((i) => i >= 0)
  const uniqueIndex = uniqueIndexes.length > 0 ? uniqueIndexes[uniqueIndexes.length - 1] : 0
  const captionIndex = captionIndexes.length > 0 ? captionIndexes[captionIndexes.length - 1] : 0
  const valueIndex = columns.length - 1

  const looksLikeMdxUniqueName = (v) => {
    const t = String(v || '').trim()
    return t.startsWith('[') && t.includes('].[')
  }

  const extractUniqueName = (row) => {
    const d = row[uniqueIndex]
    if (looksLikeMdxUniqueName(d)) return d
    return row.find((c) => looksLikeMdxUniqueName(c)) || ''
  }

  const extractCaption = (row) => {
    const d = row[captionIndex]
    if (d !== undefined && d !== null && String(d).trim() !== '') return d
    return row.find((c) => c !== undefined && c !== null && String(c).trim() !== '') || ''
  }

  const items = rows.map((row) => {
    const value = Number(row[valueIndex])
    return {
      memberUniqueName: extractUniqueName(row),
      memberCaption: extractCaption(row),
      value: Number.isFinite(value) ? value : 0,
      raw: row,
    }
  })

  const total = items.reduce((s, i) => s + i.value, 0)
  const max = items.reduce((b, i) => Math.max(b, i.value), 0)
  const average = items.length > 0 ? total / items.length : 0

  return { columns, rows, items, total, max, average }
}

function fmt(value) {
  return Number(value || 0).toLocaleString('vi-VN')
}

export default function App() {
  const [cubes, setCubes] = useState([])
  const [selectedCube, setSelectedCube] = useState('')
  const [measures, setMeasures] = useState([])
  const [dimensions, setDimensions] = useState([])
  const [levels, setLevels] = useState([])
  const [selectedMeasure, setSelectedMeasure] = useState('')
  const [selectedHierarchy, setSelectedHierarchy] = useState('')
  const [selectedLevel, setSelectedLevel] = useState('')
  const [selectedUnit, setSelectedUnit] = useState('')
  const [trail, setTrail] = useState([])
  const [result, setResult] = useState({ columns: [], rows: [], items: [], total: 0, max: 0, average: 0 })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const loadCube = async (cubeName) => {
    setLoading(true)
    setError('')
    try {
      const [measureRes, dimensionRes] = await Promise.all([
        api.get(`/cubes/${cubeName}/measures`),
        api.get(`/cubes/${cubeName}/dimensions`),
      ])
      const nextMeasures = measureRes.data || []
      const nextDimensions = dimensionRes.data || []
      setMeasures(nextMeasures)
      setDimensions(nextDimensions)

      const measure = nextMeasures[0]?.unique_name || ''
      const guessUnit = (nextMeasures[0]?.caption || '').toLowerCase().includes('doanh') ? 'VND' : ''
      const dimension = nextDimensions[0] || {}
      const hierarchy = dimension.default_hierarchy || dimension.unique_name || ''
      setSelectedMeasure(measure)
      setSelectedUnit(guessUnit)
      setSelectedHierarchy(hierarchy)

      if (hierarchy) {
        const levelRes = await api.get(`/cubes/${cubeName}/levels`, { params: { hierarchy } })
        const nextLevels = levelRes.data || []
        setLevels(nextLevels)
        const visibleLevel = nextLevels.find((l) => Number(l.number) > 0) || nextLevels[0] || {}
        const levelName = visibleLevel.unique_name || ''
        setSelectedLevel(levelName)
        await loadQuery(cubeName, measure, hierarchy, levelName)
      } else {
        setLevels([])
        setResult({ columns: [], rows: [], items: [], total: 0, max: 0, average: 0 })
      }
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || 'Không tải được thông tin từ máy chủ phân tích.')
    } finally {
      setLoading(false)
    }
  }

  const loadQuery = async (cubeName, measure, hierarchy, level, nextTrail = []) => {
    setLoading(true)
    setError('')
    try {
      const response = await api.post('/query', { cube: cubeName, measure, hierarchy, level })
      setResult(normalizeResult(response.data))
      setTrail(nextTrail)
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || 'Không truy vấn được dữ liệu.')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    api.get('/cubes')
      .then((res) => {
        const nextCubes = res.data || []
        setCubes(nextCubes)
        const first = nextCubes[0]?.name || ''
        setSelectedCube(first)
        if (first) loadCube(first)
      })
      .catch((err) => setError(err?.response?.data?.detail || err.message || 'Không kết nối được máy chủ.'))
  }, [])

  const handleCubeChange = async (e) => {
    const cubeName = e.target.value
    setSelectedCube(cubeName)
    setTrail([])
    await loadCube(cubeName)
  }

  const handleMeasureChange = async (e) => {
    const measure = e.target.value
    setSelectedMeasure(measure)
    const m = measures.find((mm) => mm.unique_name === measure)
    setSelectedUnit((m?.caption || '').toLowerCase().includes('doanh') ? 'VND' : '')
    await loadQuery(selectedCube, measure, selectedHierarchy, selectedLevel, [])
  }

  const handleHierarchyChange = async (e) => {
    const hierarchy = e.target.value
    setSelectedHierarchy(hierarchy)
    try {
      const levelRes = await api.get(`/cubes/${selectedCube}/levels`, { params: { hierarchy } })
      const nextLevels = levelRes.data || []
      setLevels(nextLevels)
      const visibleLevel = nextLevels.find((l) => Number(l.number) > 0) || nextLevels[0] || {}
      const levelName = visibleLevel.unique_name || ''
      setSelectedLevel(levelName)
      await loadQuery(selectedCube, selectedMeasure, hierarchy, levelName, [])
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || 'Không tải được phân cấp.')
    }
  }

  const handleLevelChange = async (e) => {
    const levelName = e.target.value
    setSelectedLevel(levelName)
    await loadQuery(selectedCube, selectedMeasure, selectedHierarchy, levelName, [])
  }

  const handleDrill = async (item) => {
    const parentMember = item?.memberUniqueName || item?.memberCaption || ''
    if (!parentMember) {
      setError('Không lấy được thông tin để phân tích sâu hơn. Vui lòng thử lại.')
      return
    }
    const currentIndex = levels.findIndex((l) => l.unique_name === selectedLevel)
    const nextLevel = levels[currentIndex + 1]?.unique_name
    if (!nextLevel) {
      setError('Đã đến cấp chi tiết nhất — không thể phân tích sâu hơn.')
      return
    }
    const nextTrail = [
      ...trail,
      {
        cube: selectedCube,
        measure: selectedMeasure,
        hierarchy: selectedHierarchy,
        level: selectedLevel,
        memberUniqueName: item.memberUniqueName,
        memberCaption: item.memberCaption,
      },
    ]
    try {
      setLoading(true)
      const response = await api.post('/drill', {
        cube: selectedCube,
        measure: selectedMeasure,
        parent_member: parentMember,
        parent_level: selectedLevel,
        next_level: nextLevel,
      })
      setResult(normalizeResult(response.data))
      setSelectedLevel(nextLevel)
      setTrail(nextTrail)
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || 'Không thể mở chi tiết.')
    } finally {
      setLoading(false)
    }
  }

  const handleDrillUp = async () => {
    if (trail.length === 0) return
    const previous = trail[trail.length - 1]
    const nextTrail = trail.slice(0, -1)
    setSelectedMeasure(previous.measure)
    setSelectedHierarchy(previous.hierarchy)
    setSelectedLevel(previous.level)
    await loadQuery(previous.cube, previous.measure, previous.hierarchy, previous.level, nextTrail)
  }

  const measureCaption = measures.find((m) => m.unique_name === selectedMeasure)?.caption || selectedMeasure

  const kpiCards = [
    { icon: '📊', label: 'Tổng giá trị', value: result.total, highlight: true },
    { icon: '🏆', label: 'Giá trị cao nhất', value: result.max },
    { icon: '📈', label: 'Trung bình', value: result.average },
    { icon: '🗂️', label: 'Số mục dữ liệu', value: result.items.length, noUnit: true },
  ]

  return (
    <div className="app-shell">
      {/* ── SIDEBAR ── */}
      <aside className="sidebar">
        <div className="brand-block">
          <div className="brand-mark">BI</div>
          <div>
            <div className="eyebrow">Hệ thống báo cáo</div>
            <h1>Phân tích dữ liệu</h1>
          </div>
        </div>

        <div className="sidebar-section-title">Cài đặt phân tích</div>

        <div className="control-group">
          <label><span className="label-icon">🗄️</span> Nguồn dữ liệu</label>
          <select value={selectedCube} onChange={handleCubeChange}>
            {cubes.map((c) => (
              <option key={c.name} value={c.name}>{c.caption || c.name}</option>
            ))}
          </select>
        </div>

        <div className="control-group">
          <label><span className="label-icon">📐</span> Chỉ số đo lường</label>
          <select value={selectedMeasure} onChange={handleMeasureChange}>
            {measures.map((m) => (
              <option key={m.unique_name} value={m.unique_name}>{m.caption || m.name}</option>
            ))}
          </select>
        </div>

        <div className="control-group">
          <label><span className="label-icon">🌐</span> Chiều phân tích</label>
          <select value={selectedHierarchy} onChange={handleHierarchyChange}>
            {dimensions.map((d) => (
              <option key={d.unique_name} value={d.default_hierarchy || d.unique_name}>
                {d.caption || d.name}
              </option>
            ))}
          </select>
        </div>

        <div className="control-group">
          <label><span className="label-icon">🔍</span> Mức độ chi tiết</label>
          <select value={selectedLevel} onChange={handleLevelChange}>
            {levels.map((l) => (
              <option key={l.unique_name} value={l.unique_name}>{l.caption || l.name}</option>
            ))}
          </select>
        </div>

        <div className="control-group">
          <label><span className="label-icon">💱</span> Đơn vị hiển thị</label>
          <select value={selectedUnit} onChange={(e) => setSelectedUnit(e.target.value)}>
            <option value="">(Tự động)</option>
            <option value="VND">VND</option>
            <option value="%">%</option>
            <option value="Sản phẩm">Sản phẩm</option>
            <option value="Đơn hàng">Đơn hàng</option>
          </select>
        </div>

        <div className="sidebar-tip">
          <span className="sidebar-tip-icon">💡</span>
          <p>
            Nhấn vào một cột trên biểu đồ để xem chi tiết cấp tiếp theo. Bấm <strong>"Quay lại"</strong> để trở về cấp trên.
          </p>
        </div>

        <button className="ghost-button" onClick={handleDrillUp} disabled={trail.length === 0}>
          ← Quay lại cấp trên
        </button>
      </aside>

      {/* ── MAIN ── */}
      <main className="main-panel">

        {/* Hero */}
        <header className="hero-card">
          <div>
            <span className="eyebrow">Phân tích thực tế</span>
            <h2>{cubes.find((c) => c.name === selectedCube)?.caption || selectedCube || 'Chưa chọn nguồn dữ liệu'}</h2>
            <div className="hero-subtitle">
              <span className="hero-tag">📐 {measureCaption || '—'}</span>
              <span className="hero-tag">🔍 {levels.find((l) => l.unique_name === selectedLevel)?.caption || '—'}</span>
              {selectedUnit && <span className="hero-tag">💱 {selectedUnit}</span>}
            </div>

            {/* Breadcrumb trail */}
            {trail.length > 0 && (
              <div className="breadcrumb-row">
                <span className="breadcrumb-label">Đang xem:</span>
                {trail.map((node, i) => (
                  <span className="breadcrumb-chip" key={`${node.memberUniqueName}-${i}`}>
                    {node.memberCaption || node.memberUniqueName}
                  </span>
                ))}
              </div>
            )}
          </div>

          <div className="status-pill">
            <span className={`status-dot${loading ? ' loading' : ''}`} />
            {loading ? 'Đang tải dữ liệu...' : 'Đã kết nối'}
          </div>
        </header>

        {/* Error */}
        {error && (
          <div className="error-banner">
            <span className="error-icon">⚠️</span>
            <span>{error}</span>
          </div>
        )}

        {/* KPI Cards */}
        <section className="kpi-grid">
          {kpiCards.map((card) => (
            <article className="kpi-card" key={card.label}>
              <span className="kpi-icon">{card.icon}</span>
              <span className="kpi-label">{card.label}</span>
              <strong className="kpi-value">
                {fmt(card.value)}
                {!card.noUnit && selectedUnit && (
                  <span className="kpi-unit"> {selectedUnit}</span>
                )}
              </strong>
            </article>
          ))}
        </section>

        {/* Chart + Table */}
        <section className="workspace-grid">
          <article className="panel chart-panel">
            <div className="panel-header">
              <div>
                <span className="eyebrow">Biểu đồ phân tích</span>
                <h3>{measureCaption || 'Chỉ số'} theo từng mục</h3>
              </div>
              <div className="panel-hint">
                👆 Nhấn vào cột để xem chi tiết
              </div>
            </div>
            <OlapChart
              data={result}
              onBarClick={handleDrill}
              unit={selectedUnit}
              measureCaption={measureCaption}
            />
          </article>

          <article className="panel table-panel">
            <div className="panel-header">
              <div>
                <span className="eyebrow">Bảng số liệu</span>
                <h3>Chi tiết từng mục</h3>
              </div>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th style={{ width: 40 }}>#</th>
                    <th>Tên mục</th>
                    <th className="align-right">Giá trị</th>
                  </tr>
                </thead>
                <tbody>
                  {result.items.map((item, i) => {
                    const barWidth = result.max > 0 ? Math.round((item.value / result.max) * 80) : 0
                    const rankClass = i === 0 ? 'top-1' : i === 1 ? 'top-2' : i === 2 ? 'top-3' : ''
                    return (
                      <tr key={item.memberUniqueName || i}>
                        <td>
                          <span className={`table-rank ${rankClass}`}>{i + 1}</span>
                        </td>
                        <td>{item.memberCaption}</td>
                        <td className="align-right">
                          <div className="value-bar-wrap">
                            <div className="value-bar" style={{ width: barWidth }} />
                            <span>{fmt(item.value)}{selectedUnit ? ` ${selectedUnit}` : ''}</span>
                          </div>
                        </td>
                      </tr>
                    )
                  })}
                  {result.items.length === 0 && !loading && (
                    <tr>
                      <td colSpan={3} style={{ textAlign: 'center', color: 'var(--muted)', padding: '32px 16px' }}>
                        Không có dữ liệu để hiển thị
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </article>
        </section>
      </main>
    </div>
  )
}