import React, { useEffect, useState } from 'react'
import axios from 'axios'
import OlapChart from './components/OlapChart'

const api = axios.create({ baseURL: 'http://localhost:8000/api/olap' })

function isFullHierarchyRef(value) {
  const text = String(value || '').trim()
  return text.startsWith('[') && text.includes('].[')
}

function resolveHierarchyRef(dim) {
  const defaultHierarchy = dim?.default_hierarchy || ''
  if (isFullHierarchyRef(defaultHierarchy)) return defaultHierarchy
  if (isFullHierarchyRef(dim?.unique_name)) return dim.unique_name
  return defaultHierarchy || dim?.unique_name || ''
}

function getDimensionKeyFromHierarchy(hierarchy) {
  const text = String(hierarchy || '').trim()
  const dimensionEnd = text.indexOf('].')
  return dimensionEnd >= 0 ? `${text.slice(0, dimensionEnd + 1)}]` : ''
}

function normalizeResult(result, preferredMeasure = '') {
  const columns = result?.columns || []
  const rows = result?.rows || []
  const uniqueIndexes = columns
    .map((col, i) => (col.toUpperCase().includes('MEMBER_UNIQUE_NAME') ? i : -1))
    .filter((i) => i >= 0)
  const captionIndexes = columns
    .map((col, i) => (col.toUpperCase().includes('MEMBER_CAPTION') ? i : -1))
    .filter((i) => i >= 0)
  const uniqueIndex = uniqueIndexes.length > 0 ? uniqueIndexes[0] : 0
  const captionIndex = captionIndexes.length > 0 ? captionIndexes[0] : 0

  const looksLikeMdxUniqueName = (v) => {
    const t = String(v || '').trim()
    return t.startsWith('[') && t.includes('].[')
  }

  const normalizeHint = (value) => String(value || '')
    .toLowerCase()
    .replace(/[\[\].&]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()

  const measureHint = normalizeHint(preferredMeasure)
  const excludedColumnPattern = /(MEMBER_UNIQUE_NAME|MEMBER_CAPTION|LEVEL_NUMBER|MEMBER_KEY|CHILDREN_CARDINALITY|PARENT_LEVEL|PARENT_UNIQUE_NAME)/i

  const isNumericCell = (value) => {
    if (value === null || value === undefined || value === '') return false
    const normalized = String(value).replace(/\s/g, '').replace(/\./g, '').replace(',', '.')
    return Number.isFinite(Number(normalized))
  }

  const candidateIndexes = columns
    .map((column, index) => ({ column, index }))
    .filter(({ column, index }) => !excludedColumnPattern.test(String(column || '')) && rows.some((row) => isNumericCell(row[index])))

  const scoreCandidate = ({ column, index }) => {
    const header = normalizeHint(column)
    const sampleCount = rows.reduce((count, row) => count + (isNumericCell(row[index]) ? 1 : 0), 0)
    let score = sampleCount
    if (measureHint && header) {
      if (header === measureHint) score += 1000
      if (header.includes(measureHint)) score += 500
      const measureTokens = measureHint.split(' ').filter(Boolean)
      if (measureTokens.length > 0 && measureTokens.every((token) => header.includes(token))) score += 250
    }
    return score
  }

  const valueIndex = candidateIndexes.length > 0
    ? candidateIndexes.sort((a, b) => scoreCandidate(b) - scoreCandidate(a))[0].index
    : columns.length - 1

  const extractUniqueName = (row) => {
    const d = row[uniqueIndex]
    if (looksLikeMdxUniqueName(d)) return d
    return row.find((c) => looksLikeMdxUniqueName(c)) || ''
  }

  const extractCaption = (row) => {
    const d = row[captionIndex]
    if (d !== undefined && d !== null && String(d).trim() !== '') return d
    return row.find((c) => c !== undefined && c !== null && String(c).trim() !== '' && !looksLikeMdxUniqueName(c)) || ''
  }

  const parseNumericValue = (value) => {
    if (value === null || value === undefined || value === '') return 0
    if (typeof value === 'number') return value
    const text = String(value).trim()
    const normalized = text.includes(',') && text.includes('.')
      ? text.replace(/\./g, '').replace(',', '.')
      : text.replace(',', '.')
    const parsed = Number(normalized)
    return Number.isFinite(parsed) ? parsed : 0
  }

  const items = rows.map((row) => {
    const value = parseNumericValue(row[valueIndex])
    return {
      memberUniqueName: extractUniqueName(row),
      memberCaption: extractCaption(row),
      value,
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
  const [slicers, setSlicers] = useState({})
  const [dimensionMembers, setDimensionMembers] = useState({})
  const [expandedSlicerDim, setExpandedSlicerDim] = useState(null)
  const [pivotTarget, setPivotTarget] = useState(null)

  const buildWhereClause = (activeHierarchy = '') => {
    const activeDimensionKey = getDimensionKeyFromHierarchy(activeHierarchy)
    const whereTerms = Object.entries(slicers)
      .filter(([dimensionKey, member]) => {
        if (!member || !member.trim().startsWith('[')) return false
        if (!activeDimensionKey) return true
        return dimensionKey !== activeDimensionKey
      })
      .map(([_, member]) => member)
    return whereTerms.length > 0 ? whereTerms.join(', ') : null
  }

  const loadDimensionMembers = async (cubeName, levelUniqueName, hierarchyUniqueName) => {
    if (dimensionMembers[levelUniqueName]) return
    try {
      const response = await api.post('/query', {
        cube: cubeName,
        measure: selectedMeasure,
        hierarchy: hierarchyUniqueName,
        level: levelUniqueName,
      })
      const normalized = normalizeResult(response.data, selectedMeasure)
      setDimensionMembers((prev) => ({
        ...prev,
        [levelUniqueName]: normalized.items,
      }))
    } catch (err) {
      console.error('Failed to load members for', levelUniqueName, err)
    }
  }

  const handleSliceChange = (dimensionUniqueName, memberUniqueName) => {
    setSlicers((prev) => ({
      ...prev,
      [dimensionUniqueName]: memberUniqueName || '',
    }))
  }

  const handlePivot = async (targetDimensionUniqueName) => {
    if (!targetDimensionUniqueName) return
    setLoading(true)
    setError('')
    try {
      const where = buildWhereClause(selectedHierarchy)
      const response = await api.post('/pivot', {
        cube: selectedCube,
        measure: selectedMeasure,
        hierarchy: selectedHierarchy,
        level: selectedLevel,
        columns: [targetDimensionUniqueName],
        where,
      })
      setResult(normalizeResult(response.data, selectedMeasure))
      // Update selected hierarchy to the new pivot target
      const targetDim = dimensions.find(d => d.unique_name === targetDimensionUniqueName)
      if (targetDim) {
        setSelectedHierarchy(resolveHierarchyRef(targetDim))
      }
      setPivotTarget(null)
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || 'Không xoay chiều được.')
    } finally {
      setLoading(false)
    }
  }

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
      setSlicers({})
      setDimensionMembers({})
      setExpandedSlicerDim(null)

      const measure = nextMeasures[0]?.unique_name || ''
      const guessUnit = (nextMeasures[0]?.caption || '').toLowerCase().includes('doanh') ? 'VND' : ''
      const dimension = nextDimensions[0] || {}
      const hierarchy = resolveHierarchyRef(dimension)
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
      const where = buildWhereClause(hierarchy)
      const response = await api.post('/query', { cube: cubeName, measure, hierarchy, level, where })
      setResult(normalizeResult(response.data, measure))
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

  // Auto-reload query when slicers change
  useEffect(() => {
    if (selectedCube && selectedMeasure && selectedHierarchy && selectedLevel) {
      loadQuery(selectedCube, selectedMeasure, selectedHierarchy, selectedLevel, trail)
    }
  }, [slicers])

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
      const activeDimensionKey = getDimensionKeyFromHierarchy(hierarchy)
      if (activeDimensionKey) {
        setSlicers((prev) => {
          if (!prev[activeDimensionKey]) return prev
          const next = { ...prev }
          delete next[activeDimensionKey]
          return next
        })
      }
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
    const uniqueName = item?.memberUniqueName || ''
    const caption = item?.memberCaption || ''
    const parentMember = uniqueName.startsWith('[Measures].') && !selectedHierarchy.startsWith('[Measures]')
      ? caption
      : (uniqueName || caption || '')
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
      setResult(normalizeResult(response.data, selectedMeasure))
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
    { label: 'Tổng giá trị', value: result.total, highlight: true },
    { label: 'Giá trị cao nhất', value: result.max },
    { label: 'Trung bình', value: result.average },
    { label: 'Số mục dữ liệu', value: result.items.length, noUnit: true },
  ]

  return (
    <div className="app-shell">
      {/* ── SIDEBAR ── */}
      <aside className="sidebar">
        <div className="sidebar-section-title">Cài đặt phân tích</div>

        <div className="control-group">
          <label>Nguồn dữ liệu</label>
          <select value={selectedCube} onChange={handleCubeChange}>
            {cubes.map((c) => (
              <option key={c.name} value={c.name}>{c.caption || c.name}</option>
            ))}
          </select>
        </div>

        <div className="control-group">
          <label>Chỉ số đo lường</label>
          <select value={selectedMeasure} onChange={handleMeasureChange}>
            {measures.map((m) => (
              <option key={m.unique_name} value={m.unique_name}>{m.caption || m.name}</option>
            ))}
          </select>
        </div>

        <div className="control-group">
          <label>Chiều phân tích</label>
          <select value={selectedHierarchy} onChange={handleHierarchyChange}>
            {dimensions.map((d) => (
              <option key={d.unique_name} value={resolveHierarchyRef(d)}>
                {d.caption || d.name}
              </option>
            ))}
          </select>
        </div>

        <div className="control-group">
          <label>Mức độ chi tiết</label>
          <select value={selectedLevel} onChange={handleLevelChange}>
            {levels.map((l) => (
              <option key={l.unique_name} value={l.unique_name}>{l.caption || l.name}</option>
            ))}
          </select>
        </div>

        <div className="control-group">
          <label>Đơn vị hiển thị</label>
          <select value={selectedUnit} onChange={(e) => setSelectedUnit(e.target.value)}>
            <option value="">(Tự động)</option>
            <option value="VND">VND</option>
            <option value="%">%</option>
            <option value="Sản phẩm">Sản phẩm</option>
            <option value="Đơn hàng">Đơn hàng</option>
          </select>
        </div>

        {/* Slicers section */}
        <div className="sidebar-section-title" style={{ marginTop: '24px' }}>Bộ lọc dữ liệu</div>
        {dimensions
          .filter((d) => d.unique_name !== (selectedHierarchy || '').split('].')[0] + ']')
          .map((dim) => (
            <div key={dim.unique_name} className="control-group">
              <button
                type="button"
                onClick={() => {
                  setExpandedSlicerDim(expandedSlicerDim === dim.unique_name ? null : dim.unique_name)
                  if (!expandedSlicerDim || expandedSlicerDim !== dim.unique_name) {
                    const hierarchy = resolveHierarchyRef(dim)
                    loadDimensionMembers(selectedCube, hierarchy, hierarchy)
                  }
                }}
                style={{
                  width: '100%',
                  padding: '8px 10px',
                  border: '1px solid #2f2f2f',
                  background: '#0b0b0b',
                  cursor: 'pointer',
                  borderRadius: '6px',
                  textAlign: 'left',
                  fontSize: '14px',
                  color: '#ffffff',
                }}
              >
                {dim.caption || dim.name}
              </button>
              {expandedSlicerDim === dim.unique_name && (
                <div style={{ marginTop: '8px', maxHeight: '200px', overflow: 'auto', border: '1px solid #2f2f2f', borderRadius: '6px', background: '#0b0b0b' }}>
                  <button
                    onClick={() => handleSliceChange(dim.unique_name, '')}
                    style={{
                      display: 'block',
                      width: '100%',
                      padding: '6px',
                      border: 'none',
                      background: '#0b0b0b',
                      cursor: 'pointer',
                      textAlign: 'left',
                      fontSize: '12px',
                      color: '#ffffff',
                      fontWeight: 500,
                    }}
                  >
                    Không lọc
                  </button>
                  {(dimensionMembers[resolveHierarchyRef(dim)] || []).map((member) => (
                    <button
                      key={member.memberUniqueName}
                      onClick={() => handleSliceChange(dim.unique_name, member.memberUniqueName)}
                      style={{
                        display: 'block',
                        width: '100%',
                        padding: '6px',
                        border: 'none',
                        background: slicers[dim.unique_name] === member.memberUniqueName ? '#1f2937' : '#0b0b0b',
                        cursor: 'pointer',
                        textAlign: 'left',
                        fontSize: '12px',
                        color: '#ffffff',
                      }}
                      onMouseEnter={(e) => {
                        e.target.style.background = '#1a1a1a'
                      }}
                      onMouseLeave={(e) => {
                        e.target.style.background = slicers[dim.unique_name] === member.memberUniqueName ? '#1f2937' : '#0b0b0b'
                      }}
                    >
                      {member.memberCaption}
                    </button>
                  ))}
                </div>
              )}
            </div>
          ))}

        <div className="sidebar-tip">
          <p>Drilldown: bấm vào cột. Roll-up: dùng “Quay lại cấp trên”. Pivot: đổi chiều ở khối trên cùng. Slice &amp; Dice: chọn bộ lọc bên dưới.</p>
        </div>

        <button className="ghost-button" onClick={handleDrillUp} disabled={trail.length === 0}>
          Quay lại cấp trên
        </button>
      </aside>

      {/* ── MAIN ── */}
      <main className="main-panel">

        {/* Hero */}
        <header className="hero-card">
          <div>
            <span className="eyebrow">Phân tích thực tế</span>
            <h2>{cubes.find((c) => c.name === selectedCube)?.caption || selectedCube || 'Chưa chọn nguồn dữ liệu'}</h2>

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

            {/* Pivot selector */}
            <div style={{ marginTop: '12px', display: 'flex', gap: '8px', alignItems: 'center' }}>
              <select
                value={pivotTarget || ''}
                onChange={(e) => {
                  const target = e.target.value
                  if (target) handlePivot(target)
                }}
                style={{
                  fontSize: '12px',
                  padding: '4px 10px',
                  border: '1px solid #2f2f2f',
                  borderRadius: '4px',
                  background: '#0b0b0b',
                  color: '#ffffff',
                  cursor: 'pointer',
                }}
              >
                <option value="">Xoay chiều</option>
                {dimensions
                  .filter((d) => d.unique_name !== (selectedHierarchy || '').split('].')[0] + ']')
                  .map((dim) => (
                    <option key={dim.unique_name} value={resolveHierarchyRef(dim)}>
                      {dim.caption || dim.name}
                    </option>
                  ))}
              </select>
            </div>
          </div>
        </header>

        {/* Error */}
        {error && (
          <div className="error-banner">
            <span>{error}</span>
          </div>
        )}

        {/* KPI Cards */}
        <section className="kpi-grid">
          {kpiCards.map((card) => (
            <article className="kpi-card" key={card.label}>
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