import React, { useEffect, useState } from 'react'
import axios from 'axios'
import OlapChart from './components/OlapChart'

const api = axios.create({ baseURL: 'http://localhost:8000/api/olap' })

function normalizeResult(result) {
  const columns = result?.columns || []
  const rows = result?.rows || []
  const uniqueIndexes = columns
    .map((column, index) => (column.toUpperCase().includes('MEMBER_UNIQUE_NAME') ? index : -1))
    .filter((index) => index >= 0)
  const captionIndexes = columns
    .map((column, index) => (column.toUpperCase().includes('MEMBER_CAPTION') ? index : -1))
    .filter((index) => index >= 0)
  const uniqueIndex = uniqueIndexes.length > 0 ? uniqueIndexes[uniqueIndexes.length - 1] : 0
  const captionIndex = captionIndexes.length > 0 ? captionIndexes[captionIndexes.length - 1] : 0
  const valueIndex = columns.length - 1

  const items = rows.map((row) => {
    const value = Number(row[valueIndex])
    return {
      memberUniqueName: uniqueIndex >= 0 ? row[uniqueIndex] : row[0],
      memberCaption: captionIndex >= 0 ? row[captionIndex] : row[0],
      value: Number.isFinite(value) ? value : 0,
      raw: row,
    }
  })

  const total = items.reduce((sum, item) => sum + item.value, 0)
  const max = items.reduce((best, item) => Math.max(best, item.value), 0)
  const average = items.length > 0 ? total / items.length : 0

  return { columns, rows, items, total, max, average }
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
  const [result, setResult] = useState({ columns: [], rows: [], items: [] })
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
      // guess a default unit from measure caption
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
        const visibleLevel = nextLevels.find((level) => Number(level.number) > 0) || nextLevels[0] || {}
        const levelName = visibleLevel.unique_name || ''
        setSelectedLevel(levelName)
        await loadQuery(cubeName, measure, hierarchy, levelName)
      } else {
        setLevels([])
        setResult({ columns: [], rows: [], items: [] })
      }
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || 'Không tải được metadata SSAS')
    } finally {
      setLoading(false)
    }
  }

  const loadQuery = async (cubeName, measure, hierarchy, level, nextTrail = []) => {
    setLoading(true)
    setError('')
    try {
      const response = await api.post('/query', {
        cube: cubeName,
        measure,
        hierarchy,
        level,
      })
      setResult(normalizeResult(response.data))
      setTrail(nextTrail)
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || 'Không truy vấn được SSAS')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    api.get('/cubes')
      .then((response) => {
        const nextCubes = response.data || []
        setCubes(nextCubes)
        const firstCube = nextCubes[0]?.name || ''
        setSelectedCube(firstCube)
        if (firstCube) {
          loadCube(firstCube)
        }
      })
      .catch((err) => setError(err?.response?.data?.detail || err.message || 'Không tải được danh sách cube'))
  }, [])

  const handleCubeChange = async (event) => {
    const cubeName = event.target.value
    setSelectedCube(cubeName)
    setTrail([])
    await loadCube(cubeName)
  }

  const handleMeasureChange = async (event) => {
    const measure = event.target.value
    setSelectedMeasure(measure)
    // update guessed unit when measure changes
    const m = measures.find((mm) => mm.unique_name === measure)
    const guess = (m?.caption || '').toLowerCase().includes('doanh') ? 'VND' : ''
    setSelectedUnit(guess)
    await loadQuery(selectedCube, measure, selectedHierarchy, selectedLevel, [])
  }

  const handleHierarchyChange = async (event) => {
    const hierarchy = event.target.value
    setSelectedHierarchy(hierarchy)
    try {
      const levelRes = await api.get(`/cubes/${selectedCube}/levels`, { params: { hierarchy } })
      const nextLevels = levelRes.data || []
      setLevels(nextLevels)
      const visibleLevel = nextLevels.find((level) => Number(level.number) > 0) || nextLevels[0] || {}
      const levelName = visibleLevel.unique_name || ''
      setSelectedLevel(levelName)
      await loadQuery(selectedCube, selectedMeasure, hierarchy, levelName, [])
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || 'Không tải được cấp dữ liệu')
    }
  }

  const handleLevelChange = async (event) => {
    const levelName = event.target.value
    setSelectedLevel(levelName)
    await loadQuery(selectedCube, selectedMeasure, selectedHierarchy, levelName, [])
  }

  const handleDrill = async (item) => {
    if (!item?.memberUniqueName) {
      return
    }

    const currentIndex = levels.findIndex((level) => level.unique_name === selectedLevel)
    const nextLevel = levels[currentIndex + 1]?.unique_name

    if (!nextLevel) {
      setError('Đã là cấp chi tiết nhất')
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
        parent_member: item.memberUniqueName,
        parent_level: selectedLevel,
        next_level: nextLevel,
      })
      setResult(normalizeResult(response.data))
      setSelectedLevel(nextLevel)
      setTrail(nextTrail)
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || 'Không thể mở chi tiết')
    } finally {
      setLoading(false)
    }
  }

  const handleDrillUp = async () => {
    if (trail.length === 0) {
      return
    }

    const previous = trail[trail.length - 1]
    const nextTrail = trail.slice(0, -1)
    setSelectedMeasure(previous.measure)
    setSelectedHierarchy(previous.hierarchy)
    setSelectedLevel(previous.level)
    await loadQuery(previous.cube, previous.measure, previous.hierarchy, previous.level, nextTrail)
  }

  const summaryCards = [
    { label: 'Tổng giá trị', value: result.total },
    { label: 'Giá trị lớn nhất', value: result.max },
    { label: 'Giá trị trung bình', value: result.average },
    { label: 'Số điểm dữ liệu', value: result.items.length },
  ]

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand-block">
          <div className="brand-mark">DW</div>
          <div>
            <div className="eyebrow">Enterprise BI</div>
            <h1>OLAP Dashboard</h1>
          </div>
        </div>

        <div className="control-group">
          <label>Cube</label>
          <select value={selectedCube} onChange={handleCubeChange}>
            {cubes.map((cube) => (
              <option key={cube.name} value={cube.name}>
                {cube.caption || cube.name}
              </option>
            ))}
          </select>
        </div>

        <div className="control-group">
          <label>Measure</label>
          <select value={selectedMeasure} onChange={handleMeasureChange}>
            {measures.map((measure) => (
              <option key={measure.unique_name} value={measure.unique_name}>
                {measure.caption || measure.name}
              </option>
            ))}
          </select>
        </div>

        <div className="control-group">
          <label>Hierarchy</label>
          <select value={selectedHierarchy} onChange={handleHierarchyChange}>
            {dimensions.map((dimension) => (
              <option key={dimension.unique_name} value={dimension.default_hierarchy || dimension.unique_name}>
                {dimension.caption || dimension.name}
              </option>
            ))}
          </select>
        </div>

        <div className="control-group">
          <label>Level</label>
          <select value={selectedLevel} onChange={handleLevelChange}>
            {levels.map((level) => (
              <option key={level.unique_name} value={level.unique_name}>
                {level.caption || level.name}
              </option>
            ))}
          </select>
        </div>

        <div className="control-group">
          <label>Đơn vị</label>
          <select value={selectedUnit} onChange={(e) => setSelectedUnit(e.target.value)}>
            <option value="">(Tự động)</option>
            <option value="VND">VND</option>
            <option value="%">%</option>
            <option value="Số lượng">Số lượng</option>
            <option value="Khác">Khác...</option>
          </select>
        </div>

        <div className="sidebar-note">
          <h3>Tác vụ nhanh</h3>
          <p>Bấm vào một cột để xem chi tiết cấp tiếp theo. Dùng nút "Quay lại" để trở về.</p>
        </div>

        <button className="ghost-button" onClick={handleDrillUp} disabled={trail.length === 0}>
          Quay lại
        </button>
      </aside>

      <main className="main-panel">
        <header className="hero-card">
          <div>
            <span className="eyebrow">Số liệu thực tế</span>
            <h2>{selectedCube || 'Chưa có cube'}</h2>
            <p>
              {selectedMeasure || 'Measure'} · {selectedHierarchy || 'Hierarchy'} · {selectedLevel || 'Level'}
            </p>
            <div className="breadcrumb-row">
              <span className="breadcrumb-label">Lộ trình phân tích</span>
              {trail.length === 0 ? <span className="breadcrumb-chip">Gốc</span> : null}
              {trail.map((node, index) => (
                <span className="breadcrumb-chip" key={`${node.memberUniqueName}-${index}`}>
                  {index + 1}. {node.memberCaption || node.memberUniqueName}
                </span>
              ))}
            </div>
          </div>
          <div className="status-pill">{loading ? 'Đang tải dữ liệu...' : 'Kết nối SSAS'}</div>
        </header>

        {error ? <div className="error-banner">{error}</div> : null}

        <section className="kpi-grid">
          {summaryCards.map((card) => (
            <article className="kpi-card" key={card.label}>
              <span>{card.label}</span>
              <strong>{Number(card.value).toLocaleString('vi-VN')} {selectedUnit}</strong>
            </article>
          ))}
        </section>

        <section className="workspace-grid">
          <article className="panel chart-panel">
            <div className="panel-header">
              <div>
                <span className="eyebrow">Biểu đồ</span>
                <h3>Phân tích theo cấp dữ liệu</h3>
              </div>
              <div className="panel-hint">Bấm vào cột để xem phân rã chi tiết</div>
            </div>
            <OlapChart data={result} onBarClick={handleDrill} unit={selectedUnit} measureCaption={measures.find(m=>m.unique_name===selectedMeasure)?.caption} />
          </article>

          <article className="panel table-panel">
            <div className="panel-header">
              <div>
                <span className="eyebrow">Bảng</span>
                <h3>Chi tiết</h3>
              </div>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Phần tử</th>
                    <th className="align-right">Giá trị</th>
                  </tr>
                </thead>
                <tbody>
                  {result.items.map((item) => (
                    <tr key={item.memberUniqueName}>
                      <td>{item.memberCaption}</td>
                      <td className="align-right">{Number(item.value).toLocaleString('vi-VN')} {selectedUnit}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </article>
        </section>
      </main>
    </div>
  )
}
