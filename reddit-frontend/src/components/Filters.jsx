export default function Filters({ subreddits, filters, onChange }) {
  const update = (key, value) => onChange({ ...filters, [key]: value })

  const inputStyle = {
    padding: '6px 10px',
    borderRadius: 4,
    border: '1px solid #ccc',
    fontSize: 14,
    background: '#fff',
  }

  const sourceStyle = {
    ...inputStyle,
    fontWeight: 600,
    background: filters.source === 'trino' ? '#fff3cd' : '#d1ecf1',
    borderColor: filters.source === 'trino' ? '#ffc107' : '#0dcaf0',
  }

  return (
    <div
      style={{
        display: 'flex',
        gap: 8,
        flexWrap: 'wrap',
        marginBottom: 12,
        background: '#fff',
        padding: '10px 12px',
        borderRadius: 4,
        border: '1px solid #ccc',
      }}
    >
      <select
        style={sourceStyle}
        value={filters.source}
        onChange={(e) => update('source', e.target.value)}
        title={filters.source === 'postgres' ? 'Clean data from Postgres' : 'Raw deduplicated data from Trino'}
      >
        <option value="postgres">📊 Postgres (Clean)</option>
        <option value="trino">🗄️ Trino (Raw)</option>
      </select>

      <input
        style={{ ...inputStyle, flex: '1 1 180px' }}
        placeholder="Search title or author…"
        value={filters.search}
        onChange={(e) => update('search', e.target.value)}
      />

      <select
        style={inputStyle}
        value={filters.subreddit}
        onChange={(e) => update('subreddit', e.target.value)}
      >
        <option value="">All subreddits</option>
        {subreddits.map((s) => (
          <option key={s} value={s}>
            r/{s}
          </option>
        ))}
      </select>

      <select
        style={inputStyle}
        value={filters.sort}
        onChange={(e) => update('sort', e.target.value)}
      >
        <option value="created_utc">Newest</option>
        <option value="score">Top Score</option>
        <option value="num_comments">Most Comments</option>
      </select>

      <select
        style={inputStyle}
        value={filters.order}
        onChange={(e) => update('order', e.target.value)}
      >
        <option value="desc">Descending</option>
        <option value="asc">Ascending</option>
      </select>
    </div>
  )
}
