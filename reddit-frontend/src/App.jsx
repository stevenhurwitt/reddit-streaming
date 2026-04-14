import { useState, useEffect } from 'react'
import Filters from './components/Filters'
import Feed from './components/Feed'

const API_BASE = import.meta.env.VITE_API_URL || '/api'

export default function App() {
  const [subreddits, setSubreddits] = useState([])
  const [filters, setFilters] = useState({
    subreddit: '',
    search: '',
    sort: 'created_utc',
    order: 'desc',
  })
  const [page, setPage] = useState(0)
  const LIMIT = 25

  useEffect(() => {
    fetch(`${API_BASE}/subreddits`)
      .then((r) => r.json())
      .then((d) => setSubreddits(d.subreddits))
  }, [])

  const handleFilterChange = (newFilters) => {
    setFilters(newFilters)
    setPage(0)
  }

  return (
    <div style={{ maxWidth: 800, margin: '0 auto', padding: '16px' }}>
      <h1 style={{ marginBottom: 16, fontSize: 24, color: '#ff4500' }}>
        🤖 Reddit Feed
      </h1>
      <Filters
        subreddits={subreddits}
        filters={filters}
        onChange={handleFilterChange}
      />
      <Feed
        apiBase={API_BASE}
        filters={filters}
        page={page}
        limit={LIMIT}
        onPageChange={setPage}
      />
    </div>
  )
}
