import { useState, useEffect } from 'react'
import PostCard from './PostCard'

export default function Feed({ apiBase, filters, page, limit, onPageChange }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    setLoading(true)
    setError(null)

    const params = new URLSearchParams({
      limit,
      offset: page * limit,
      sort: filters.sort,
      order: filters.order,
      source: filters.source || 'postgres',
    })
    if (filters.subreddit) params.set('subreddit', filters.subreddit)
    if (filters.search) params.set('search', filters.search)

    fetch(`${apiBase}/posts?${params}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((d) => {
        setData(d)
        setLoading(false)
      })
      .catch((e) => {
        setError(e.message)
        setLoading(false)
      })
  }, [apiBase, filters, page, limit])

  const totalPages = data ? Math.ceil(data.total / limit) : 0

  if (loading) return <p style={{ textAlign: 'center', padding: 32 }}>Loading…</p>
  if (error) return <p style={{ color: 'red', padding: 16 }}>Error: {error}</p>
  if (!data?.posts?.length) return <p style={{ padding: 16 }}>No posts found.</p>

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
        <p style={{ fontSize: 12, color: '#666' }}>
          Showing {page * limit + 1}–{Math.min((page + 1) * limit, data.total)} of{' '}
          {data.total.toLocaleString()} posts
          {data.duplicate_count > 0 && (
            <span style={{ marginLeft: 8, color: '#856404', fontWeight: 600 }}>
              ({data.duplicate_count.toLocaleString()} duplicates removed)
            </span>
          )}
        </p>
        <span style={{ 
          fontSize: 11, 
          padding: '2px 8px', 
          borderRadius: 3, 
          background: data.source === 'trino' ? '#fff3cd' : '#d1ecf1',
          color: data.source === 'trino' ? '#856404' : '#055160',
          fontWeight: 600 
        }}>
          {data.source === 'postgres' ? '📊 Clean' : '🗄️ Raw'}
        </span>
      </div>

      {data.posts.map((post) => (
        <PostCard key={`${post.subreddit}-${post.post_id}`} post={post} />
      ))}

      <div style={{ display: 'flex', justifyContent: 'center', gap: 8, marginTop: 16 }}>
        <button
          disabled={page === 0}
          onClick={() => onPageChange(page - 1)}
          style={btnStyle}
        >
          ← Prev
        </button>
        <span style={{ padding: '6px 12px', fontSize: 14 }}>
          Page {page + 1} / {totalPages}
        </span>
        <button
          disabled={page >= totalPages - 1}
          onClick={() => onPageChange(page + 1)}
          style={btnStyle}
        >
          Next →
        </button>
      </div>
    </div>
  )
}

const btnStyle = {
  padding: '6px 16px',
  borderRadius: 4,
  border: '1px solid #ccc',
  background: '#fff',
  cursor: 'pointer',
  fontSize: 14,
}
