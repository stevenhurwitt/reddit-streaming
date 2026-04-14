const SUBREDDIT_COLORS = {
  news: '#0079d3',
  technology: '#46d160',
  worldnews: '#ff585b',
  programmerhumor: '#ff66ac',
}

function timeAgo(isoString) {
  const seconds = Math.floor((Date.now() - new Date(isoString)) / 1000)
  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  return `${days}d ago`
}

export default function PostCard({ post }) {
  const color = SUBREDDIT_COLORS[post.subreddit] || '#888'
  const isLink = post.url && !post.url.includes('reddit.com')

  return (
    <div
      style={{
        background: '#fff',
        borderRadius: 4,
        padding: '12px 16px',
        marginBottom: 8,
        border: '1px solid #ccc',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
        <span
          style={{
            background: color,
            color: '#fff',
            borderRadius: 12,
            padding: '2px 8px',
            fontSize: 11,
            fontWeight: 600,
            textTransform: 'uppercase',
          }}
        >
          r/{post.subreddit}
        </span>
        <span style={{ color: '#888', fontSize: 12 }}>
          by u/{post.author} · {timeAgo(post.created_utc)}
        </span>
        {isLink && (
          <span
            style={{
              marginLeft: 'auto',
              fontSize: 11,
              color: '#888',
              background: '#f6f7f8',
              border: '1px solid #ddd',
              borderRadius: 4,
              padding: '1px 6px',
            }}
          >
            🔗 link
          </span>
        )}
      </div>

      <a
        href={post.url || '#'}
        target="_blank"
        rel="noopener noreferrer"
        style={{ fontWeight: 600, fontSize: 16, lineHeight: 1.3, display: 'block', marginBottom: 6 }}
      >
        {post.title}
      </a>

      {post.selftext && (
        <p
          style={{
            color: '#555',
            fontSize: 13,
            marginBottom: 6,
            overflow: 'hidden',
            display: '-webkit-box',
            WebkitLineClamp: 3,
            WebkitBoxOrient: 'vertical',
          }}
        >
          {post.selftext}
        </p>
      )}

      <div style={{ display: 'flex', gap: 16, fontSize: 12, color: '#666' }}>
        <span>▲ {post.score.toLocaleString()} pts</span>
        <span>💬 {post.num_comments.toLocaleString()} comments</span>
      </div>
    </div>
  )
}
