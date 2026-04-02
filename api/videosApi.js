// pages/api/videosApi.js
// I.15違反対応＋playlistフィルタ完全対応版

const YOUTUBE_API_KEY = 'AIzaSyCCei86Wkk6Qme7vnbbx7O2P66Kbcr9z_4';
const CHANNEL_ID = 'UCbcc8fwhdUNlqi-J99ISYu4A';
const UPLOADS_PLAYLIST_ID = 'UUbcc8fwhdUNlqi-J99ISYu4A';

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method Not Allowed' });

  const {
    page = 1,
    search = '',
    playlists = '',
    startDate = '',
    endDate = '',
    type = '',
    sortBy = 'published_at',
    sortOrder = '-1'
  } = req.query;

  const limit = 20;
  const skip = (parseInt(page) - 1) * limit;

  try {
    // 1. プレイリストマッピングを取得（各動画がどのプレイリストに所属しているか）
    const playlistMap = await fetchPlaylistMapping();

    // 2. YouTubeから最新動画を直接取得
    let videos = await fetchAllVideosFromYouTube(playlistMap);

    // 3. フィルタ適用
    let filtered = videos;

    // タイトル検索
    if (search) {
      const keywords = search.split(',').map(k => k.trim().toLowerCase()).filter(Boolean);
      if (keywords.length > 0) {
        filtered = filtered.filter(v =>
          keywords.every(kw => v.title.toLowerCase().includes(kw))
        );
      }
    }

    // プレイリストフィルタ（ここが重要！）
    if (playlists) {
      const playlistTitles = playlists.split(',').map(t => decodeURIComponent(t.trim())).filter(Boolean);
      if (playlistTitles.length > 0) {
        filtered = filtered.filter(v =>
          v.playlist_titles && v.playlist_titles.some(t => playlistTitles.includes(t))
        );
      }
    }

    // 日付フィルタ
    if (startDate || endDate) {
      filtered = filtered.filter(v => {
        const pubDate = new Date(v.published_at);
        if (startDate && pubDate < new Date(startDate)) return false;
        if (endDate && pubDate > new Date(endDate + 'T23:59:59.999Z')) return false;
        return true;
      });
    }

    // 種類フィルタ
    if (type) {
      filtered = filtered.filter(v => v.content_category === type);
    }

    // ソート
    const sortDir = sortOrder === '-1' ? -1 : 1;
    filtered.sort((a, b) => {
      const valA = a[sortBy] || 0;
      const valB = b[sortBy] || 0;
      return valA > valB ? sortDir : valA < valB ? -sortDir : 0;
    });

    // ページネーション
    const totalVideos = filtered.length;
    const paginatedVideos = filtered.slice(skip, skip + limit);

    res.status(200).json({
      videos: paginatedVideos,
      currentPage: parseInt(page),
      totalPages: Math.ceil(totalVideos / limit),
      totalVideos: totalVideos
    });

  } catch (error) {
    console.error('YouTube API Error:', error);
    res.status(500).json({ error: 'YouTube API呼び出しに失敗しました', details: error.message });
  }
}

// ==================== ヘルパー関数 ====================

// 全プレイリストから「動画ID → 所属プレイリストタイトルの配列」を作成
async function fetchPlaylistMapping() {
  const map = new Map(); // videoId → ["プレイリスト名1", "プレイリスト名2", ...]

  let pageToken = '';
  do {
    const url = `https://www.googleapis.com/youtube/v3/playlists?part=snippet&channelId=${CHANNEL_ID}&maxResults=50&key=${YOUTUBE_API_KEY}&pageToken=${pageToken}`;
    const res = await fetch(url);
    const data = await res.json();

    if (!data.items) break;

    for (const pl of data.items) {
      const title = pl.snippet.title;
      let itemToken = '';

      do {
        const itemUrl = `https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId=${pl.id}&maxResults=50&key=${YOUTUBE_API_KEY}&pageToken=${itemToken}`;
        const itemRes = await fetch(itemUrl);
        const itemData = await itemRes.json();

        for (const item of itemData.items || []) {
          const vid = item.contentDetails.videoId;
          if (!map.has(vid)) map.set(vid, []);
          map.get(vid).push(title);
        }
        itemToken = itemData.nextPageToken || '';
      } while (itemToken);
    }

    pageToken = data.nextPageToken || '';
  } while (pageToken);

  return map;
}

// アップロードプレイリストから全動画を取得 + playlist_titlesを付与
async function fetchAllVideosFromYouTube(playlistMap) {
  const videoList = [];

  // 1. アップロード動画ID取得
  let pageToken = '';
  do {
    const url = `https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId=${UPLOADS_PLAYLIST_ID}&maxResults=50&key=${YOUTUBE_API_KEY}&pageToken=${pageToken}`;
    const res = await fetch(url);
    const data = await res.json();

    if (!data.items) break;
    const ids = data.items.map(item => item.contentDetails.videoId);
    videoList.push(...ids);
    pageToken = data.nextPageToken || '';
  } while (pageToken);

  // 2. 動画詳細取得
  const idsStr = videoList.join(',');
  const detailUrl = `https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails&id=${idsStr}&key=${YOUTUBE_API_KEY}`;
  const detailRes = await fetch(detailUrl);
  const detailData = await detailRes.json();

  return detailData.items.map(item => ({
    _id: item.id,
    title: item.snippet.title,
    published_at: item.snippet.publishedAt,
    view_count: parseInt(item.statistics?.viewCount || 0),
    like_count: parseInt(item.statistics?.likeCount || 0),
    comment_count: parseInt(item.statistics?.commentCount || 0),
    thumbnail_url: item.snippet.thumbnails?.maxres?.url ||
                    item.snippet.thumbnails?.high?.url ||
                    `https://i.ytimg.com/vi/${item.id}/maxresdefault.jpg`,
    content_category: item.contentDetails?.duration.includes('PT0M') ? 'SHORTS' : 'NORMAL_VIDEO',
    duration_sec: parseDuration(item.contentDetails?.duration || 'PT0S'),
    playlist_titles: playlistMap.get(item.id) || []   // ← これでプレイリストフィルタが効く！
  }));
}

function parseDuration(duration) {
  const match = duration.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
  if (!match) return 0;
  const hours = parseInt(match[1] || 0);
  const minutes = parseInt(match[2] || 0);
  const seconds = parseInt(match[3] || 0);
  return hours * 3600 + minutes * 60 + seconds;
}