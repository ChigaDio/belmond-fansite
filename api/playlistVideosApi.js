// pages/api/playlistVideosApi.js
import { MongoClient } from 'mongodb';

const uri = process.env.DB;
const client = new MongoClient(uri);

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method Not Allowed' });

  const {
    playlistTitle = '',
    sortBy = 'published_at',
    sortOrder = '-1'
  } = req.query;

  if (!playlistTitle.trim()) {
    return res.status(400).json({ error: 'playlistTitle is required' });
  }

  const decodedTitle = decodeURIComponent(playlistTitle.trim());

  try {
    await client.connect();
    const db = client.db('belmond_fan_data');
    const collection = db.collection('videos');

    const filter = {
      playlist_titles: decodedTitle   // 完全一致
      // 大文字小文字を無視したい場合は以下のように変更可能：
      // playlist_titles: { $regex: new RegExp(`^${decodedTitle}$`, 'i') }
    };

    const sortDirection = sortOrder === '-1' ? -1 : 1;
    const sort = { [sortBy]: sortDirection };

    // 全件取得（skip/limit を削除）
    const videos = await collection
      .find(filter)
      .sort(sort)
      .toArray();

    const totalVideos = videos.length;

    res.status(200).json({
      videos,
      totalVideos,
      // page 関連は不要になったので削除（または固定値で返すことも可）
      currentPage: 1,
      totalPages: 1
    });

  } catch (error) {
    console.error('PlaylistVideos API Error:', error);
    res.status(500).json({ error: 'サーバーエラー', details: error.message });
  }
}