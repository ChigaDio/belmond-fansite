// pages/api/videosDetailsApi.js

import { MongoClient } from 'mongodb';

const uri = process.env.DB;
const client = new MongoClient(uri);

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
    await client.connect();
    const db = client.db('belmond_fan_data');
    const collection = db.collection('videos');

    const filter = {};

    // タイトル検索
    if (search) {
      const keywords = search.split(',').map(k => k.trim()).filter(Boolean);
      if (keywords.length > 0) {
        filter.$and = keywords.map(kw => ({
          title: { $regex: kw, $options: 'i' }
        }));
      }
    }

    // プレイリストフィルタ（デバッグログ追加）
    if (playlists) {
      const playlistTitles = playlists.split(',').map(t => decodeURIComponent(t.trim())).filter(Boolean);
      console.log('Received playlists param:', playlists);               // ← Vercelログで確認用
      console.log('Decoded playlist titles:', playlistTitles);          
      if (playlistTitles.length > 0) {
        filter.playlist_titles = { $in: playlistTitles };
        console.log('Applied filter.playlist_titles:', filter.playlist_titles); // フィルタ適用ログ
      }
    }

    // 他のフィルタ（変更なし）
    if (type) filter.content_category = type;
    if (startDate || endDate) {
      filter.published_at = {};
      if (startDate) filter.published_at.$gte = new Date(startDate);
      if (endDate) filter.published_at.$lte = new Date(endDate + 'T23:59:59.999Z');
    }

    const sortDirection = sortOrder === '-1' ? -1 : 1;
    const sort = { [sortBy]: sortDirection };

    const totalCount = await collection.countDocuments(filter);
    const videos = await collection.find(filter).sort(sort).skip(skip).limit(limit).toArray();

    res.status(200).json({
      videos,
      currentPage: parseInt(page),
      totalPages: Math.ceil(totalCount / limit),
      totalVideos: totalCount
    });

  } catch (error) {
    console.error('API Error:', error);
    res.status(500).json({ error: 'サーバーエラー', details: error.message });
  }
}