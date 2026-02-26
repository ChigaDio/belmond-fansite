// pages/api/videosDetailsApi.js または app/api/videosDetailsApi/route.js（Next.jsの場合）

import { MongoClient } from 'mongodb';

const uri = process.env.DB; // MongoDB接続文字列（環境変数）
const client = new MongoClient(uri);

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method Not Allowed' });
  }

  const {
    page = 1,
    search = '',          // カンマ区切りで複数タグ
    playlists = '',       // カンマ区切りで複数プレイリストID
    startDate = '',
    endDate = '',
    type = '',
    sortBy = 'published_at',
    sortOrder = '-1'      // -1 = desc, 1 = asc
  } = req.query;

  const limit = 20;
  const skip = (parseInt(page) - 1) * limit;

  try {
    await client.connect();
    const db = client.db('belmond_fan_data');
    const collection = db.collection('videos');

    // フィルタ構築
    const filter = {};

    // タイトル検索（複数AND）
    if (search) {
      const keywords = search.split(',').map(k => k.trim()).filter(Boolean);
      if (keywords.length > 0) {
        filter.title = {
          $and: keywords.map(kw => ({ $regex: kw, $options: 'i' }))
        };
      }
    }

    // プレイリスト絞り込み（複数）
    if (playlists) {
      const playlistTitles = playlists.split(',').map(t => decodeURIComponent(t.trim())).filter(Boolean);
      if (playlistTitles.length > 0) {
        filter.playlist_titles = { $in: playlistTitles };
      }
    }

    // タイプフィルタ（live / shorts / normal_video）
    if (type) {
      filter.type = type; // DBにtypeフィールドがある前提
    }

    // 日付範囲
    if (startDate || endDate) {
      filter.published_at = {};
      if (startDate) filter.published_at.$gte = new Date(startDate);
      if (endDate) filter.published_at.$lte = new Date(endDate + 'T23:59:59.999Z');
    }

    // ソート
    const sortDirection = sortOrder === '-1' ? -1 : 1;
    const sort = { [sortBy]: sortDirection };

    // 総件数
    const totalCount = await collection.countDocuments(filter);

    // データ取得
    const videos = await collection
      .find(filter)
      .sort(sort)
      .skip(skip)
      .limit(limit)
      .toArray();

    res.status(200).json({
      videos,
      currentPage: parseInt(page),
      totalPages: Math.ceil(totalCount / limit),
      totalVideos: totalCount
    });

  } catch (error) {
    console.error('API Error:', error);
    res.status(500).json({
      error: 'サーバーエラー',
      details: error.message
    });
  } finally {
    // await client.close(); // Serverlessでは接続を閉じない方が良い場合が多い
  }
}