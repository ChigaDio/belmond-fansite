// pages/api/favoritesApi.js
import { MongoClient } from 'mongodb';

const uri = process.env.DB;
const client = new MongoClient(uri);

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method Not Allowed' });

  const { ids = '' } = req.query;

  if (!ids.trim()) {
    return res.status(200).json({ videos: [] });
  }

  const videoIds = ids.split(',').map(id => id.trim()).filter(Boolean);

  try {
    await client.connect();
    const db = client.db('belmond_fan_data');
    const collection = db.collection('videos');

    const videos = await collection
      .find({ _id: { $in: videoIds } })
      .toArray();

    // クライアントが渡した順番を保つ
    const ordered = videoIds
      .map(id => videos.find(v => v._id === id))
      .filter(Boolean);

    res.status(200).json({ videos: ordered });

  } catch (error) {
    console.error('Favorites API Error:', error);
    res.status(500).json({ error: 'サーバーエラー', details: error.message });
  } finally {
    // client.close() は Vercel Serverless では不要（自動管理）
  }
}