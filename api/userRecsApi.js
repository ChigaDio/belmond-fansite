// pages/api/userRecsApi.js
import { MongoClient } from 'mongodb';

const uri = process.env.DB_COUNT; // DB_COUNTを使用
const client = new MongoClient(uri);

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');

  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    await client.connect();
    const db = client.db('belmond_fan_data');
    const collection = db.collection('user_recommendations'); // 新規コレクション

    const { userId } = req.query || req.body;

    if (req.method === 'GET') {
      if (!userId) return res.status(400).json({ error: 'userIdが必要です' });
      const userRec = await collection.findOne({ userId });
      const recIds = userRec ? userRec.recIds || [] : [];
      return res.status(200).json({ recIds });
    }

    if (req.method === 'POST') {
      const { userId, videoId, action } = req.body; // action: 'add' | 'remove'
      if (!userId || !videoId) return res.status(400).json({ error: 'パラメータ不足' });

      let userRec = await collection.findOne({ userId });
      if (!userRec) {
        userRec = { userId, recIds: [] };
        await collection.insertOne(userRec);
      }

      let recIds = userRec.recIds || [];

      if (action === 'add') {
        if (recIds.length >= 5) return res.status(400).json({ error: '最大5件までです' });
        if (!recIds.includes(videoId)) recIds.push(videoId);
      } else if (action === 'remove') {
        recIds = recIds.filter(id => id !== videoId);
      }

      await collection.updateOne({ userId }, { $set: { recIds } }, { upsert: true });
      return res.status(200).json({ success: true, recIds });
    }

    return res.status(405).json({ error: 'Method Not Allowed' });
  } catch (error) {
    console.error('userRecsApi Error:', error);
    res.status(500).json({ error: 'サーバーエラー', details: error.message });
  } finally {
    // Vercel Serverlessでは自動クローズ
  }
}