// pages/api/favoritesApi.js
import { MongoClient } from 'mongodb';

const uri = process.env.DB_COUNT;
const client = new MongoClient(uri);

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method Not Allowed' });

  const { ids = '', addNum = 1 } = req.query;

  if (!ids.trim()) {
    return res.status(200).json({ success: true, modified: 0 });
  }

  const videoIds = ids.split(',').map(id => id.trim()).filter(Boolean);

  try {
    await client.connect();
    const db = client.db('belmond_fan_data');
    const collection = db.collection('videos');

    const result = await collection.updateMany(
      { _id: { $in: videoIds } },
      { $inc: { favoNum: parseInt(addNum) } }
    );

    res.status(200).json({
      success: true,
      modified: result.modifiedCount
    });

  } catch (error) {
    console.error('Favorites API Error:', error);
    res.status(500).json({ error: 'サーバーエラー', details: error.message });
  }
}