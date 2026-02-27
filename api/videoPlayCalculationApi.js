// pages/api/videosPlayCountApi.js
import { MongoClient } from 'mongodb';

const uri = process.env.DB_COUNT;
const client = new MongoClient(uri);

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method Not Allowed' });
  }

  const { id, addNum = 1 } = req.query;

  if (!id) {
    return res.status(400).json({ error: 'id is required' });
  }

  const num = parseInt(addNum);

  if (isNaN(num)) {
    return res.status(400).json({ error: 'addNum must be a number' });
  }

  try {
    await client.connect();
    const db = client.db('belmond_fan_data');
    const collection = db.collection('videos');

    const result = await collection.updateOne(
      { _id: id },
      { $inc: { playNum: num } }
    );

    res.status(200).json({
      success: true,
      modified: result.modifiedCount
    });

  } catch (error) {
    console.error('PlayCount API Error:', error);
    res.status(500).json({ error: 'サーバーエラー', details: error.message });
  }
}