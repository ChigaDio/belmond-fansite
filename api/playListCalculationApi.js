// pages/api/playNumApi.js
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

  const {
    playlistId = '',
    videoId = '',
    addNum = 1
  } = req.query;

  if (!playlistId && !videoId) {
    return res.status(400).json({ error: 'playlistId or videoId is required' });
  }

  const num = parseInt(addNum);
  if (isNaN(num)) {
    return res.status(400).json({ error: 'addNum must be a number' });
  }

  try {
    await client.connect();
    const db = client.db('belmond_fan_data');

    const playlistCol = db.collection('playlists');
    const videoCol = db.collection('videos');

    let playlistModified = 0;
    let videoModified = 0;

    if (playlistId) {
      const r = await playlistCol.updateOne(
        { title: playlistId },
        { $inc: { playNum: num } }
      );
      playlistModified = r.modifiedCount;
    }

    if (videoId) {
      const r = await videoCol.updateOne(
        { _id: videoId },
        { $inc: { playNum: num } }
      );
      videoModified = r.modifiedCount;
    }

    res.status(200).json({
      success: true,
      playlistModified,
      videoModified
    });

  } catch (error) {
    console.error('PlayNum API Error:', error);
    res.status(500).json({ error: 'サーバーエラー', details: error.message });
  }
}