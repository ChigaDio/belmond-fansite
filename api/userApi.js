// pages/api/userApi.js
import { MongoClient } from 'mongodb';

const uri = process.env.DB_COUNT;
const client = new MongoClient(uri);

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  try {
    await client.connect();
    const database = client.db('belmond_fan_data');

    // 元のchannels取得
    if (req.method === 'GET' && !req.query.userId && !req.query.allRecs) {
      const collection = database.collection('channels');
      const data = await collection.findOne({});
      if (!data) {
        return res.status(404).json({ error: 'データが見つかりませんでした' });
      }
      return res.status(200).json(data);
    }

    // 全ユーザーのおすすめを取得（新しい機能）
    if (req.method === 'GET' && req.query.allRecs === 'true') {
      const recCollection = database.collection('user_recommendations');
      const allUsersRecs = await recCollection.find({}).toArray();

      // すべてのユーザーのおすすめIDをフラットに集める（重複除去）
      const allRecIds = [...new Set(allUsersRecs.flatMap(user => user.recIds || []))];

      return res.status(200).json({ recIds: allRecIds });
    }

    // 自分のおすすめを取得（既存）
    if (req.method === 'GET' && req.query.userId) {
      const { userId } = req.query;
      const recCollection = database.collection('user_recommendations');
      const userRec = await recCollection.findOne({ userId });
      const recIds = userRec ? userRec.recIds || [] : [];
      return res.status(200).json({ recIds });
    }

    // POST（おすすめの追加/削除）
    if (req.method === 'POST') {
      const { userId, videoId, action } = req.body;
      if (!userId || !videoId || !action) {
        return res.status(400).json({ error: 'パラメータ不足' });
      }

      const recCollection = database.collection('user_recommendations');
      let userRec = await recCollection.findOne({ userId });
      if (!userRec) {
        userRec = { userId, recIds: [] };
        await recCollection.insertOne(userRec);
      }

      let recIds = userRec.recIds || [];

      if (action === 'add') {
        if (recIds.length >= 5) return res.status(400).json({ error: '最大5件までです' });
        if (!recIds.includes(videoId)) recIds.push(videoId);
      } else if (action === 'remove') {
        recIds = recIds.filter(id => id !== videoId);
      }

      await recCollection.updateOne({ userId }, { $set: { recIds } }, { upsert: true });
      return res.status(200).json({ success: true, recIds });
    }

    return res.status(405).json({ error: 'Method Not Allowed' });

  } catch (error) {
    console.error('Database Error:', error);
    res.status(500).json({ error: '接続エラー', details: error.message });
  }
}