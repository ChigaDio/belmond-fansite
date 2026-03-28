// pages/api/userApi.js
import { MongoClient } from 'mongodb';

const uri = process.env.DB_COUNT;
const client = new MongoClient(uri);

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With');

  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    await client.connect();
    const db = client.db('belmond_fan_data');

    // 元のchannels取得
    if (req.method === 'GET' && !req.query.userId && !req.query.allUsersRecs) {
      const collection = db.collection('channels');
      const data = await collection.findOne({});
      if (!data) return res.status(404).json({ error: 'データなし' });
      return res.status(200).json(data);
    }

    // 全ユーザーのおすすめ一覧（ページング付き）
    if (req.method === 'GET' && req.query.allUsersRecs === 'true') {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const skip = (page - 1) * limit;

      const recCollection = db.collection('user_recommendations');
      
      // おすすめを登録しているユーザーのみカウント
      const totalUsers = await recCollection.countDocuments({ 
        recIds: { $exists: true, $ne: [] } 
      });

      const users = await recCollection.find({ 
        recIds: { $exists: true, $ne: [] } 
      })
        .skip(skip)
        .limit(limit)
        .toArray();

      return res.status(200).json({
        users: users.map(u => ({
          userId: u.userId,
          name: u.name || u.userId.slice(0, 8) + '...',
          recCount: u.recIds ? u.recIds.length : 0
        })),
        totalUsers,
        totalPages: Math.ceil(totalUsers / limit),
        currentPage: page,
        limit: limit
      });
    }

    // 既存の自分のおすすめ取得
    if (req.method === 'GET' && req.query.userId) {
      const { userId } = req.query;
      const recCollection = db.collection('user_recommendations');
      const userRec = await recCollection.findOne({ userId });
      const recIds = userRec ? userRec.recIds || [] : [];
      return res.status(200).json({ recIds });
    }

    // POST（追加/削除）
    if (req.method === 'POST') {
      const { userId, videoId, action } = req.body;
      if (!userId || !videoId || !action) {
        return res.status(400).json({ error: 'パラメータ不足' });
      }

      const recCollection = db.collection('user_recommendations');
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