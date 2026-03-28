// pages/api/userRecsApi.js
import { MongoClient } from 'mongodb';

const uri = process.env.DB_COUNT;   // あなたの環境に合わせて
const client = new MongoClient(uri);

export default async function handler(req, res) {
  // ==================== 元のCORS設定（いじらずに残す） ====================
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // ==================== ここから userRecsApi の処理を追加 ====================
  // POSTメソッドも許可するように拡張


  try {
    await client.connect();
    const database = client.db('belmond_fan_data');

    // ====================== 元の処理（userApi.js の部分） ======================
    if (req.method === 'GET' && !req.query.action) {   // actionパラメータがない場合は元のchannels取得
      const collection = database.collection('channels');
      const data = await collection.findOne({});

      if (!data) {
        return res.status(404).json({
          error: 'データが見つかりませんでした',
          info: 'DB: belmond_fan_data 内に channels コレクションが見つからないか、空です。'
        });
      }
      return res.status(200).json(data);
    }

    // ====================== ここから新規：ユーザーおすすめ処理 ======================
    const collection = database.collection('user_recommendations');

    if (req.method === 'GET') {
      const { userId } = req.query;
      if (!userId) return res.status(400).json({ error: 'userIdが必要です' });

      const userRec = await collection.findOne({ userId });
      const recIds = userRec ? userRec.recIds || [] : [];
      return res.status(200).json({ recIds });
    }

    if (req.method === 'POST') {
      const { userId, videoId, action } = req.body;

      if (!userId || !videoId || !action) {
        return res.status(400).json({ error: 'パラメータ不足です（userId, videoId, actionが必要）' });
      }

      let userRec = await collection.findOne({ userId });
      if (!userRec) {
        userRec = { userId, recIds: [] };
        await collection.insertOne(userRec);
      }

      let recIds = userRec.recIds || [];

      if (action === 'add') {
        if (recIds.length >= 5) {
          return res.status(400).json({ error: '最大5件までです' });
        }
        if (!recIds.includes(videoId)) {
          recIds.push(videoId);
        }
      } else if (action === 'remove') {
        recIds = recIds.filter(id => id !== videoId);
      }

      await collection.updateOne({ userId }, { $set: { recIds } }, { upsert: true });

      return res.status(200).json({ success: true, recIds });
    }

    return res.status(405).json({ error: 'Method Not Allowed' });

  } catch (error) {
    console.error('Database Error:', error);
    res.status(500).json({ error: '接続エラー', details: error.message });
  }
}