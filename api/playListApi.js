import { MongoClient } from 'mongodb';

const uri = process.env.DB;
const client = new MongoClient(uri);

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    await client.connect();
    
    // デバッグ結果に基づき、正しいDB名「belmond_fan_data」を指定
    const database = client.db('belmond_fan_data'); 
    
    // コレクション名「channels」を指定
    const collection = database.collection('playlists');
    
    const data = await collection.findOne({});

    if (!data) {
      // もしここでも出ない場合は、channelsという名前が合っているか再確認
      return res.status(404).json({ 
        error: 'データが見つかりませんでした',
        info: 'DB: belmond_fan_data 内に playlists コレクションが見つからないか、空です。'
      });
    }

    res.status(200).json(data);
  } catch (error) {
    console.error('Database Error:', error);
    res.status(500).json({ error: '接続エラー', details: error.message });
  }
}