import { MongoClient } from 'mongodb';

// 環境変数 DB には接続文字列「mongodb+srv://...」が入っている
const uri = process.env.DB; 
const db_name = process.env.DB_NAME; // 追加: DB名も環境変数から取得する場合
const client = new MongoClient(uri);

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    await client.connect();
    
    // 【重要】 .db() の中身は接続文字列ではなく「実際のDB名」を書く必要があります。
    // MongoDB Atlas の画面で確認したデータベース名（例: 'test' や 'belmondfansitedb'）を直接入力してください。
    const database = client.db(db_name); 
    
    const collection = database.collection('channnls');
    const data = await collection.findOne({});

    if (!data) {
      return res.status(404).json({ error: 'データが見つかりませんでした' });
    }

    res.status(200).json(data);
  } catch (error) {
    console.error('Database Error:', error);
    res.status(500).json({ error: 'データの取得に失敗しました', details: error.message });
  }
}