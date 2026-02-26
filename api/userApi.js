import { MongoClient } from 'mongodb';

const user = process.env.DB_USER;
const password = process.env.DB_PASSWORD;
const dbName = process.env.DB;

const uri = `mongodb+srv://${user}:${password}@belmondfansitedb.vxxqgmv.mongodb.net/${dbName}?retryWrites=true&w=majority`;
const client = new MongoClient(uri);

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  try {
    await client.connect();
    const database = client.db(dbName);
    const collection = database.collection('channnls'); // コレクション名: channnls
    
    // findOne() を使うことで、最初の1件をオブジェクトとして取得
    const data = await collection.findOne({});

    if (!data) {
      return res.status(404).json({ error: 'データが見つかりませんでした' });
    }

    // 配列ではなく { ... } の形で返されます
    res.status(200).json(data);
  } catch (error) {
    console.error('Database Error:', error);
    res.status(500).json({ error: 'データの取得に失敗しました', details: error.message });
  }
}