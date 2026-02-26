import { MongoClient } from 'mongodb';

const uri = process.env.DB;
const client = new MongoClient(uri);

export default async function handler(req, res) {
  try {
    await client.connect();
    
    // 1. 全てのデータベース名をリストアップ
    const adminDb = client.db().admin();
    const dbs = await adminDb.listDatabases();
    const dbNames = dbs.databases.map(d => d.name);

    // 2. 特定のDB（BelmondFanSiteDB）の中にあるコレクション名をリストアップ
    const targetDb = client.db('BelmondFanSiteDB');
    const collections = await targetDb.listCollections().toArray();
    const collectionNames = collections.map(c => c.name);

    // 3. 実際に中身を1件だけ検索
    const collection = targetDb.collection('channels');
    const data = await collection.findOne({});

    res.status(200).json({
      message: "Debug Info",
      available_databases: dbNames,         // あなたのクラスター内にある全DB名
      target_db_collections: collectionNames, // BelmondFanSiteDB内にある全コレクション名
      found_data: data                       // 見つかったデータ（nullなら空）
    });

  } catch (error) {
    res.status(500).json({ error: error.message });
  }
}