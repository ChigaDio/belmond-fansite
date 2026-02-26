import { MongoClient } from 'mongodb';

const uri = process.env.DB;
const client = new MongoClient(uri);

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  if (req.method === 'OPTIONS') return res.status(200).end();

  // 1. クエリパラメータからページ番号を取得（デフォルトは1ページ目）
  const page = parseInt(req.query.page) || 1;
  const limit = 20; // 1ページあたりの件数
  const skip = (page - 1) * limit;

  try {
    await client.connect();
    const database = client.db('belmond_fan_data'); 
    const collection = database.collection('videos');
    
    // 2. 全体の件数を取得（ページボタンを作るのに必要）
    const totalCount = await collection.countDocuments({});

    // 3. データを取得（新しい順に並べる場合は .sort({ publishedAt: -1 }) などを追加）
    const data = await collection.find({})
      .sort({ publishedAt: -1 }) // 新しい動画を上にする（フィールド名はDBに合わせて）
      .skip(skip)
      .limit(limit)
      .toArray();

    res.status(200).json({
      videos: data,
      currentPage: page,
      totalPages: Math.ceil(totalCount / limit),
      totalVideos: totalCount
    });

  } catch (error) {
    res.status(500).json({ error: '接続エラー', details: error.message });
  }
}