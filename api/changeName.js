import { MongoClient } from 'mongodb';

const uri = process.env.DB;
const client = new MongoClient(uri);

export default async function handler(req, res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') return res.status(200).end();

    try {
        await client.connect();
        const db = client.db('belmond_fan_data');
        const rankingCollection = db.collection('quizRanking');   // ← QuizAPIと同じコレクションを使用（存在しなければ自動作成）

        const { userId, name } = req.body;

        if (!userId || !name) {
            return res.status(400).json({ error: 'userId と name は必須です' });
        }

        await rankingCollection.updateOne(
            { userId },
            { 
                $set: { 
                    name: name.trim(),
                    updatedAt: new Date()
                }
            },
            { upsert: true }
        );

        res.status(200).json({ success: true, message: '名前を変更しました' });
    } catch (error) {
        console.error('changeName Error:', error);
        res.status(500).json({ error: 'サーバーエラー', details: error.message });
    }
}