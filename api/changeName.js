import { MongoClient } from 'mongodb';

const uri = process.env.DB_COUNT;   // quizAPIと同じ環境変数を使用
const client = new MongoClient(uri);
const DB_NAME = 'belmond_fan_data';

async function parseBody(req) {
    return new Promise((resolve) => {
        let data = '';
        req.on('data', chunk => data += chunk);
        req.on('end', () => {
            try { resolve(data ? JSON.parse(data) : {}); }
            catch { resolve({}); }
        });
    });
}

export default async function handler(req, res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') return res.status(200).end();
    if (req.method === 'POST') req.body = await parseBody(req);

    try {
        await client.connect();
        const db = client.db(DB_NAME);

        const { userId, name } = req.body;

        if (!userId || !name) {
            return res.status(400).json({ error: 'userId と name は必須です' });
        }


        const trimmedName = name.trim();

        // users コレクションにも名前を保存（将来の拡張に備えて）
        const usersColl = db.collection('users');

        //存在チェック
        const existingUser = await usersColl.findOne({ name: trimmedName });
        if (existingUser) {
            return res.status(400).json({ error: 'その名前は既に使用されています' });
        }

        await usersColl.updateOne(
            { userId },
            { $set: { name: trimmedName, updatedAt: new Date() } },
            { upsert: true }
        );

        // scores コレクション（ランキング用）に名前を保存
        const scoresColl = db.collection('scores');
        await scoresColl.updateOne(
            { userId },
            { $set: { name: trimmedName, updatedAt: new Date() } },
            { upsert: true }
        );

        const user_recommendations = db.collection('user_recommendations');
        await user_recommendations.updateOne(
            { userId },
            { $set: { userName: trimmedName, updatedAt: new Date() } },
            { upsert: true }
        );

        return res.status(200).json({ success: true, message: '名前を変更しました' });
    } catch (error) {
        console.error('changeName Error:', error);
        return res.status(500).json({ error: 'サーバーエラー', details: error.message });
    }
}