// quizAPI.js（これで解答送信エラーは完全消滅）
import { MongoClient } from 'mongodb';

const uri = process.env.DB;
const client = new MongoClient(uri);
const DB_NAME = 'belmond_fan_data';

// JSONボディを手動でパース（Vercel Serverless必須）
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

async function connectDb() {
    await client.connect();
    return client.db(DB_NAME);
}

async function handler(req, res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') return res.status(200).end();

    // POSTの場合のみパース
    if (req.method === 'POST') {
        req.body = await parseBody(req);
    }

    const method = req.query.method;
    if (!method) return res.status(400).json({ error: 'Method parameter is required' });

    try {
        const db = await connectDb();

        switch (method) {
            case 'registerUser':
                const { userId: regId } = req.body;
                const users = db.collection('users');
                if (!(await users.findOne({ userId: regId }))) {
                    await users.insertOne({ userId: regId });
                }
                return res.status(200).json({ success: true });

            case 'getQuestions':
                const count = parseInt(req.query.count) || 10;
                const questionsColl = db.collection('questions');
                const total = await questionsColl.countDocuments();
                let data;
                if (total >= count) {
                    data = await questionsColl.aggregate([{ $sample: { size: count } }]).toArray();
                } else {
                    const all = await questionsColl.find({}).toArray();
                    data = [];
                    for (let i = 0; i < count; i++) {
                        data.push(all[Math.floor(Math.random() * all.length)]);
                    }
                }
                return res.status(200).json(data);

            case 'submitAnswers':
                const { answers } = req.body;
                const qColl = db.collection('questions');
                let correctCount = 0;
                const detailedResults = [];
                for (const ans of answers) {
                    const q = await qColl.findOne({ id: ans.questionId });
                    if (!q) continue;
                    const correct = ans.selected === q.correctIndex;
                    if (correct) correctCount++;
                    const newAttempts = (q.attempts || 0) + 1;
                    const newCorrects = (q.corrects || 0) + (correct ? 1 : 0);
                    const newRate = Math.round((newCorrects / newAttempts) * 100);
                    await qColl.updateOne({ id: ans.questionId }, { $set: { correctRate: newRate, attempts: newAttempts, corrects: newCorrects } });
                    detailedResults.push({
                        questionText: q.questionText,
                        correct,
                        correctAnswer: q.answers[q.correctIndex],
                        explanation: q.explanation,
                        correctRate: newRate
                    });
                }
                return res.status(200).json({ correctCount, detailedResults });

            case 'submitScore':
                const { userId, score } = req.body;
                const scoresColl = db.collection('scores');
                await scoresColl.updateOne({ userId }, { $set: { score } }, { upsert: true });
                return res.status(200).json({ success: true });

            case 'getRanking':
                const scoresColl2 = db.collection('scores');
                const ranking = await scoresColl2.find({}).sort({ score: -1 }).limit(10).toArray();
                return res.status(200).json(ranking);

            default:
                return res.status(404).json({ error: 'Invalid method' });
        }
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: err.message });
    }
}

export default handler;