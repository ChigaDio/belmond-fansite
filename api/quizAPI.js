// quizAPI.js (Vercelのapi/quizAPI.jsに配置)

import { MongoClient } from 'mongodb';

const uri = process.env.DB;
const client = new MongoClient(uri);
const DB_NAME = 'belmond_fan_data';

async function connectDb() {
  await client.connect();
  return client.db(DB_NAME);
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  // 必要に応じて追加ヘッダ
  // res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();

  const method = req.query.method;

  if (!method) {
    return res.status(400).json({ error: 'Method parameter is required' });
  }

  try {
    const db = await connectDb();

    switch (method) {
      case 'registerUser': {
        if (req.method !== 'POST') return res.status(405).end();
        const { userId } = req.body;
        const usersColl = db.collection('users');
        const existing = await usersColl.findOne({ userId });
        if (!existing) {
          await usersColl.insertOne({ userId });
        }
        return res.status(200).json({ success: true });
      }

      case 'getQuestions': {
        if (req.method !== 'GET') return res.status(405).end();
        const count = parseInt(req.query.count) || 10;
        const questionsColl = db.collection('questions');
        const total = await questionsColl.countDocuments();
        let out;
        if (total >= count) {
          out = await questionsColl
            .aggregate([{ $sample: { size: count } }])
            .toArray();
        } else {
          const allQuestions = await questionsColl.find({}).toArray();
          out = [];
          for (let i = 0; i < count; i++) {
            const randomIndex = Math.floor(Math.random() * allQuestions.length);
            out.push(allQuestions[randomIndex]);
          }
        }
        return res.status(200).json(out);
      }

      case 'submitAnswers': {
        if (req.method !== 'POST') return res.status(405).end();
        const { answers } = req.body; // [{questionId, selected}]
        const questionsColl = db.collection('questions');
        let correctCount = 0;
        const detailedResults = [];
        for (const ans of answers) {
          const q = await questionsColl.findOne({ id: ans.questionId });
          const correct = ans.selected === q.correctIndex;
          if (correct) correctCount++;
          const newAttempts = (q.attempts || 0) + 1;
          const newCorrects = (q.corrects || 0) + (correct ? 1 : 0);
          const newRate = Math.round((newCorrects / newAttempts) * 100);
          await questionsColl.updateOne(
            { id: ans.questionId },
            {
              $set: {
                correctRate: newRate,
                attempts: newAttempts,
                corrects: newCorrects
              }
            }
          );
          detailedResults.push({
            questionText: q.questionText,
            correct,
            correctAnswer: q.answers[q.correctIndex],
            explanation: q.explanation,
            correctRate: newRate
          });
        }
        return res.status(200).json({ correctCount, detailedResults });
      }

      case 'submitScore': {
        if (req.method !== 'POST') return res.status(405).end();
        const { userId, score } = req.body;
        const scoresColl = db.collection('scores');
        await scoresColl.updateOne(
          { userId },
          { $set: { score } },
          { upsert: true }
        );
        return res.status(200).json({ success: true });
      }

      case 'getRanking': {
        if (req.method !== 'GET') return res.status(405).end();
        const scoresColl = db.collection('scores');
        const ranking = await scoresColl
          .find({})
          .sort({ score: -1 })
          .limit(10)
          .toArray();
        return res.status(200).json(ranking);
      }

      default:
        return res.status(404).json({ error: 'Invalid method' });
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    await client.close();
  }
}

export default handler;