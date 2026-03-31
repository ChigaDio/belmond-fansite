// pages/api/quizAPI.js （Vercel用・完全修正版＋クイズ作成／編集機能）
import { MongoClient } from 'mongodb';

const uri = process.env.DB_COUNT;
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
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') return res.status(200).end();
    if (req.method === 'POST') req.body = await parseBody(req);

    const method = req.query.method;
    if (!method) return res.status(400).json({ error: 'Method parameter is required' });

    try {
        await client.connect();
        const db = client.db(DB_NAME);

        switch (method) {
            case 'registerUser':
                const { userId: regId } = req.body;
                const usersColl = db.collection('users');
                if (!(await usersColl.findOne({ userId: regId }))) {
                    await usersColl.insertOne({ userId: regId }).catch(() => {});
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
                    for (let i = 0; i < count; i++) data.push(all[Math.floor(Math.random() * all.length)]);
                }
                return res.status(200).json(data);

            case 'submitAnswers':
                const { answers: submitAnswersData } = req.body;
                const qColl = db.collection('questions');
                let correctCount = 0;
                const detailedResults = [];
                for (const ans of submitAnswersData) {
                    const q = await qColl.findOne({ id: ans.questionId });
                    if (!q) continue;
                    const correct = ans.selected === q.answers[q.correctIndex];
                    if (correct) correctCount++;
                    const newAttempts = (q.attempts || 0) + 1;
                    const newCorrects = (q.corrects || 0) + (correct ? 1 : 0);
                    const newRate = Math.round((newCorrects / newAttempts) * 100);
                    await qColl.updateOne({ id: ans.questionId }, { $set: { correctRate: newRate, attempts: newAttempts, corrects: newCorrects } }).catch(() => {});
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
                const { userId, name, score } = req.body;
                const scoresColl = db.collection('scores');
                await scoresColl.updateOne({ userId }, { $set: { name, score } }, { upsert: true }).catch(() => {});
                return res.status(200).json({ success: true });

            case 'getRanking':
                const ranking = await db.collection('scores')
                    .find({ score: { $exists: true, $ne: null } })
                    .sort({ score: -1 })
                    .limit(50)
                    .toArray();
                return res.status(200).json(ranking);

            // ====================== クイズ管理API ======================
            case 'getQuizzes':
                const page = parseInt(req.query.page) || 1;
                const limit = parseInt(req.query.limit) || 20;
                const search = (req.query.search || '').trim();
                const myOnly = req.query.myOnly === 'true';
                const uid = req.query.userId || '';
                const skip = (page - 1) * limit;

                const quizColl = db.collection('questions');

                let filter = {};
                if (myOnly && uid) filter.authorID = uid;
                if (search) {
                    const words = search.split(/\s+/).filter(w => w.length > 0);
                    if (words.length > 0) {
                        filter.$and = words.map(word => ({
                            questionText: { $regex: word, $options: 'i' }
                        }));
                    }
                }

                const totalCount = await quizColl.countDocuments(filter);
                const quizzes = await quizColl.find(filter)
                    .sort({ id: -1 })
                    .skip(skip)
                    .limit(limit)
                    .toArray();

                return res.status(200).json({
                    quizzes,
                    total: totalCount,
                    page,
                    limit,
                    totalPages: Math.ceil(totalCount / limit)
                });

            case 'createQuiz':
                const { questionText: createQuestionText, answers: createAnswers, correctIndex: createCorrectIndex, difficulty: createDifficulty, explanation: createExplanation, authorID: createAuthorID } = req.body;

                if (!createQuestionText || !Array.isArray(createAnswers) || createAnswers.length < 2 || createAnswers.length > 4 ||
                    createCorrectIndex === undefined || !['easy','medium','hard'].includes(createDifficulty) ||
                    !createExplanation || !createAuthorID) {
                    return res.status(400).json({ error: '入力データが不正です（選択肢2〜4個、正解選択必須）' });
                }

                const createColl = db.collection('questions');
                const maxDoc = await createColl.findOne({}, { sort: { id: -1 } });
                const newId = maxDoc ? maxDoc.id + 1 : 0;

                const newQuiz = {
                    id: newId,
                    questionText: createQuestionText.trim(),
                    answers: createAnswers.map(a => a.trim()),
                    correctIndex: parseInt(createCorrectIndex),
                    difficulty: createDifficulty,
                    explanation: createExplanation.trim(),
                    authorID: createAuthorID,
                    lastReset: new Date(),
                    attempts: 0,
                    corrects: 0,
                    correctRate: 0
                };

                await createColl.insertOne(newQuiz);
                return res.status(200).json({ success: true, id: newId });

            case 'updateQuiz':
                const { id: updateId, questionText: updateQuestionText, answers: updateAnswers, 
                        correctIndex: updateCorrectIndex, difficulty: updateDifficulty, 
                        explanation: updateExplanation, authorID: updateAuthorID } = req.body;

                // 【修正】id=0の場合も正しく扱う（!updateId ではなく明示的にチェック）
                if (updateId === undefined || updateId === null || 
                    !updateQuestionText || 
                    !Array.isArray(updateAnswers) || updateAnswers.length < 2 || updateAnswers.length > 4 ||
                    updateCorrectIndex === undefined || updateCorrectIndex === null ||
                    !['easy','medium','hard'].includes(updateDifficulty) ||
                    !updateExplanation || !updateAuthorID) {

                    return res.status(400).json({ 
                        error: '入力データが不正です（必須項目が不足または不正）',
                        received: {
                            id: updateId,
                            questionText: !!updateQuestionText,
                            answersLength: updateAnswers?.length,
                            correctIndex: updateCorrectIndex,
                            difficulty: updateDifficulty,
                            explanation: !!updateExplanation,
                            authorID: !!updateAuthorID
                        },
                        note: "id=0は正常な値です。!updateId ではなく明示的なチェックを使用"
                    });
                }

                const updateColl = db.collection('questions');
                const target = await updateColl.findOne({ id: parseInt(updateId) });

                if (!target || target.authorID !== updateAuthorID) {
                    return res.status(403).json({ error: '編集権限がありません（自分が作成したクイズのみ）' });
                }

                await updateColl.updateOne(
                    { id: parseInt(updateId) },
                    {
                        $set: {
                            questionText: updateQuestionText.trim(),
                            answers: updateAnswers.map(a => a.trim()),
                            correctIndex: parseInt(updateCorrectIndex),
                            difficulty: updateDifficulty,
                            explanation: updateExplanation.trim(),
                            lastReset: new Date()
                        }
                    }
                );

                return res.status(200).json({ success: true, id: parseInt(updateId) });

            case 'deleteQuiz':
                const { id: delId, userId: delUserId } = req.body;
                if (!delId || !delUserId) return res.status(400).json({ error: 'IDとユーザーIDが必要です' });

                const delColl = db.collection('questions');
                const targetQuiz = await delColl.findOne({ id: parseInt(delId) });

                if (!targetQuiz || targetQuiz.authorID !== delUserId) {
                    return res.status(403).json({ error: '削除権限がありません（自分が作成したクイズのみ）' });
                }

                await delColl.deleteOne({ id: parseInt(delId) });
                return res.status(200).json({ success: true });

            default:
                return res.status(404).json({ error: 'Invalid method' });
        }
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: err.message });
    }
}