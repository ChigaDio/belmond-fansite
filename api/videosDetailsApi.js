import { MongoClient } from 'mongodb';

const uri = process.env.DB;
const client = new MongoClient(uri);

export default async function handler(req, res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    if (req.method === 'OPTIONS') return res.status(200).end();

    const page = parseInt(req.query.page) || 1;
    const limit = 20;
    const skip = (page - 1) * limit;

    const search = req.query.search?.trim();
    const startDate = req.query.startDate;
    const endDate = req.query.endDate;
    const type = req.query.type;
    const sortBy = req.query.sortBy || 'publishedAt';
    const sortOrder = parseInt(req.query.sortOrder) || -1;

    try {
        await client.connect();
        const db = client.db('belmond_fan_data');
        const collection = db.collection('videos');

        // クエリ構築
        const query = {};
        if (search) query.title = { $regex: search, $options: 'i' };
        if (startDate || endDate) {
            query.published_at = {};
            if (startDate) query.published_at.$gte = new Date(startDate);
            if (endDate) query.published_at.$lte = new Date(endDate + 'T23:59:59.999Z');
        }
        if (type) query.content_category = type;

        const totalCount = await collection.countDocuments(query);
        const videos = await collection.find(query)
            .sort({ [sortBy]: sortOrder })
            .skip(skip)
            .limit(limit)
            .toArray();

        res.status(200).json({
            videos,
            currentPage: page,
            totalPages: Math.ceil(totalCount / limit),
            totalVideos: totalCount
        });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: err.message });
    }
}