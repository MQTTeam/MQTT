// 라이브러리 임포트 //
const express = require('express');
const mqtt = require('mqtt');
const cors = require('cors');
const { Pool } = require('pg');
const redis = require('redis');
const promClient = require('prom-client');

// 애플리케이션 설정값 설정 //
// DB 설정 //
const PORT = process.env.PORT || 3000; // 서버실행 포트 지정, 지정하지 않으면 3000번 포트사용
const DB_USER = process.env.DB_USER || 'postgres';
const DB_HOST = process.env.DB_HOST || 'localhost' // DB서버 IP 지정
const DB_NAME = process.env.DB_NAME || 'chatdb';
const DB_PASSWORD = process.env.DB_PASSWORD || 'password';
const DB_PORT = process.env.DB_PORT || 5432;
// REDIS 설정 //
const REDIS_HOST = process.env.REDIS_HOST || 'localhost'; // REDIS서버 IP 지정
const REDIS_PORT = process.env.REDIS_PORT || 6379;
const REDIS_NICKNAME_KEY = process.env.REDIS_NICKNAME_KEY || 'active_nicknames';
// MQTT 설정 //
const MQTT_BROKER_URL = process.env.MQTT_BROKER_URL || 'mqtt://localhost:1883'
const MQTT_TOPIC_PUBLIC = process.env.MQTT_TOPIC_PUBLIC || 'k8s-chat/public'
const MQTT_TOPIC_REACTION = process.env.MQTT_TOPIC_REACTION || 'k8s-chat/reaction'


const app = express(); // Express를 이용해 웹 서버 객체를 생성

// 미들웨어 설정 //
app.use(cors()); // 모든 외부 도메인에서 요청을 허용
app.use(express.json()); // API요청의 body가 json형태 일 경우, js객체로 자동변환

// 프로메테우스 메트릭 설정 // 
/* const register = new promClient.Registry();
promClient.collectDefaultMetrics({ register });

const messageCounter = new promClient.Counter({
    name: 'total_chat_messages',
    help: '처리된 총 채팅 메시지 수',
    registers: [register]
});

const reactionCounter = new promClient.Counter({
    name: 'total_chat_reactions',
    help: '처리된 총 리액션 수',
    registers: [register]
});

const activeUsersGauge = new promClient.Gauge({
    name: 'active_chat_users',
    help: '현재 활성 사용자 수',
    registers: [register]
}); */

// DB연결 설정(PostgreSQL) //
const pool = new Pool({
    user: DB_USER,
    host: DB_HOST,
    database: DB_NAME,
    password: DB_PASSWORD,
    port: DB_PORT,
});

// Rdis연결 설정 //
const redisClient = redis.createClient({
    socket: {
        host: REDIS_HOST,
        port: REDIS_PORT,
    }
});
redisClient.on('error', (err) => {
    console.error('Redis 클라이언트 오류', err);
});
redisClient.on('connect', () => {
    console.log('Redis연결 성공');
});

const mqttClient = mqtt.connect(MQTT_BROKER_URL, {
    clientID: `archiver_${Math.random().toString(16).substr(2, 8)}`,
    clean: true,
    reconnectPeriod: 1000,
});

mqttClient.on('connect', () => {
    console.log('MQTT 브로커에 연결되었습니다.')

    mqttClient.subscribe(MQTT_TOPIC_PUBLIC, { qos: 1 }, (err) => {
        if (err) {
            console.error(`${MQTT_TOPIC_PUBLIC} 채팅 토픽 구독 실패:`, err);
        }
        else {
            console.log(`${MQTT_TOPIC_PUBLIC} 채팅 토픽을 성공적으로 구독했습니다.`);
        }
    });

    mqttClient.subscribe(MQTT_TOPIC_REACTION, { qos: 1 }, (err) => {
        if (err) {
            console.error(`${MQTT_TOPIC_REACTION} 리액션 토픽 구독 실패:`, err);
        }
        else {
            console.log(`${MQTT_TOPIC_REACTION} 리액션 토픽을 성공적으로 구독했습니다.`);
        }
    });
});

mqttClient.on('message', async (topic, message) => {
    try {
        const payload = JSON.parse(message.toString());
        if (topic === MQTT_TOPIC_PUBLIC) {
            await handleChatMessage(payload);
            // messageCounter.inc(); // 메트릭 카운터 증가
        }
        else if (topic === MQTT_TOPIC_REACTION) {
            await handleReaction(payload);
            // reactionCounter.inc(); // 메트릭 카운터 증가
        }
    }
    catch (error) {
        console.error(`${topic} 토픽 메시지 처리 중 오류 발생:`, error);
    }
});

mqttClient.on('close', () => {
    console.log('MQTT 연결이 종료되었습니다.');
    // 클라이언트에서 api호출하는것으로 변경예정
});


/**
 * 채팅 메시지를 받아 데이터베이스에 저장하는 함수
 * @param {object} payload - 메시지 내용, 닉네임, 멘션 등이 담긴 객체
 */
async function handleChatMessage(payload) {
    const { nickname, content, mentions = [] } = payload;
    try {
        const query = `
        INSERT INTO messages (nickname, content, mentions)
        VALUES ($1, $2, $3)
        RETURNING id
        `;
        const values = [nickname, content, mentions];
        const result = await pool.query(query, values);
        console.log(`메시지 저장 완료 (ID: ${result.rows[0].id})`);
    }
    catch (error) {
        console.error('메시지 저장 중 오류 발생:', error);
    }
}


/**
 * 리액션 이벤트를 받아 데이터베이스를 업데이트하는 함수
 * @param {object} payload - 메시지ID, 리액션 종류, 닉네임이 담긴 객체
 */
async function handleReaction(payload) {
    const { messageId, reactionType, nickname } = payload;
    try {
        const getCurrentQuery = 'SELECT reactions FROM messages WHERE id = $1';
        const currentResult = await pool.query(getCurrentQuery, [messageId]);

        if (currentResult.rows.length === 0) {
            console.error(`ID가 ${messageId}인 메시지를 찾을 수 없습니다.`);
            return;
        }

        let reactions = currentResult.rows[0].reactions || {};

        if (!reactions[reactionType]) {
            reactions[reactionType] = [];
        }

        const userIndex = reactions[reactionType].indexOf(nickname);
        if (userIndex === -1) {
            reactions[reactionType].push(nickname);
        }
        else {
            reactions[reactionType].splice(userIndex, 1);
        }

        const updateQuery = 'UPDATE messages SET reactions = $1 WHERE id = $2';
        await pool.query(updateQuery, [JSON.stringify(reactions), messageId]);

        console.log(`메시지(ID: ${messageId})의 리액션(${reactionType})이 업데이트 되었습니다. (작업자: ${nickname})`);
    }

    catch (error) {
        console.error('리액션 처리 중 오류 발생:', error);
    }
}


/**
 * POST /api/join
 * 사용자가 채팅 참여를 요청할 때 닉네임 중복을 검사하고, 활성 사용자 목록에 추가합니다.
 */
app.post('/api/join', async (req, res) => {
    const { nickname } = req.body;

    if (!nickname || typeof nickname !== 'string' || nickname.trim.length() === 0) {
        return res.status(400).json({ status: 'error', message: '올바른 닉네임이 필요합니다.' });
    }

    const trimmedNickname = nickname.trim();
    if (trimmedNickname.length > 10) {
        return res.status(400).json({ status: 'error', message: '닉네임은 10자를 초과할 수 없습니다. ' });
    }
    try {
        const isMember = await redisClient.sIsMember(REDIS_NICKNAME_KEY, trimmedNickname);
        if (isMember) {
            return res.status(409).json({ status: 'error', message: '이미 사용 중인 닉네임입니다.' });
        }

        await redisClient.sAdd(REDIS_NICKNAME_KEY, trimmedNickname);
        const activeCount = await redisClient.sCard(REDIS_NICKNAME_KEY);
        // activeUsersGauge.set(activeCount); // 메트릭 업데이트

        console.log(`사용자 '${trimmedNickname}' 참여. 현재 활성 사용자: ${activeCount}명`);
        res.status(200).json({ status: 'success', message: '닉네임 사용 가능합니다.' });
    }
    catch (error) {
        console.error('닉네임 확인 중 오류 발생:', error);
        res.status(500).json({ status: 'error', message: '서버 내부 오류가 발생했습니다.' });
    }
});

// POST /api/leave
app.post('/api/leave', async (req, res) => {
    const { nickname } = req.body;
    if (!nickname || typeof nickname !== 'string') {
        return res.status(400).json({ status: 'error', message: '올바른 닉네임을 설정하세요.' });
    }

    try {
        await redisClient.sRem(REDIS_NICKNAME_KEY, nickname.trim());
        const activeCount = await redisClient.sCard(REDIS_NICKNAME_KEY);
        // activeUsersGauge.set(activeCount); // 메트릭 업데이트

        console.log(`사용자 '${nickname}' 퇴장. 현재 활성 사용자: ${activeCount}명`);
        res.status(200).json({ status: 'success', message: '활성 사용자 목록에서 제거되었습니다.' });
    }

    catch (error) {
        console.error('사용자 제거 중 오류 발생:', error);
        res.status(500).json({ status: 'error', message: '서버 내부 오류가 발생했습니다.' });
    }
});


// GET /api/messages
app.get('/api/messages', async (req, res) => {
    const limit = parseInt(req.query.limit) || 50;

    try {
        const query = `
        SELECT id, nickname, content, created_at AS time, reactions, mentions
        FROM messages
        ORDER BY created_at DESC
        LIMIT $1
        `;
        const result = await pool.query(query, [limit]);

        const messages = result.rows.reverse();
        res.status(200).json({ status: 'success', data: messages });
    }

    catch (error) {
        console.error('메시지 조회 중 오류 발생:', error);
        res.status(500).json({ status: 'error', message: '메시지 기록을 가져오는데 실패했습니다.' });
    }
});


// GET /api/active-users
app.get('/api/active-users', async (req, res) => {
    try {
        const activeUsers = await redisClient.sMembers(REDIS_NICKNAME_KEY);
        res.status(200).json({ status: 'success', data: activeUsers });
    }

    catch (error) {
        console.error('활성 사용자 조회 중 오류 발생:', error);
        res.status(500).json({ status: 'error', message: '활성 사용자 목록을 가져오는데 실패했습니다.' });
    }
});


// GET /metrics  (추후 작성)

// GET /health
app.get('/health', async (req, res) => { // DB, REDIS, MQTT연결확인
    try {
        await pool.query('SELECT 1');
        await redisClient.ping();
        if (!mqttClient.connected) {
            throw new Error('MQTT 브로커에 연결되지 않았습니다.');
        }
        res.status(200).json({
            stauts: 'healthy',
            database: 'connected',
            redis: 'connected',
            mqtt: 'connected',
            timestamp: new Date().toISOString()
        });
    }
    catch (error) {
        console.error('health 체크 실패', error.message);
        res.status(500).json({
            status: 'unhealthy',
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
});

async function createDatabaseTable() {
    try {
        const createTableQuery = `
            CREATE TABLE IF NOT EXISTS messages (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                nickname VARCHAR(50) NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                reactions JSONB DEFAULT '{}'::jsonb,
                mentions VARCHAR(50)[] DEFAULT ARRAY[]::VARCHAR[]
            );
            CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at DESC);
    `;
        await pool.query(createTableQuery);
        console.log('데이터베이스 스키마가 성공적으로 초기화되었습니다.');
    }
    catch (error) {
        console.error('데이터베이스 초기화 중 오류 발생:', error);
        throw error;
    }
}

async function Shutdown(signal) {
    console.log(`${signal} 신호를 수신했습니다. 안전하게 종료합니다.`);
    try {
        if (mqttClient.connected) {
            await new Promise((resolve) => mqttClient.end(false, {}, resolve));
            console.log('MQTT 연결이 종료되었습니다.');
        }
        if (redisClient.isOpen) {
            await redisClient.quit();
            console.log('Redis 연결이 종료되었습니다.');
        }
        await pool.end();
        console.log('데이터베이스 연결 풀이 종료되었습니다.');
        process.exit(0);
    }
    catch (error) {
        console.error('안전한 종류 중 오류 발생:', error);
        process.exit(1);
    }
}
process.on('SIGTERM', () => Shutdown('SIGTERM'));
process.on('SIGINT', () => Shutdown('SIGINT'));

async function startServer() {
    try {
        await redisClient.connect();
        await createDatabaseTable();
        app.listen(PORT, () => {
            console.log(`메시지 아카이버 서버가 ${PORT}번 포트에서 실행 중입니다.`);
            console.log(`헬스 체크: http://localhost:${PORT}/health`);
            console.log(`메트릭: http://localhost:${PORT}/metrics`);
        });
    }
    catch (error) {
        console.error('서버 시작에 실패했습니다:', error);
        process.exit(1);
    }
}

startServer();