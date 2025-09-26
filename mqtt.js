// 라이브러리 임포트 //
const express = require('express');
const mqtt = require('mqtt');
const cors = require('cors');
const { Pool } = require('pg');
const redis = require('redis');
const promClient = require('prom-client');

// 상수 정의 //
const CONSTANTS = {
    MAX_MESSAGE_LENGTH: 1000,
    MAX_NICKNAME_LENGTH: 10,
    MIN_NICKNAME_LENGTH: 2,
    DEFAULT_MESSAGE_LIMIT: 50,
    MAX_MESSAGE_LIMIT: 100,
    NICKNAME_REGEX: /^[a-zA-Z0-9가-힣_-]+$/,
    MQTT_QOS: 1,
    DB_QUERY_TIMEOUT: 5000
};


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
app.use(cors({
    origin: process.env.ALLOWED_ORIGINS ? process.env.ALLOWED_ORIGINS.split(',') : '*',
    credentials: true
})); // 모든 외부 도메인에서 요청을 허용
app.use(express.json({ limit: '10mb' })); // API요청의 body가 json형태 일 경우, js객체로 자동변환
app.use(express.urlencoded({ extended: true }));

app.use((req, res, next) => {
    console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
    next();
})

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
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
    query_timeout: CONSTANTS.DB_QUERY_TIMEOUT
});

pool.on('connect', () => {
    console.log('데이터베이스에 새 연결이 생성되었습니다.');
})

pool.on('error', (err) => {
    console.error('데이터베이스 연결 풀 오류:', err);
});

// Rdis연결 설정 //
const redisClient = redis.createClient({
    socket: {
        host: REDIS_HOST,
        port: REDIS_PORT,
        reconnectStrategy: (retries) => {
            if (retries > 5) {
                console.error('Redis 재연결 시도 한계 초과');
                return new Error('Redis 연결 재시도 한계 초과');
            }
            return Math.min(retries * 100, 3000);
        }
    }
});

redisClient.on('error', (err) => {
    console.error('Redis 클라이언트 오류', err);
});

redisClient.on('connect', () => {
    console.log('Redis연결 성공');
});

redisClient.on('reconnecting', () => {
    console.log('Redis 재연결 중...');
});

const mqttClient = mqtt.connect(MQTT_BROKER_URL, {
    clientId: `archiver_${Math.random().toString(16).substr(2, 8)}`,
    clean: true,
    reconnectPeriod: 1000,
    keepalive: 60,
    connectTimeout: 30000
});

mqttClient.on('connect', () => {
    console.log('MQTT 브로커에 연결되었습니다.')

    mqttClient.subscribe(MQTT_TOPIC_PUBLIC, { qos: CONSTANTS.MQTT_QOS }, (err) => {
        if (err) {
            console.error(`${MQTT_TOPIC_PUBLIC} 채팅 토픽 구독 실패:`, err);
        }
        else {
            console.log(`${MQTT_TOPIC_PUBLIC} 채팅 토픽을 성공적으로 구독했습니다.`);
        }
    });

    mqttClient.subscribe(MQTT_TOPIC_REACTION, { qos: CONSTANTS.MQTT_QOS }, (err) => {
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
        const messageStr = message.toString();
        if (!messageStr.trim()) {
            console.warn('빈 메시지 수신', topic);
            return;
        }

        const payload = JSON.parse(messageStr);

        if (topic === MQTT_TOPIC_PUBLIC) {
            await handleChatMessage(payload);
            // messageCounter.inc();
        }
        else if (topic === MQTT_TOPIC_REACTION) {
            await handleReaction(payload);
            // reactionCounter.inc();
        }
    }
    catch (error) {
        if (error instanceof SyntaxError) {
            console.error(`JSON 파싱 오류 - Topic: ${topic}, Message: ${messageStr.substring(0, 100)}...`);
        }
        else {
            console.error(`${topic} 토픽 메시지 처리 중 오류 발생`, error);
        }
    }
}
);

mqttClient.on('reconnect', () => {
    console.log('MQTT 브로커 재연결 중...');
});

mqttClient.on('offline', () => {
    console.log('MQTT 브로커 연결 끊어짐');
});

mqttClient.on('error', (error) => {
    console.error('MQTT 연결 오류', error);
});

mqttClient.on('close', () => {
    console.log('MQTT 연결이 종료되었습니다.');
    // 클라이언트에서 api호출하는것으로 변경예정
});

function validateNickname(nickname) {
    if (!nickname || typeof nickname !== 'string') {
        return { valid: false, error: '닉네임은 문자열이어야 합니다.' };
    }

    const trimmed = nickname.trim();
    if (trimmed.length < CONSTANTS.MIN_NICKNAME_LENGTH) {
        return { valid: false, error: `닉네임은 최소 ${CONSTANTS.MIN_NICKNAME_LENGTH}자 이상이어야 합니다.` };
    }
    if (trimmed.length > CONSTANTS.MAX_NICKNAME_LENGTH) {
        return { valid: false, error: `닉네임은 ${CONSTANTS.MAX_NICKNAME_LENGTH}자를 초과할 수 없습니다.` };
    }

    if (!CONSTANTS.NICKNAME_REGEX.test(trimmed)) {
        return { valid: false, error: "닉네임에는 영문, 한글, 숫자, _, - 만 사용할 수 있습니다." };
    }

    return { valid: true, nickname: trimmed };
}

function validateMessage(content) {

    if (!content || typeof content !== 'string') {
        return { valid: false, error: '메시지 내용이 필요합니다.' };
    }

    const trimmed = content.trim();

    if (trimmed.length === 0) {
        return { valid: false, error: '빈 메시지는 보낼 수 없습니다.' };
    }

    if (trimmed.length > CONSTANTS.MAX_MESSAGE_LENGTH) {
        return { valid: false, error: `메시지는 ${CONSTANTS.MAX_MESSAGE_LENGTH}자를 초과할 수 없습니다.` };
    }

    return { valid: true, content: trimmed };
}

function validateMentions(mentions) {
    if (!mentions) return { valid: true, mentions: [] };

    if (!Array.isArray(mentions)) {
        return { valid: false, error: 'mentions는 배열이어야 합니다.' };
    }

    for (const mention of mentions) {
        if (typeof mention !== 'string') {
            return { valid: false, error: '모든 mention은 문자열이어야 합니다.' };
        }
    }

    return { valid: true, mentions };
}

/**
 * 채팅 메시지를 받아 데이터베이스에 저장하는 함수
 * @param {object} payload - 메시지 내용, 닉네임, 멘션 등이 담긴 객체
 */
async function handleChatMessage(payload) {
    const { nickname, content, mentions = [] } = payload;

    const nicknameValidation = validateNickname(nickname);
    if (!nicknameValidation.valid) {
        console.error('닉네임 검증 실패:', nicknameValidation.error, payload);
        return;
    }

    const contentValidation = validateMessage(content);
    if (!contentValidation.valid) {
        console.error('메시지 검증 실패:', contentValidation.error, payload);
        return;
    }

    const mentionsValidation = validateMentions(mentions);
    if (!mentionsValidation.valid) {
        console.error('멘션 검증 실패:', mentionsValidation.error, payload);
        return;
    }

    try {
        const query = `
        INSERT INTO messages (nickname, content, mentions)
        VALUES ($1, $2, $3)
        RETURNING id
        `;
        const values = [
            nicknameValidation.nickname,
            contentValidation.content,
            mentionsValidation.mentions
        ];

        const result = await pool.query(query, values);
        console.log(`메시지 저장 완료 (ID: ${result.rows[0].id}, 시간: ${result.rows[0].created_at})`);
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

    if (!messageId || !reactionType || !nickname) {
        console.error('필수 필드가 누락된 리액션:', payload);
        return;
    }

    if (typeof messageId !== 'number' && !Number.isInteger(parseInt(messageId))) {
        console.error('잘못된 messageId 형식: ', messageId);
        return;
    }

    if (typeof reactionType !== 'string' || reactionType.trim().length === 0) {
        console.error('잘못된 reactionType 형식', reactionType);
        return;
    }

    const nicknameValidation = validateNickname(nickname);
    if (!nicknameValidation.valid) {
        console.error('리액션에서 닉네임 검증 실패:', nicknameValidation.error);
        return;
    }

    try {
        const client = await pool.connect();
        try {
            await client.query('BEGIN');

            const getCurrentQuery = 'SELECT reactions FROM messages WHERE id = $1 FOR UPDATE';
            const currentResult = await client.query(getCurrentQuery, [messageId]);

            if (currentResult.rows.length === 0) {
                console.error(`ID가 ${messageId}인 메시지를 찾을 수 없습니다.`);
                await client.query('ROLLBACK');
                return;
            }

            let reactions = currentResult.rows[0].reactions || {};

            if (!reactions[reactionType]) {
                reactions[reactionType] = [];
            }

            const userIndex = reactions[reactionType].indexOf(nicknameValidation.nickname);
            if (userIndex === -1) {
                reactions[reactionType].push(nicknameValidation.nickname);
                console.log(`리액션 추가: ${nicknameValidation.nickname} -> ${reactionType}`);
            }
            else {
                reactions[reactionType].splice(userIndex, 1);
                if (reactions[reactionType].length === 0) {
                    delete reactions[reactionType];
                }
                console.log(`리액션 제거: ${nicknameValidation.nickname} -> ${reactionType}`);
            }

            const updateQuery = 'UPDATE messages SET reactions = $1 WHERE id = $2';
            await client.query(updateQuery, [JSON.stringify(reactions), messageId]);

            await client.query('COMMIT');
            console.log(`메시지(ID: ${messageId})의 리액션(${reactionType})이 업데이트되었습니다. (작업자:
                ${nicknameValidation.nickname})`);
        }
        catch (error) {
            await client.query('ROLLBACK');
            throw error;
        }
        finally {
            client.release();
        }
    }
    catch (error) {
        console.error('리액션 처리 중 오류 발생', error);
    }
}


/**
 * POST /api/join
 * 사용자가 채팅 참여를 요청할 때 닉네임 중복을 검사하고, 활성 사용자 목록에 추가합니다.
 */
app.post('/api/join', async (req, res) => {
    const { nickname } = req.body;

    const validation = validateNickname(nickname);
    if (!validation.valid) {
        return res.status(400).json({
            status: 'error',
            message: validation.error
        });
    }

    try {
        const isMember = await redisClient.sIsMember(REDIS_NICKNAME_KEY, validation.nickname);
        if (isMember) {
            return res.status(409).json({
                status: 'error',
                message: '이미 사용 중인 닉네임입니다.'
            });
        }

        await redisClient.sAdd(REDIS_NICKNAME_KEY, validation.nickname);
        const activeCount = await redisClient.sCard(REDIS_NICKNAME_KEY);
        // activeUsersGauge.set(activeCount);

        console.log(`사용자 '${validation.nickname}' 참여. 현재 활성 사용자: ${activeCount}명`);
        res.status(200).json({
            status: 'success',
            message: '닉네임 사용 가능합니다.',
            data: { nickname: validation.nickname, activeUsers: activeCount }
        });
    }
    catch (error) {
        console.error('닉네임 확인 중 오류 발생:', error);
        res.status(500).json({
            status: 'error',
            message: '서버 내부 오류가 발생했습니다.'
        });
    }
})

// POST /api/leave
app.post('/api/leave', async (req, res) => {
    const { nickname } = req.body;

    const validation = validateNickname(nickname);
    if (!validation.valid) {
        return res.status(400).json({
            status: 'error',
            message: validation.error
        });
    }

    try {
        const removed = await redisClient.sRem(REDIS_NICKNAME_KEY, validation.nickname);
        const activeCount = await redisClient.sCard(REDIS_NICKNAME_KEY);
        // activeUsersGauge.set(activeCount);

        if (removed === 0) {
            console.log(`사용자 '${validation.nickname}' 이미 목록에 없음`);
        }
        else {
            console.log(`사용자 '${validation.nickname}' 퇴장. 현재 활성 사용자: ${activeCount}명`);
        }

        res.status(200).json({
            status: 'success',
            message: '활성 사용자 목록에서 제거되었습니다.',
            data: { activeUsers: activeCount }
        });
    }
    catch (error) {
        console.error('사용자 제거 중 오류 발생:', error);
        res.status(500).json({
            status: 'error',
            message: '서버 내부 오류가 발생했습니다.'
        });
    }
});


// GET /api/messages
app.get('/api/messages', async (req, res) => {
    const limit = Math.min(
        parseInt(req.query.limit) || CONSTANTS.DEFAULT_MESSAGE_LIMIT,
        CONSTANTS.MAX_MESSAGE_LIMIT
    );

    try {
        const query = `
        SELECT id, nickname, content, created_at AS time, reactions, mentions
        FROM messages
        ORDER BY created_at DESC
        LIMIT $1
        `;
        const result = await pool.query(query, [limit]);

        const messages = result.rows.reverse();
        res.status(200).json({
            status: 'success',
            data: messages,
            meta: { count: messages.length, limit }
        });
    }

    catch (error) {
        console.error('메시지 조회 중 오류 발생:', error);
        res.status(500).json({
            status: 'error',
            message: '메시지 기록을 가져오는데 실패했습니다.'
        });
    }
});


// GET /api/active-users
app.get('/api/active-users', async (req, res) => {
    try {
        const activeUsers = await redisClient.sMembers(REDIS_NICKNAME_KEY);
        const count = activeUsers.length;

        res.status(200).json({
            status: 'success',
            data: activeUsers.sort(),
            meta: { count }
        });
    }
    catch (error) {
        console.error('활성 사용자 조회 중 오류 발생:', error);
        res.status(500).json({
            status: 'error',
            message: '활성 사용자 목록을 가져오는데 실패했습니다.'
        });
    }
});


// GET /metrics  (추후 작성)

// GET /health
app.get('/health', async (req, res) => { // DB, REDIS, MQTT연결확인
    const checks = {
        database: false,
        redis: false,
        mqtt: false
    };

    try {
        await redisClient.ping();
        checks.redis = true;
    }
    catch (e) {
        console.error('Redis 헬스체크 실패: ', e.message);
    }

    try {
        await pool.query('SELECT 1');
        checks.database = true;
    }
    catch (e) {
        console.error('DB 헬스체크 실패:', e.message);
    }

    checks.mqtt = mqttClient.connected;

    const isHealthy = Object.values(checks).every(Boolean);

    res.status(isHealthy ? 200 : 503).json({
        status: isHealthy ? 'healthy' : 'degraded',
        checks,
        timestamp: new Date().toISOString(),
        uptime: process.uptime()
    });
});

app.use('*', (req, res) => {
    res.status(404).json({
        status: 'error',
        message: '요청한 엔드포인트를 찾을 수 없습니다.',
        path: req.originalUrl
    });
});

app.use((error, req, res, next) => {
    console.error('전역 에러 핸들러:', error);
    res.status(500).json({
        status: 'error',
        message: '서버 내부 오류가 발생했습니다.',
        timestamp: new Date().toISOString()
    });
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
            CREATE INDEX IF NOT EXISTS idx_messages_nickname  ON messages(nickname);
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
    const ShutdownPromises = [];

    try {
        if (mqttClient.connected) {
            ShutdownPromises.push(
                new Promise((resolve) => {
                    mqttClient.end(false, {}, () => {
                        console.log('MQTT 연결이 종료되었습니다.');
                        resolve();
                    });
                })
            );
        }

        if (redisClient.isOpen) {
            ShutdownPromises.push(
                redisClient.quit().then(() => {
                    console.log('Redis 연결이 종료되었습니다.');
                })
            );
        }

        ShutdownPromises.push(
            pool.end().then(() => {
                console.log('데이터베이스 연결 풀이 종료되었습니다.');
            })
        );

        await Promise.race([
            Promise.all(ShutdownPromises),
            new Promise((_, reject) => {
                setTimeout(() => reject(new Error('종료 시간 초과')), 10000);
            })
        ]);

        console.log('모든 연결이 안전하게 종료되었습니다.');
        process.exit(0);
    }
    catch (error) {
        console.error('안전한 종료 중 오류 발생:', error);
        process.exit(1);
    }
}

process.on('SIGTERM', () => Shutdown('SIGTERM'));
process.on('SIGINT', () => Shutdown('SIGINT'));

process.on('unhandledRejection', (error, promise) => {
    console.error('처리되지 않은 Promise rejection:', error);
});

process.on('uncaughtException', (error) => {
    console.error('처리되지 않은 Promise rejection:', error);
    Shutdown('UNCAUGHT_EXCEPTION');
});

async function startServer() {
    try {
        await redisClient.connect();
        console.log('Redis 연결이 설정되었습니다.');

        await createDatabaseTable();

        const server = app.listen(PORT, () => {
            console.log(`메시지 아카이버 서버가 ${PORT}번 포트에서 실행 중입니다.`);
            console.log(`헬스 체크: http://localhost:${PORT}/health`);
            // console.log(`메트릭: http://localhost:${PORT}/metrics`);
            console.log(`환경: ${process.env.NODE_ENV || 'development'}`);
        });

        server.on('close', () => {
            console.log('HTTP 서버가 종료되었습니다.');
        });
    }
    catch (error) {
        console.error('서버 시작에 실패했습니다.', error);
        await Shutdown('STARTUP_ERROR');
    }
}

startServer();