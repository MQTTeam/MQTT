// 라이브러리 임포트 //
const express = require('express');
const mqtt = require('mqtt');
const cors = require('cors');
// const { Pool } = require('pg');
// const redis = require('redis');
// const promClinet = require('prom-client');

const PORT = process.env.PORT || 3000; // 서버실행 포트 지정, 지정하지 않으면 3000번 포트사용


/*
const DB_USER = process.env.DB_USER || 'postgres';
const DB_HOST = process.env.DB_HOST || 'localhost'
const DB_NAME= process.env.DB_NAME || 'chatdb';
const DB_PASSWORD = process.env.DB_PASSWORD || 'password';
const DB_PORT = process.env.DB_PORT || 5432;

const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = process.env.REDIS_PORT || 6379;
const REDIS_NICKNAME_KEY = process.env.REDIS_NICKNAME_KEY || 'active_nicknames';
*/


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

const activeUsersGauge = new promCilent.Gauge({
    name: 'active_chat_users',
    help: '현재 활성 사용자 수',
    registers: [register]
}); */

// DB연결 설정(PostgreSQL) //
/* const pool = new Pool({
    user: DB_USER,
    host: DB_HOST,
    database: DB_NAME,
    password: DB_PASSWORD,
    port: DB_PORT,
}); */

// Rdis연결 설정 //
/* const redisClient = redis.createClient({
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
}); */

// MQTT 설정 //
const MQTT_BROKER_URL = process.env.MQTT_BROKER_URL || 'mqtt://localhost:1883'
const MQTT_TOPIC_PUBLIC = process.env.MQTT_TOPIC_PUBLIC || 'k8s-chat/public'
const MQTT_TOPIC_REACTION = process.env.MQTT_TOPIC_REACTION || 'k8s-chat/reaction'

const app = express(); // Express를 이용해 웹 서버 객체를 생성
app.use(cors()); // 모든 외부 도메인에서 요청을 허용
app.use(express.json()); // API요청의 body가 json형태 일 경우, js객체로 자동변환


// MQTT 브로커 연결 //
const mqttClient = mqtt.connect(MQTT_BROKER_URL, { //클라이언트 ID 중복방지, 매번 다른 ID로 실행
    clientid: `test-archiver_${Math.random().toString(16).substr(2, 8)}`,
    clean: true, // 접속 시 이전 세션정보를 모두 지움
    reconnectPeriod: 1000, // 연결이 끊어지면 1초마다 자동 재연결 설정
});

mqttClient.on('connect', () => { // connect이벤트 실행 (MQTT브로커와 연결됐을 시 1번 호출)
    console.log('MQTT 브로커에 연결되었습니다.');

    // 채팅방(토픽)을 qos option = 1 (메시지 최소 한번 전달 보장)을 사용하여 구독.
    mqttClient.subscribe([MQTT_TOPIC_PUBLIC, MQTT_TOPIC_REACTION], { qos: 1 }, (err) => {
        if (err) {
            console.error(`토픽 구독 실패`, err);
        }
        else {
            console.log(`'${MQTT_TOPIC_PUBLIC}'와 '${MQTT_TOPIC_REACTION}'구독.`);
        }
    });
});


mqttClient.on('message', async (topic, message) => { // message이벤트 실행, 구독한 토픽에 새 메시지가 도착할때 마다 호출
    console.log(`[MQTT 메시지 수신] 토픽: ${topic}, 내용: ${message.toString()}`); // 프론트엔드에서 보낸 메시지가 MQTT 브로커를 거쳐 잘 도착했는지 확인용도
});


mqttClient.on('close', () => { // close이벤트 실행, MQTT브로커와 연결이 끊겼을 때 호출
    console.log('MQTT 연결이 종료되었습니다.');
});

mqttClient.on('error', (err) => { //error이벤트 실행, MQTT 연결 중 에러가 발생했을 때 호출
    console.error('MQTT 연결 오류: ', err);
})


/* 프론트엔드가 호출하는 API 목록, 프론트엔드와의 테스트서버이기 때문에 실제 DB연동 대신 가짜 데이터 반환 */


const mockMessage = [{ // 가짜 메시지
    id: 1, nickname: 'admin', content: 'MQTTEAM 채팅에 서버에 오신 것을 환영합니다.',
    created_at: new Date().toISOString(), reactions: {}, mentions: []
}];

app.post('/api/join', (req, res) => { // 가짜 닉네임 중복 검사 요청 API (실제 검사로직없이 항상 사용가능 응답)
    const { nickname } = req.body;
    console.log(`[API Mock] /api/join 요청 수신 (nickname: ${nickname})`);
    res.status(200).json({ status: 'success', message: '닉네임 사용 가능! (테스트)' })
})

app.post('/api/leave', async (req, res) => { // 사용자의 퇴장을 알리는 API (항상 성공응답을 보냄)
    const { nickname } = req.body;
    console.log(`[API Mock] /api/leave 요청 수신 (nicknae: ${nickname})`);
    res.status(200).json({ status: 'success', message: '사용자 퇴장 처리 (테스트)' });
});

app.get('/api/messages', async (req, res) => { // 이전 대화 기록을 요청받는 API (미리 만들어 둔 mockMessages데이터 반환)
    console.log(`[API Mock] /api/messages 요청 수신`);
    res.status(200).json({ status: 'success', data: mockMessage });
});

app.get('/api/active-users', async (req, res) => { // 현재 접속자 목록을 요청받는 API. (가짜 사용자 목록 반환)
    console.log(`[API Mock] /api/active-users 요청 수신`);
    res.status(200).json({ status: 'success', data: ['admin', 'user'] });
});

app.get(' /health', async (req, res) => { // 서버가 살아있는지 확인하는 API (MQTT 브로커와의 연결상태만 확인하여 응답)
    const mqttStatus = mqttClient.connected;
    if (mqttStatus) {
        res.status(200).json({ status: 'healthy', mqtt: 'connected' });
    }
    else {
        res.status(500).json({ statis: 'unhealthy', mqtt: 'disconnected' });
    }
});

async function startServer() {
    try {
        app.listen(PORT, () => {
            console.log(`테스트 서버가 ${PORT}번 포트에서 실행 중.`);
            console.log(` MQTT 브로커 타겟: ${MQTT_BROKER_URL}`);
        });
    }
    catch (error) {
        console.error('서버 시작에 실패했습니다.', error);
        process.exit(1);
    }
}

startServer();