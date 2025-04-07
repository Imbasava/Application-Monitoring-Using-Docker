const { Kafka } = require('kafkajs');
const pool = require('./db');
require('dotenv').config();

// Set up Kafka consumer
const kafka = new Kafka({
    clientId: 'log-analytics-platform',
    brokers: ['kafka:9092'],
});

const consumer = kafka.consumer({ groupId: 'log-group' });

// Function to save logs into PostgreSQL
const saveLogToDB = async (log) => {
    const {
        endpoint,
        method,
        timestamp,
        message,
        severity,         // default if not provided
        log_type,  // default if not provided
        response_time = null       // can be null if not applicable
    } = log;

    try {
        await pool.query(`
            INSERT INTO logs (endpoint, method, timestamp, message, severity, log_type, response_time)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        `, [endpoint, method, timestamp, message, severity, log_type, response_time]);

        console.log(`✅ Log stored in DB: ${endpoint} (${severity})`);
    } catch (err) {
        console.error("❌ Error inserting log into database:", err);
    }
};

// Kafka consumer logic
const runConsumer = async () => {
    await consumer.connect();
    console.log("🔌 Kafka Consumer connected!");

    await consumer.subscribe({ topic: 'api-logs', fromBeginning: true });
    await consumer.subscribe({ topic: 'api-errors', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const log = JSON.parse(message.value.toString());
            console.log(`📥 Received log from topic ${topic}:`, log);
            await saveLogToDB(log);
        },
    });
};

runConsumer().catch(console.error);
