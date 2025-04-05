const { Kafka } = require('kafkajs');
const pool = require('./db');
require('dotenv').config();

// Set up Kafka consumer
const kafka = new Kafka({
    clientId: 'log-analytics-platform',
    brokers: ['kafka:9092'],  // Kafka service name from docker-compose.yml
});

const consumer = kafka.consumer({ groupId: 'log-group' });

// Function to save logs into PostgreSQL
const saveLogToDB = async (log) => {
    const { level, message, meta } = log;

    try {
        await pool.query(`
            INSERT INTO logs (level, message, meta)
            VALUES ($1, $2, $3)
        `, [level, message, meta]);

        console.log(`Log stored: ${message}`);
    } catch (err) {
        console.error("Error inserting log into database:", err);
    }
};

// Kafka consumer logic
const runConsumer = async () => {
    await consumer.connect();
    console.log("Kafka Consumer connected!");

    // Subscribe to multiple topics (api-logs, api-errors)
    await consumer.subscribe({ topic: 'api-logs', fromBeginning: true });
    await consumer.subscribe({ topic: 'api-errors', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const log = JSON.parse(message.value.toString());
            console.log(`Received log from topic ${topic}:`, log);
            await saveLogToDB(log);
        },
    });
};

// Start consumer
runConsumer().catch(console.error);
