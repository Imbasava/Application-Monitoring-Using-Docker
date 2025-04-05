const { Kafka } = require('kafkajs');

// Set up Kafka producer
const kafka = new Kafka({
    clientId: 'log-analytics-platform',
    brokers: ['kafka:9092'],  // Kafka service name from docker-compose.yml
});

const producer = kafka.producer();

// Function to send logs to Kafka with topic name
const sendLogToKafka = async (log, topic) => {
    try {
        await producer.connect();
        await producer.send({
            topic: topic,  // Topic name is now dynamic
            messages: [
                { value: JSON.stringify(log) },
            ],
        });
        console.log(`Log sent to Kafka topic: ${topic}`);
    } catch (error) {
        console.error('Error sending log to Kafka:', error);
    } finally {
        await producer.disconnect();
    }
};

module.exports = { sendLogToKafka };
