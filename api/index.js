const express = require('express');
const app = express();
const port = 3000;
const cors = require('cors');
//const kafkaProducer = require('../kafka/producer');  // Import Kafka producer
const kafkaProducer = require('/usr/src/app/kafka/producer');

app.use(cors());  // Enable CORS

// Sample API endpoints with meaningful names
app.get('/api/healthcheck', async (req, res) => {
    const log = {
        endpoint: '/api/healthcheck',
        method: 'GET',
        timestamp: new Date().toISOString(),
        message: 'Request received at /api/healthcheck'
    };

    // Send the log to Kafka (general logs)
    await kafkaProducer.sendLogToKafka(log, 'api-logs');

    res.send('API is up and running');
});

app.get('/api/users', async (req, res) => {
    const log = {
        endpoint: '/api/users',
        method: 'GET',
        timestamp: new Date().toISOString(),
        message: 'Request received at /api/users'
    };

    // Send the log to Kafka (general logs)
    await kafkaProducer.sendLogToKafka(log, 'api-logs');

    res.send('List of all users');
});

app.get('/api/products', async (req, res) => {
    const log = {
        endpoint: '/api/products',
        method: 'GET',
        timestamp: new Date().toISOString(),
        message: 'Request received at /api/products'
    };

    // Send the log to Kafka (general logs)
    await kafkaProducer.sendLogToKafka(log, 'api-logs');

    res.send('List of all products');
});

app.get('/api/orders', async (req, res) => {
    const log = {
        endpoint: '/api/orders',
        method: 'GET',
        timestamp: new Date().toISOString(),
        message: 'Request received at /api/orders'
    };

    // Send the log to Kafka (general logs)
    await kafkaProducer.sendLogToKafka(log, 'api-logs');

    res.send('List of all orders');
});

app.get('/api/traffic', async (req, res) => {
    const log = {
        endpoint: '/api/traffic',
        method: 'GET',
        timestamp: new Date().toISOString(),
        message: 'Request received at /api/traffic'
    };

    // Send the log to Kafka (general logs)
    await kafkaProducer.sendLogToKafka(log, 'api-logs');

    res.send('API traffic data');
});

app.get('/api/errors', async (req, res) => {
    const log = {
        endpoint: '/api/errors',
        method: 'GET',
        timestamp: new Date().toISOString(),
        message: 'Request received at /api/errors'
    };

    // Simulate an error and log it to a separate topic
    const errorLog = {
        endpoint: '/api/errors',
        method: 'GET',
        timestamp: new Date().toISOString(),
        message: 'Error: Request failed at /api/errors'
    };

    // Send the error log to Kafka (error logs)
    await kafkaProducer.sendLogToKafka(errorLog, 'api-errors');

    res.send('List of most frequent errors');
});

app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});
