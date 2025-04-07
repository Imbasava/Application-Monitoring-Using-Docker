const express = require('express');
const app = express();
const port = 3000;
const cors = require('cors');
const kafkaProducer = require('/usr/src/app/kafka/producer');

app.use(cors());  // Enable CORS

// Helper to generate log
const generateLog = (endpoint, method, message, severity, log_type, startTime) => {
    return {
        endpoint,
        method,
        timestamp: new Date().toISOString(),
        message,
        severity,
        log_type,
        response_time: Date.now() - startTime   // ✅ NOT duration
    };
};



app.get('/api/healthcheck', async (req, res) => {
    const startTime = Date.now();

    const log = generateLog('/api/healthcheck', 'GET', 'Request received at /api/healthcheck', 'INFO', 'application', startTime);
    await kafkaProducer.sendLogToKafka(log, 'api-logs');

    res.send('API is up and running');
});

app.get('/api/users', async (req, res) => {
    const startTime = Date.now();

    const log = generateLog('/api/users', 'GET', 'Request received at /api/users', 'INFO', 'application', startTime);
    await kafkaProducer.sendLogToKafka(log, 'api-logs');

    res.send('List of all users');
});

app.get('/api/products', async (req, res) => {
    const startTime = Date.now();

    const log = generateLog('/api/products', 'GET', 'Request received at /api/products', 'INFO', 'application', startTime);
    await kafkaProducer.sendLogToKafka(log, 'api-logs');

    res.send('List of all products');
});

app.get('/api/orders', async (req, res) => {
    const startTime = Date.now();

    const log = generateLog('/api/orders', 'GET', 'Request received at /api/orders', 'INFO', 'application', startTime);
    await kafkaProducer.sendLogToKafka(log, 'api-logs');

    res.send('List of all orders');
});

app.get('/api/traffic', async (req, res) => {
    const startTime = Date.now();

    const log = generateLog('/api/traffic', 'GET', 'Request received at /api/traffic', 'INFO', 'application', startTime);
    await kafkaProducer.sendLogToKafka(log, 'api-logs');

    res.send('API traffic data');
});

app.get('/api/errors', async (req, res) => {
    const startTime = Date.now();

    const log = generateLog('/api/errors', 'GET', 'Request received at /api/errors', 'INFO', 'application', startTime);
    await kafkaProducer.sendLogToKafka(log, 'api-logs');

    // Simulate an error log
    const errorLog = generateLog('/api/errors', 'GET', 'Error: Request failed at /api/errors', 'ERROR', 'error', startTime);
    await kafkaProducer.sendLogToKafka(errorLog, 'api-errors');

    res.send('List of most frequent errors');
});

app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});
