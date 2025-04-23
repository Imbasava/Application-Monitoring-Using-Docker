const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
    user: process.env.DB_USER || 'postgres',
    host: process.env.DB_HOST || 'database',
    database: process.env.DB_NAME || 'logs_db',
    password: process.env.DB_PASSWORD || 'mysecretpassword',
    port: process.env.DB_PORT || 5432
});

// Test connection


const connectDB = async () => {
    try {
        const client = await pool.connect();
        console.log("✅ Connected to PostgreSQL");
        client.release();  // Release the client back to the pool
    } catch (err) {
        console.error("❌ Database connection error:", err);
        setTimeout(connectDB, 5000); // Retry connection after 5 seconds
    }
};

// Initialize connection
connectDB();

module.exports = pool;
