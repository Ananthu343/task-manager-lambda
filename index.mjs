import dotenv from 'dotenv';
dotenv.config();

import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { Signer } from "@aws-sdk/rds-signer";
import pkg from 'pg';
import { io } from 'socket.io-client';

const { Pool } = pkg;

// 1. Initialize Clients outside the handler for connection pooling (Performance)
const region = process.env.AWS_REGION;
const s3Client = new S3Client({ region });

// Connect to your backend's Socket.io server
// You must define BACKEND_SOCKET_URL in your Lambda's environment variables (e.g., https://api.yourdomain.com)
const socket = io(process.env.BACKEND_SOCKET_URL, {
    transports: ['websocket'],
    autoConnect: true
});

// Socket Event Monitoring
socket.on('connect', () => {
    console.log('[SOCKET] Connected to backend:', socket.id);
});

socket.on('disconnect', (reason) => {
    console.log('[SOCKET] Disconnected from backend. Reason:', reason);
});

socket.on('connect_error', (error) => {
    console.error('[SOCKET] Connection error:', error.message);
});

socket.on('error', (error) => {
    console.error('[SOCKET] Socket error:', error);
});

socket.on('reconnect', (attemptNumber) => {
    console.log('[SOCKET] Reconnected after', attemptNumber, 'attempts');
});

socket.on('reconnect_attempt', (attemptNumber) => {
    console.log('[SOCKET] Reconnection attempt #', attemptNumber);
});

socket.on('reconnect_error', (error) => {
    console.error('[SOCKET] Reconnection error:', error.message);
});

socket.on('reconnect_failed', () => {
    console.error('[SOCKET] Reconnection failed after max attempts');
});

// Create the pool once. Lambda will reuse this pool across warm invocations.
const pool = new Pool({
  host: process.env.DB_HOST, // Get this from the 'Endpoints' tab
  port: 5432,
  database: process.env.DB_NAME, // Or your specific DB name
  user: process.env.DB_USER,     // Your master username
  password: process.env.DB_PASSWORD, 
  max: 5,
  ssl: { rejectUnauthorized: false }, // REQUIRED for RDS
  connectionTimeoutMillis: 5000,
});

// Retry logic with exponential backoff
const connectWithRetry = async (maxRetries = 3, baseDelay = 500) => {
  let lastError;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`DB connection attempt ${attempt}/${maxRetries}`);
      const client = await pool.connect();
      console.log("DB connection successful");
      return client;
    } catch (error) {
      lastError = error;
      console.warn(`DB connection failed (attempt ${attempt}): ${error.message}`);
      
      if (attempt < maxRetries) {
        const delayMs = baseDelay * Math.pow(2, attempt - 1); // Exponential backoff
        console.log(`Retrying in ${delayMs}ms...`);
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
  }
  
  throw new Error(`Failed to connect to database after ${maxRetries} attempts: ${lastError.message}`);
};

export const handler = async (event) => {
    // We use a pool client for this specific execution with retry logic
    const client = await connectWithRetry();

    try {
        for (const record of event.Records) {
            const { reportId, tenantId, dummyCount } = JSON.parse(record.body);

            // 1. Update Status to 'processing'
            await client.query('UPDATE report_history SET status = $1 WHERE id = $2', ['processing', reportId]);

            // ** EMIT: Initial progress **
            console.log(`[EMIT] download_progress - Report: ${reportId}, Tenant: ${tenantId}, Progress: 10%`);
            socket.emit('download_progress', { reportId, tenantId, progress: 10 });

            // 2. Generate Dummy CSV Data
            let csvContent = "TaskID,Title,Status,Priority,CreatedAt\n";
            
            // To simulate dynamic progress, emit events at intervals
            const batchSize = Math.max(1, Math.floor(dummyCount / 5)); // 5 checkpoints
            
            for (let i = 0; i < dummyCount; i++) {
                csvContent += `${Math.floor(Math.random() * 1000)},Dummy Task ${i},completed,low,${new Date().toISOString()}\n`;
                
                if ((i + 1) % batchSize === 0) {
                    // Calculate progress logically up to 80%
                    const progressPercent = 10 + Math.floor(((i + 1) / dummyCount) * 70); 
                    console.log(`[EMIT] download_progress - Report: ${reportId}, Tenant: ${tenantId}, Progress: ${progressPercent}%`);
                    socket.emit('download_progress', { reportId, tenantId, progress: progressPercent });
                }
            }
            
            // Data generation complete
            console.log(`[EMIT] download_progress - Report: ${reportId}, Tenant: ${tenantId}, Progress: 85%`);
            socket.emit('download_progress', { reportId, tenantId, progress: 85 });

            // 3. Upload to S3
            const bucketName = process.env.S3_BUCKET_NAME;
            const fileName = `reports/${tenantId}/Dummy_report_${reportId.slice(5)}.csv`;
            
            await s3Client.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: fileName,
                Body: csvContent,
                ContentType: "text/csv"
            }));

            // Upload complete
            console.log(`[EMIT] download_progress - Report: ${reportId}, Tenant: ${tenantId}, Progress: 95%`);
            socket.emit('download_progress', { reportId, tenantId, progress: 95 });

            const s3Url = `https://${bucketName}.s3.${region}.amazonaws.com/${fileName}`;

            // 4. Update DB to 'completed'
            await client.query(
                'UPDATE report_history SET status = $1, link = $2 WHERE id = $3',
                ['completed', s3Url, reportId]
            );

            // ** EMIT: Final Completion with link **
            console.log(`[EMIT] report_completed - Report: ${reportId}, Tenant: ${tenantId}, URL: ${s3Url}`);
            socket.emit('report_completed', { reportId, tenantId, link: s3Url });

            console.log(`Report ${reportId} processed successfully.`);
        }
        
        // Delay slightly to ensure Socket.io flushes all network events before Lambda freezes
        await new Promise(resolve => setTimeout(resolve, 500)); 
        
    } catch (err) {
        console.error("Lambda Error:", err);
        throw err; // Trigger SQS retry logic
    } finally {
        // IMPORTANT: Release the client back to the pool, do NOT end the pool
        client.release();
    }
};
