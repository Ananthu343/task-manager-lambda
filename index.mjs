import dotenv from 'dotenv';
dotenv.config();

import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { Worker } from 'worker_threads';
import pkg from 'pg';
import { io } from 'socket.io-client';

const { Pool } = pkg;

// 1. Initialize Clients outside the handler for connection pooling (Performance)
const region = process.env.AWS_REGION;
const s3Client = new S3Client({ region });

// Connect to your backend's Socket.io server
// You must define BACKEND_SOCKET_URL in your Lambda's environment variables (e.g., https://api.yourdomain.com)
const socket = io(process.env.BACKEND_SOCKET_URL || "http://localhost:4000", {
    path: '/socket',
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
      client.release();
      console.log("DB connection successful");
      return;
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

const updateReportProgress = async (reportId, status, progress) => {
  const query = `
    UPDATE report_history
    SET status = $1,
        progress = $2
    WHERE id = $3
  `;

  await pool.query(query, [status, progress, reportId]);
};

const LARGE_DUMMY_THRESHOLD = 50000;

const generateCsvInWorker = (reportId, dummyCount, emitProgress) => {
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL('./generateCsvWorker.mjs', import.meta.url), {
      workerData: { reportId, dummyCount }
    });

    const progressPromises = [];
    let csvContent = '';
    let resolved = false;

    worker.on('message', (message) => {
      if (message.type === 'progress') {
        progressPromises.push(emitProgress(message.progress));
        return;
      }

      if (message.type === 'done') {
        csvContent = message.csvContent;
        Promise.all(progressPromises)
          .then(() => {
            resolved = true;
            resolve(csvContent);
          })
          .catch(reject);
      }
    });

    worker.on('error', (error) => {
      if (!resolved) reject(error);
    });

    worker.on('exit', (code) => {
      if (code !== 0 && !resolved) {
        reject(new Error(`Worker exited with code ${code}`));
      }
    });
  });
};

const processRecord = async (record) => {
  const { reportId, tenantId, dummyCount } = JSON.parse(record.body);
  const bucketName = process.env.S3_BUCKET_NAME;
  const fileName = `reports/${tenantId}/Dummy_report_${reportId.slice(5)}.csv`;

  const emitProgress = async (progress) => {
    console.log(`[EMIT] download_progress - Report: ${reportId}, Tenant: ${tenantId}, Progress: ${progress}%`);
    socket.emit('download_progress', { reportId, tenantId, progress });
    await updateReportProgress(reportId, 'processing', progress);
  };

  let csvContent = "TaskID,Title,Status,Priority,CreatedAt\n";
  await emitProgress(10);

  if (dummyCount > LARGE_DUMMY_THRESHOLD) {
    csvContent = await generateCsvInWorker(reportId, dummyCount, emitProgress);
  } else {
    const batchSize = Math.max(1, Math.floor(dummyCount / 5));
    for (let i = 0; i < dummyCount; i++) {
      csvContent += `${Math.floor(Math.random() * 1000)},Dummy Task ${i},completed,low,${new Date().toISOString()}\n`;

      if ((i + 1) % batchSize === 0 || i === dummyCount - 1) {
        const progressPercent = 10 + Math.floor(((i + 1) / dummyCount) * 70);
        await emitProgress(progressPercent);
      }
    }
  }

  console.log(`[EMIT] download_progress - Report: ${reportId}, Tenant: ${tenantId}, Progress: 85%`);
  socket.emit('download_progress', { reportId, tenantId, progress: 85 });
  await updateReportProgress(reportId, 'processing', 85);

  await s3Client.send(new PutObjectCommand({
    Bucket: bucketName,
    Key: fileName,
    Body: csvContent,
    ContentType: "text/csv"
  }));

  console.log(`[EMIT] download_progress - Report: ${reportId}, Tenant: ${tenantId}, Progress: 95%`);
  socket.emit('download_progress', { reportId, tenantId, progress: 95 });
  await updateReportProgress(reportId, 'processing', 95);

  const s3Url = `https://${bucketName}.s3.${region}.amazonaws.com/${fileName}`;

  await pool.query(
    'UPDATE report_history SET status = $1, link = $2, progress = $3 WHERE id = $4',
    ['completed', s3Url, 100, reportId]
  );

  console.log(`[EMIT] report_completed - Report: ${reportId}, Tenant: ${tenantId}, URL: ${s3Url}`);
  socket.emit('report_completed', { reportId, tenantId, link: s3Url });
  console.log(`Report ${reportId} processed successfully.`);
};

export const handler = async (event) => {
  await connectWithRetry();

  const recordPromises = event.Records.map(async (record) => {
    try {
      await processRecord(record);
    } catch (error) {
      const { reportId } = JSON.parse(record.body);
      console.error(`Record failed for report ${reportId}:`, error.message || error);
      await pool.query(
        'UPDATE report_history SET status = $1, progress = $2 WHERE id = $3',
        ['failed', 100, reportId]
      );
    }
  });

  const settled = await Promise.allSettled(recordPromises);
  const failures = settled.filter(result => result.status === 'rejected');
  if (failures.length > 0) {
    console.warn(`${failures.length} record(s) failed, but batch will complete so messages are removed from SQS.`);
  }

  await new Promise(resolve => setTimeout(resolve, 500));
};
