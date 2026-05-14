import dotenv from 'dotenv';
dotenv.config();

import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { Signer } from "@aws-sdk/rds-signer";
import pkg from 'pg';
const { Pool } = pkg;

// 1. Initialize Clients outside the handler for connection pooling (Performance)
const region = process.env.AWS_REGION;
const s3Client = new S3Client({ region });

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

            // 2. Generate Dummy CSV Data
            let csvContent = "TaskID,Title,Status,Priority,CreatedAt\n";
            for (let i = 0; i < dummyCount; i++) {
                csvContent += `${Math.floor(Math.random() * 1000)},Dummy Task ${i},completed,low,${new Date().toISOString()}\n`;
            }

            // 3. Upload to S3
            const bucketName = process.env.S3_BUCKET_NAME;
            const fileName = `reports/${tenantId}/${reportId}.csv`;
            
            await s3Client.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: fileName,
                Body: csvContent,
                ContentType: "text/csv"
            }));

            const s3Url = `https://${bucketName}.s3.${region}.amazonaws.com/${fileName}`;

            // 4. Update DB to 'completed'
            await client.query(
                'UPDATE report_history SET status = $1, link = $2 WHERE id = $3',
                ['completed', s3Url, reportId]
            );

            console.log(`Report ${reportId} processed successfully.`);
        }
    } catch (err) {
        console.error("Lambda Error:", err);
        throw err; // Trigger SQS retry logic
    } finally {
        // IMPORTANT: Release the client back to the pool, do NOT end the pool
        client.release();
    }
};