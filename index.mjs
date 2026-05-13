import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import pkg from 'pg';
const { Client } = pkg;

const s3Client = new S3Client({});

export const handler = async (event) => {
    const client = new Client({
        connectionString: process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false }
    });

    try {
        await client.connect();

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

            const s3Url = `https://${bucketName}.s3.amazonaws.com/${fileName}`;

            // 4. Update DB to 'completed'
            await client.query(
                'UPDATE report_history SET status = $1, link = $2 WHERE id = $3',
                ['completed', s3Url, reportId]
            );

            // 5. Notify the main server to emit Socket.io event
            await fetch(`${process.env.MAIN_SERVER_URL}/api/v1/internal/notify-report`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'x-internal-key': process.env.INTERNAL_SECRET },
                body: JSON.stringify({ tenantId, reportId, s3Url })
            });
        }
    } catch (err) {
        console.error("Lambda Error:", err);
    } finally {
        await client.end();
    }
};