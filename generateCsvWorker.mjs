import { parentPort, workerData } from 'worker_threads';

if (!parentPort) {
  throw new Error('Worker must be run as a worker thread.');
}

const { reportId, dummyCount } = workerData;
let csvContent = "TaskID,Title,Status,Priority,CreatedAt\n";

const batchSize = Math.max(1, Math.floor(dummyCount / 5));
for (let i = 0; i < dummyCount; i++) {
  csvContent += `${Math.floor(Math.random() * 1000)},Dummy Task ${i},completed,low,${new Date().toISOString()}\n`;

  if ((i + 1) % batchSize === 0 || i === dummyCount - 1) {
    const progressPercent = 10 + Math.floor(((i + 1) / dummyCount) * 70);
    parentPort.postMessage({ type: 'progress', progress: progressPercent });
  }
}

parentPort.postMessage({ type: 'done', csvContent });
