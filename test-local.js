// test-local.js

import { handler } from "./index.mjs";

const mockSQSEvent = {
    Records: [
        {
            messageId: 'test-001',
            body: JSON.stringify({
                reportId: 'af3e433b-2942-4c62-9916-59bb336f1b05',
                tenantId: '0d0686bb-2fac-492e-afb0-da2eadf82a3a',
                dummyCount: '100000'
            }),
            eventSource: 'aws:sqs'
        }
    ]
};

// Run it
handler(mockSQSEvent)
    .then(result => console.log('Result:', result))
    .catch(err => console.error('Error:', err));