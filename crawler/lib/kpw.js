const http = require('http');

const KPW_PRODUCER_URL = process.env.KPW_PRODUCER_URL;

function enqueue(job) {
    if (!KPW_PRODUCER_URL) {
        return Promise.resolve("NO-OP (no KPW_PRODUCER_URL configured)");
    }

    const url = KPW_PRODUCER_URL;
    const payload = Buffer.from(JSON.stringify(job), 'utf8');
    const options = {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': payload.length,
        },
    };

    return new Promise((resolve, reject) => {
        const req = http.request(url, options, (res) => {
            if (res.statusCode === 200) {
                resolve(res.statusMessage);
            } else {
                reject(res.statusMessage);
            }
        });
        req.on('error', (err) => {
            reject(err);
        })
        req.write(payload);
        req.end();
    });
}

module.exports = {
    enqueue: enqueue,
};