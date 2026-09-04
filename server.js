const ChatbaseServer = require('./ChatbaseServer');

const chatbaseServer = new ChatbaseServer();

process.on('SIGINT', () => {
    console.log('\nReceived SIGINT. Graceful shutdown...');
    chatbaseServer.stop();
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\nReceived SIGTERM. Graceful shutdown...');
    chatbaseServer.stop();
    process.exit(0);
});