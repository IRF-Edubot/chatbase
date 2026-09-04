module.exports = {
    apps: [{
        name              : 'chatbase',
        script            : './server.js',
        node_args         : '--max-old-space-size=128',
        max_memory_restart: '256M'
    }]
};
