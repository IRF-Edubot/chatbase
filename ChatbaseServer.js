const express = require('express');
const axios = require('axios');
const {createServer} = require('node:https');
const {readFileSync} = require('node:fs');
const {Readable} = require('stream');
const crypto = require('crypto');
const cookieParser = require('cookie-parser');

class ChatbaseServer {
    constructor() {
        this.options = {
            host     : '0.0.0.0',
            port     : 3000,
            chatbotId: '****',
            apiKey   : '****',,
            apiUrl   : 'https://www.chatbase.co/api/v1/chat'
        };

        this.app = express();

        // Session storage - in production, consider using Redis or a database
        this.sessions = new Map();

        this.setupCORS();
        this.app.use(express.json());
        this.app.use(cookieParser());

        this.sslOptions = {
            key : readFileSync('/etc/ssl/classyedu/classyedu.eu.key'),
            cert: readFileSync('/etc/ssl/classyedu/fullchain.cer')
        };
        this.server = createServer(this.sslOptions, this.app);
        this.setupSessionMiddleware();
        this.setupRoutes();
        this.init();
    }

    generateUUID() {
        return crypto.randomUUID();
    }

    setupSessionMiddleware() {
        this.app.use((req, res, next) => {
            let sessionId = req.cookies.sessionId;
            if (!sessionId || !this.sessions.has(sessionId)) {
                sessionId = this.generateUUID();
                res.cookie('sessionId', sessionId, {
                    httpOnly: true,
                    secure  : true,
                    sameSite: 'strict',
                    maxAge  : 24 * 60 * 60 * 1000
                });
                this.sessions.set(sessionId, {
                    id                 : sessionId,
                    createdAt          : new Date(),
                    lastActivity       : new Date(),
                    conversationHistory: []
                });
                console.log(`New session created: ${sessionId}`);
            } else {
                const session = this.sessions.get(sessionId);
                session.lastActivity = new Date();
                console.log(`Existing session: ${sessionId}`);
            }
            req.sessionId = sessionId;
            req.session = this.sessions.get(sessionId);
            next();
        });
        console.log('Session middleware configured');
    }

    cleanupSessions() {
        const now = new Date();
        const maxAge = 24 * 60 * 60 * 1000;
        for (const [sessionId, session] of this.sessions.entries()) {
            if (now - session.lastActivity > maxAge) {
                this.sessions.delete(sessionId);
                console.log(`Session expired and removed: ${sessionId}`);
            }
        }
    }

    setupCORS() {
        this.app.use((req, res, next) => {
            res.setHeader('Access-Control-Allow-Origin', req.headers.origin);
            res.setHeader('Access-Control-Allow-Headers',
                'Origin, X-Requested-With, Content-Type, Accept, Authorization, Cache-Control, Pragma'
            );
            res.setHeader('Access-Control-Allow-Methods',
                'GET, POST, PUT, DELETE, OPTIONS, HEAD'
            );
            res.setHeader('Access-Control-Allow-Credentials', 'true');
            res.setHeader('Access-Control-Max-Age', '86400');
            if (req.method === 'OPTIONS') {
                res.status(200).end();
                return;
            }
            next();
        });
        console.log('CORS middleware configured');
    }

    async readChatbotReply(messages) {
        try {
            const response = await axios.post(
                this.options.apiUrl,
                {
                    messages,
                    chatbotId  : this.options.chatbotId,
                    stream     : true,
                    temperature: 0
                },
                {
                    headers     : {
                        Authorization : `Bearer ${this.options.apiKey}`,
                        'Content-Type': 'application/json'
                    },
                    responseType: 'stream'
                }
            );
            const readable = new Readable({
                read() {
                }
            });
            response.data.on('data', (chunk) => {
                readable.push(chunk);
            });
            response.data.on('end', () => {
                readable.push(null);
            });
            const decoder = new TextDecoder();
            let done = false;
            readable.on('data', (chunk) => {
                const chunkValue = decoder.decode(chunk);
                process.stdout.write(chunkValue);
            });
            readable.on('end', () => {
                done = true;
            });
        } catch (error) {
            console.log('Error:', error.message);
        }
    }

    setupRoutes() {
        this.app.get('/api/session', (req, res) => {
            const sessionInfo = {
                sessionId        : req.sessionId,
                createdAt        : req.session.createdAt,
                lastActivity     : req.session.lastActivity,
                conversationCount: req.session.conversationHistory.length
            };
            console.log(`Session info requested for: ${req.sessionId}`);
            res.json(sessionInfo);
        });

        this.app.post('/api/chat', async (req, res) => {
            try {
                const {message, temperature = 0} = req.body;
                if (!message) {
                    return res.status(400).json({error: 'Message is required'});
                }
                req.session.conversationHistory.push({
                    content  : message,
                    role     : 'user',
                    timestamp: new Date()
                });
                // const messages = [{
                //     content: message,
                //     role   : 'user'
                // }];
                res.setHeader('Content-Type', 'text/event-stream');
                res.setHeader('Cache-Control', 'no-cache');
                res.setHeader('Connection', 'keep-alive');
                console.log(`Streaming chat request from session ${req.sessionId}:`, req.session.conversationHistory);
                const response = await axios.post(this.options.apiUrl, {
                    messages : req.session.conversationHistory,
                    chatbotId: this.options.chatbotId,
                    stream   : true,
                    temperature
                }, {
                    headers     : {
                        Authorization : `Bearer ${this.options.apiKey}`,
                        'Content-Type': 'application/json'
                    },
                    responseType: 'stream'
                });
                let botResponse = '';
                const decoder = new TextDecoder();
                response.data.on('data', (chunk) => {
                    const chunkValue = decoder.decode(chunk);
                    botResponse += chunkValue;
                    res.write(`data: ${JSON.stringify({chunk: chunkValue})}\n\n`);
                });
                response.data.on('end', () => {
                    req.session.conversationHistory.push({
                        content  : botResponse,
                        role     : 'assistant',
                        timestamp: new Date()
                    });
                    res.write(`data: ${JSON.stringify({done: true})}\n\n`);
                    res.end();
                    console.log(`Streaming response completed for session ${req.sessionId}`);
                });
                response.data.on('error', (error) => {
                    console.error('Stream error:', error.message);
                    res.write(`data: ${JSON.stringify({error: error.message})}\n\n`);
                    res.end();
                });
            } catch (error) {
                console.error('Chat API Error:', error.message);
                if (!res.headersSent) {
                    res.status(500).json({error: 'Internal server error'});
                }
            }
        });

        this.app.post('/api/chat-simple', async (req, res) => {
            try {
                const {message, temperature = 0} = req.body;
                if (!message) {
                    return res.status(400).json({error: 'Message is required'});
                }
                req.session.conversationHistory.push({
                    content  : message,
                    role     : 'user',
                    timestamp: new Date()
                });
                // const messages = [{
                //     content: message,
                //     role   : 'user'
                // }];
                console.log(`Simple chat request from session ${req.sessionId}:`, req.session.conversationHistory);
                const response = await axios.post(this.options.apiUrl, {
                    messages : req.session.conversationHistory,
                    chatbotId: this.options.chatbotId,
                    stream   : false,
                    temperature
                }, {
                    headers: {
                        Authorization : `Bearer ${this.options.apiKey}`,
                        'Content-Type': 'application/json'
                    }
                });
                req.session.conversationHistory.push({
                    content  : response.data.text,
                    role     : 'assistant',
                    timestamp: new Date()
                });
                console.log(`Simple chat response received for session ${req.sessionId}`);
                res.json({
                    response : response.data,
                    sessionId: req.sessionId
                });
            } catch (error) {
                console.error('Simple Chat API Error:', error.message);
                res.status(500).json({error: 'Internal server error'});
            }
        });

        this.app.get('/api/conversation-history', (req, res) => {
            const history = req.session.conversationHistory || [];
            console.log(`Conversation history requested for session ${req.sessionId}`);
            res.json({
                sessionId: req.sessionId,
                history  : history
            });
        });

        this.app.delete('/api/conversation-history', (req, res) => {
            req.session.conversationHistory = [];
            console.log(`Conversation history cleared for session ${req.sessionId}`);
            res.json({
                message  : 'Conversation history cleared',
                sessionId: req.sessionId
            });
        });

        this.app.get('/health', (req, res) => {
            console.log(`Health check requested from session ${req.sessionId}`);
            res.json({
                status   : 'OK',
                timestamp: new Date().toISOString(),
                server   : 'Chatbase Server',
                version  : '1.0.0',
                sessionId: req.sessionId
            });
        });

        this.app.get('/api/info', (req, res) => {
            res.json({
                server   : 'Chatbase Server',
                version  : '1.0.0',
                endpoints: [
                    'GET /health',
                    'GET /api/info',
                    'GET /api/session',
                    'GET /api/conversation-history',
                    'DELETE /api/conversation-history',
                    'POST /api/chat',
                    'POST /api/chat-simple'
                ],
                timestamp: new Date().toISOString(),
                cors     : 'enabled',
                sessions : 'UUID-based with cookies',
                sessionId: req.sessionId
            });
        });

        this.app.use(express.static('public'));
        console.log('Routes configured');
    }

    init() {
        setInterval(() => {
            this.cleanupSessions();
        }, 60 * 60 * 1000);
        this.server.listen(this.options.port, this.options.host, () => {
            console.log(`Server running on port ${this.options.port}`);
            console.log(`Health check: https://${this.options.host}:${this.options.port}/health`);
            console.log(`Session info: GET https://${this.options.host}:${this.options.port}/api/session`);
            console.log(`Conversation history: GET https://${this.options.host}:${this.options.port}/api/conversation-history`);
            console.log(`Streaming chat: POST https://${this.options.host}:${this.options.port}/api/chat`);
            console.log(`Simple chat: POST https://${this.options.host}:${this.options.port}/api/chat-simple`);
            console.log(`Server info: https://${this.options.host}:${this.options.port}/api/info`);
            console.log(`CORS: Enabled for all origins`);
            console.log(`Sessions: UUID-based with cookie storage`);
        });
    }

    stop() {
        if (this.server) {
            this.server.close(() => {
                console.log('Server stopped');
                this.sessions.clear();
            });
        }
    }
}

module.exports = ChatbaseServer;
