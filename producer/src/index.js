import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import amqp from 'amqplib';
import { v4 as uuidv4 } from 'uuid';

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, { cors: { origin: '*' } });

app.use(cors());
app.use(express.json());

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const TASKS_QUEUE = 'tasks';
const RESULTS_QUEUE = 'task.results';
const DLX = 'tasks.dlx';
const DEAD_QUEUE = 'tasks.dead';

// In-memory task store (keyed by id)
const tasks = new Map();

let channel;

async function connectWithRetry(retries = 15) {
  for (let i = 0; i < retries; i++) {
    try {
      const conn = await amqp.connect(RABBITMQ_URL);
      conn.on('error', (err) => console.error('RabbitMQ connection error:', err.message));
      return conn;
    } catch (err) {
      console.log(`RabbitMQ not ready, retrying in 3s... (${i + 1}/${retries})`);
      await new Promise((r) => setTimeout(r, 3000));
    }
  }
  throw new Error('Could not connect to RabbitMQ after retries');
}

async function setup() {
  const conn = await connectWithRetry();
  channel = await conn.createChannel();

  // Dead-letter exchange + queue
  await channel.assertExchange(DLX, 'direct', { durable: true });
  await channel.assertQueue(DEAD_QUEUE, { durable: true });
  await channel.bindQueue(DEAD_QUEUE, DLX, TASKS_QUEUE);

  // Main task queue — durable, routes nack'd messages to DLX
  await channel.assertQueue(TASKS_QUEUE, {
    durable: true,
    arguments: { 'x-dead-letter-exchange': DLX, 'x-dead-letter-routing-key': TASKS_QUEUE },
  });

  // Results queue — ephemeral, only lives while producer is running
  await channel.assertQueue(RESULTS_QUEUE, { durable: false });

  // Consume results and push to connected clients via Socket.io
  channel.consume(RESULTS_QUEUE, (msg) => {
    if (!msg) return;
    const update = JSON.parse(msg.content.toString());
    const task = tasks.get(update.taskId);
    if (task) {
      Object.assign(task, update);
      io.emit('task:update', task);
    }
    channel.ack(msg);
  });

  console.log('Producer connected to RabbitMQ');
}

app.post('/tasks', (req, res) => {
  const { name, type = 'default' } = req.body;
  if (!name) return res.status(400).json({ error: 'name required' });

  const task = {
    id: uuidv4(),
    name,
    type,
    status: 'pending',
    createdAt: Date.now(),
    workerId: null,
    startedAt: null,
    completedAt: null,
    error: null,
  };

  tasks.set(task.id, task);

  channel.sendToQueue(TASKS_QUEUE, Buffer.from(JSON.stringify(task)), {
    persistent: true,
    messageId: task.id,
  });

  io.emit('task:new', task);
  res.json(task);
});

app.get('/tasks', (_req, res) => {
  res.json([...tasks.values()].sort((a, b) => b.createdAt - a.createdAt));
});

app.get('/queue/stats', async (_req, res) => {
  try {
    const main = await channel.checkQueue(TASKS_QUEUE);
    const dead = await channel.checkQueue(DEAD_QUEUE);
    res.json({
      messageCount: main.messageCount,
      consumerCount: main.consumerCount,
      deadCount: dead.messageCount,
    });
  } catch {
    res.json({ messageCount: 0, consumerCount: 0, deadCount: 0 });
  }
});

const PORT = process.env.PORT || 3001;
httpServer.listen(PORT, async () => {
  console.log(`Producer listening on :${PORT}`);
  await setup();
});
