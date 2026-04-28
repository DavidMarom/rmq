import amqp from 'amqplib';
import { randomUUID } from 'crypto';

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const TASKS_QUEUE = 'tasks';
const RESULTS_QUEUE = 'task.results';
const DLX = 'tasks.dlx';
const DEAD_QUEUE = 'tasks.dead';

const WORKER_ID = process.env.WORKER_ID || `worker-${randomUUID().slice(0, 6)}`;
const FAIL_RATE = parseFloat(process.env.FAIL_RATE ?? '0.2');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function connectWithRetry(retries = 15) {
  for (let i = 0; i < retries; i++) {
    try {
      const conn = await amqp.connect(RABBITMQ_URL);
      conn.on('error', (err) => console.error('Connection error:', err.message));
      return conn;
    } catch {
      console.log(`RabbitMQ not ready, retrying in 3s... (${i + 1}/${retries})`);
      await sleep(3000);
    }
  }
  throw new Error('Could not connect to RabbitMQ');
}

async function start() {
  const conn = await connectWithRetry();
  const channel = await conn.createChannel();

  // Assert same topology as the producer so workers can start independently
  await channel.assertExchange(DLX, 'direct', { durable: true });
  await channel.assertQueue(DEAD_QUEUE, { durable: true });
  await channel.bindQueue(DEAD_QUEUE, DLX, TASKS_QUEUE);

  await channel.assertQueue(TASKS_QUEUE, {
    durable: true,
    arguments: { 'x-dead-letter-exchange': DLX, 'x-dead-letter-routing-key': TASKS_QUEUE },
  });

  await channel.assertQueue(RESULTS_QUEUE, { durable: false });

  // Process one task at a time — fair dispatch
  channel.prefetch(1);

  console.log(`[${WORKER_ID}] Ready. Waiting for tasks... (fail rate: ${FAIL_RATE * 100}%)`);

  channel.consume(TASKS_QUEUE, async (msg) => {
    if (!msg) return;

    const task = JSON.parse(msg.content.toString());
    const startedAt = Date.now();

    console.log(`[${WORKER_ID}] → ${task.name} (${task.type})`);

    // Notify: picked up by this worker
    publish(channel, { taskId: task.id, status: 'processing', workerId: WORKER_ID, startedAt });

    // Simulate variable-length work: 2–6 seconds
    await sleep(2000 + Math.random() * 4000);

    const failed = Math.random() < FAIL_RATE;
    const completedAt = Date.now();

    if (failed) {
      console.log(`[${WORKER_ID}] ✗ ${task.name} — failed`);
      publish(channel, {
        taskId: task.id,
        status: 'failed',
        workerId: WORKER_ID,
        startedAt,
        completedAt,
        error: 'Simulated processing error',
      });
      // nack without requeue → message goes to dead-letter queue
      channel.nack(msg, false, false);
    } else {
      console.log(`[${WORKER_ID}] ✓ ${task.name}`);
      publish(channel, {
        taskId: task.id,
        status: 'done',
        workerId: WORKER_ID,
        startedAt,
        completedAt,
      });
      channel.ack(msg);
    }
  });
}

function publish(channel, payload) {
  channel.sendToQueue(RESULTS_QUEUE, Buffer.from(JSON.stringify(payload)));
}

start().catch((err) => {
  console.error('Worker failed to start:', err.message);
  process.exit(1);
});
