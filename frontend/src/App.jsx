import { useState, useEffect, useRef } from 'react';
import { io } from 'socket.io-client';

const API = 'http://localhost:3001';

const STATUS = {
  pending:    { label: 'Pending',    color: '#f59e0b', bg: '#f59e0b22' },
  processing: { label: 'Processing', color: '#3b82f6', bg: '#3b82f622' },
  done:       { label: 'Done',       color: '#22c55e', bg: '#22c55e22' },
  failed:     { label: 'Failed',     color: '#ef4444', bg: '#ef444422' },
};

const TASK_TYPES = [
  'image-resize', 'email-send', 'data-export',
  'report-gen', 'thumbnail', 'transcription',
];

export default function App() {
  const [tasks, setTasks] = useState([]);
  const [stats, setStats] = useState({ messageCount: 0, consumerCount: 0, deadCount: 0 });
  const [connected, setConnected] = useState(false);
  const [taskName, setTaskName] = useState('');
  const [taskType, setTaskType] = useState(TASK_TYPES[0]);
  const [submitting, setSubmitting] = useState(false);
  const socketRef = useRef(null);

  useEffect(() => {
    fetch(`${API}/tasks`)
      .then((r) => r.json())
      .then(setTasks)
      .catch(() => {});

    const socket = io(API, { transports: ['websocket'] });
    socketRef.current = socket;

    socket.on('connect', () => setConnected(true));
    socket.on('disconnect', () => setConnected(false));
    socket.on('task:new', (task) => setTasks((prev) => [task, ...prev]));
    socket.on('task:update', (update) =>
      setTasks((prev) => prev.map((t) => (t.id === update.id ? { ...t, ...update } : t)))
    );

    const pollStats = () =>
      fetch(`${API}/queue/stats`).then((r) => r.json()).then(setStats).catch(() => {});
    pollStats();
    const interval = setInterval(pollStats, 2000);

    return () => { socket.disconnect(); clearInterval(interval); };
  }, []);

  const submit = async (e) => {
    e.preventDefault();
    if (!taskName.trim()) return;
    setSubmitting(true);
    try {
      await fetch(`${API}/tasks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: taskName.trim(), type: taskType }),
      });
      setTaskName('');
    } finally {
      setSubmitting(false);
    }
  };

  const submitBatch = async () => {
    for (const type of TASK_TYPES) {
      await fetch(`${API}/tasks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: `${type}-${Date.now().toString(36)}`, type }),
      });
    }
  };

  const counts = Object.fromEntries(
    Object.keys(STATUS).map((s) => [s, tasks.filter((t) => t.status === s).length])
  );

  return (
    <div style={{ minHeight: '100vh', background: '#0f172a', color: '#e2e8f0' }}>
      {/* Header */}
      <header style={{
        background: '#1e293b', borderBottom: '1px solid #334155',
        padding: '14px 24px', display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{ fontSize: 22 }}>🐇</span>
          <span style={{ fontSize: 18, fontWeight: 700 }}>RabbitMQ Task Queue</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <div style={{
            width: 8, height: 8, borderRadius: '50%',
            background: connected ? '#22c55e' : '#ef4444',
            animation: connected ? 'none' : 'pulse 1.5s infinite',
          }} />
          <span style={{ fontSize: 13, color: '#94a3b8' }}>
            {connected ? 'Connected' : 'Disconnected'}
          </span>
        </div>
      </header>

      <div style={{ display: 'grid', gridTemplateColumns: '320px 1fr', gap: 20, padding: 20, maxWidth: 1300, margin: '0 auto' }}>

        {/* Sidebar */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

          {/* Submit form */}
          <Panel title="Submit Task">
            <form onSubmit={submit} style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              <input
                value={taskName}
                onChange={(e) => setTaskName(e.target.value)}
                placeholder="Task name…"
                style={inputStyle}
              />
              <select value={taskType} onChange={(e) => setTaskType(e.target.value)} style={inputStyle}>
                {TASK_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
              </select>
              <button type="submit" disabled={submitting || !taskName.trim()} style={btnStyle('#f97316')}>
                {submitting ? 'Enqueueing…' : 'Enqueue Task'}
              </button>
            </form>
            <button onClick={submitBatch} style={{ ...btnStyle('#1d4ed8'), marginTop: 8 }}>
              Enqueue Batch (6 tasks)
            </button>
          </Panel>

          {/* Queue stats */}
          <Panel title="Queue Stats">
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
              <StatCard label="In Queue"   value={stats.messageCount}   color="#f59e0b" />
              <StatCard label="Workers"    value={stats.consumerCount}  color="#3b82f6" />
              <StatCard label="Processing" value={counts.processing}    color="#3b82f6" />
              <StatCard label="Done"       value={counts.done}          color="#22c55e" />
              <StatCard label="Failed"     value={counts.failed}        color="#ef4444" />
              <StatCard label="Dead-letter" value={stats.deadCount}     color="#94a3b8" />
            </div>
          </Panel>

          {/* How it works */}
          <Panel title="How it works">
            <ol style={{ paddingLeft: 18, display: 'flex', flexDirection: 'column', gap: 8, fontSize: 13, color: '#94a3b8', lineHeight: 1.6 }}>
              <li>Tasks are published to a <Hl>durable queue</Hl></li>
              <li>Workers use <Hl>prefetch(1)</Hl> — one task at a time per worker</li>
              <li>On success the worker sends <Hl>ack</Hl></li>
              <li>On failure it sends <Hl>nack</Hl> (no requeue)</li>
              <li>Nack'd messages route to the <Hl>dead-letter queue</Hl></li>
              <li>Results flow back via a <Hl>results queue</Hl> → WebSocket</li>
            </ol>
          </Panel>

          {/* Start workers hint */}
          <Panel title="Start workers">
            <pre style={{ fontSize: 11, color: '#64748b', lineHeight: 1.8, overflowX: 'auto' }}>
{`# terminal 1
cd worker
WORKER_ID=alpha node src/worker.js

# terminal 2
cd worker
WORKER_ID=beta node src/worker.js`}
            </pre>
          </Panel>
        </div>

        {/* Task list */}
        <div style={{ background: '#1e293b', borderRadius: 12, border: '1px solid #334155', overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
          <div style={{
            padding: '14px 20px', borderBottom: '1px solid #334155',
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          }}>
            <span style={{ fontWeight: 600 }}>Tasks ({tasks.length})</span>
            <div style={{ display: 'flex', gap: 14 }}>
              {Object.entries(STATUS).map(([s, { color }]) => (
                <span key={s} style={{ fontSize: 12, color }}>
                  {counts[s]} {s}
                </span>
              ))}
            </div>
          </div>

          <div style={{ overflowY: 'auto', flex: 1 }}>
            {tasks.length === 0 ? (
              <div style={{ padding: 48, textAlign: 'center', color: '#475569', fontSize: 14 }}>
                No tasks yet — enqueue one to get started.
              </div>
            ) : (
              tasks.map((task) => <TaskRow key={task.id} task={task} />)
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

function TaskRow({ task }) {
  const s = STATUS[task.status];
  const duration =
    task.completedAt && task.startedAt
      ? `${((task.completedAt - task.startedAt) / 1000).toFixed(1)}s`
      : task.startedAt
      ? 'running…'
      : null;

  return (
    <div style={{
      padding: '13px 20px',
      borderBottom: '1px solid #1a2744',
      display: 'grid',
      gridTemplateColumns: '1fr auto auto',
      gap: 16,
      alignItems: 'center',
      background: task.status === 'processing' ? '#1e3a5f18' : 'transparent',
      transition: 'background 0.4s',
    }}>
      <div>
        <div style={{ fontSize: 14, fontWeight: 500, color: '#f1f5f9' }}>{task.name}</div>
        <div style={{ fontSize: 12, color: '#475569', marginTop: 3 }}>
          {task.type}
          {task.workerId ? <> · <span style={{ color: '#64748b' }}>{task.workerId}</span></> : null}
          {task.error ? <> · <span style={{ color: '#ef4444' }}>{task.error}</span></> : null}
        </div>
      </div>
      {duration && <span style={{ fontSize: 12, color: '#64748b', whiteSpace: 'nowrap' }}>{duration}</span>}
      <span style={{
        fontSize: 12, fontWeight: 600,
        color: s.color, background: s.bg,
        padding: '4px 10px', borderRadius: 20, whiteSpace: 'nowrap',
      }}>
        {task.status === 'processing' && (
          <span style={{ display: 'inline-block', marginRight: 5, animation: 'spin 1s linear infinite' }}>⟳</span>
        )}
        {s.label}
      </span>
    </div>
  );
}

function Panel({ title, children }) {
  return (
    <div style={{ background: '#1e293b', borderRadius: 12, padding: 18, border: '1px solid #334155' }}>
      <div style={{ fontSize: 13, fontWeight: 600, color: '#94a3b8', marginBottom: 14, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
        {title}
      </div>
      {children}
    </div>
  );
}

function StatCard({ label, value, color }) {
  return (
    <div style={{ background: '#0f172a', borderRadius: 8, padding: '12px 14px', textAlign: 'center' }}>
      <div style={{ fontSize: 22, fontWeight: 700, color }}>{value}</div>
      <div style={{ fontSize: 11, color: '#475569', marginTop: 2 }}>{label}</div>
    </div>
  );
}

function Hl({ children }) {
  return <strong style={{ color: '#f97316' }}>{children}</strong>;
}

const inputStyle = {
  background: '#0f172a', border: '1px solid #475569', borderRadius: 8,
  padding: '9px 12px', color: '#e2e8f0', fontSize: 14, width: '100%', outline: 'none',
};

function btnStyle(bg) {
  return {
    background: bg, border: 'none', borderRadius: 8, padding: '10px 16px',
    color: 'white', fontSize: 14, fontWeight: 600, cursor: 'pointer', width: '100%',
  };
}
