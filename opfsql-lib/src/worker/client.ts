import type { Result, ParamValue } from '../engine/index.js';

// ---------------------------------------------------------------------------
// Message protocol (client side — mirrors worker.ts)
// ---------------------------------------------------------------------------

type OutPayload =
  | { type: 'open';    dbName: string }
  | { type: 'close' }
  | { type: 'exec';    sql: string; params?: ParamValue[] }
  | { type: 'prepare'; sql: string }
  | { type: 'run';     stmtId: number; params?: ParamValue[] }
  | { type: 'free';    stmtId: number };

type InMsg =
  | { id: number; ok: true }
  | { id: number; results: Result[] }
  | { id: number; stmtId: number }
  | { id: number; error: string };

// ---------------------------------------------------------------------------
// RemotePreparedStatement
// ---------------------------------------------------------------------------

export class RemotePreparedStatement {
  constructor(
    private readonly client: WorkerEngine,
    readonly stmtId: number,
  ) {}

  run(params: ParamValue[] = []): Promise<Result> {
    return this.client
      .rpc<{ results: Result[] }>({ type: 'run', stmtId: this.stmtId, params })
      .then((r) => r.results[0]);
  }

  free(): Promise<void> {
    return this.client.rpc({ type: 'free', stmtId: this.stmtId }).then(() => {});
  }
}

// ---------------------------------------------------------------------------
// WorkerEngine — main-thread handle to the worker
// ---------------------------------------------------------------------------

export class WorkerEngine {
  private readonly worker: Worker;
  private readonly pending = new Map<number, { resolve: (v: any) => void; reject: (e: Error) => void }>();
  private seq = 0;

  constructor(workerUrl: string | URL) {
    this.worker = new Worker(workerUrl, { type: 'module' });
    this.worker.onmessage = ({ data }: MessageEvent<InMsg>) => {
      const entry = this.pending.get(data.id);
      if (!entry) return;
      this.pending.delete(data.id);
      if ('error' in data) entry.reject(new Error(data.error));
      else entry.resolve(data);
    };
    this.worker.onerror = (e) => {
      const msg = `Worker error: ${e.message} (${e.filename}:${e.lineno})`;
      console.error(msg, e);
      for (const entry of this.pending.values()) entry.reject(new Error(msg));
      this.pending.clear();
    };
    this.worker.onmessageerror = (e) => {
      const msg = 'Worker message deserialization error';
      console.error(msg, e);
      for (const entry of this.pending.values()) entry.reject(new Error(msg));
      this.pending.clear();
    };
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  open(dbName: string): Promise<void> {
    return this.rpc({ type: 'open', dbName }).then(() => {});
  }

  close(): Promise<void> {
    return this.rpc({ type: 'close' }).then(() => {});
  }

  exec(sql: string, params?: ParamValue[]): Promise<Result[]> {
    return this.rpc<{ results: Result[] }>({ type: 'exec', sql, params }).then((r) => r.results);
  }

  async prepare(sql: string): Promise<RemotePreparedStatement> {
    const { stmtId } = await this.rpc<{ stmtId: number }>({ type: 'prepare', sql });
    return new RemotePreparedStatement(this, stmtId);
  }

  // -------------------------------------------------------------------------
  // RPC plumbing
  // -------------------------------------------------------------------------

  rpc<T = { ok: true }>(payload: OutPayload, timeoutMs = 10_000): Promise<T> {
    return new Promise((resolve, reject) => {
      const id = this.seq++;
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`WorkerEngine: rpc("${payload.type}") timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      this.pending.set(id, {
        resolve: (v) => { clearTimeout(timer); resolve(v); },
        reject:  (e) => { clearTimeout(timer); reject(e); },
      });
      this.worker.postMessage({ id, ...payload });
    });
  }
}
