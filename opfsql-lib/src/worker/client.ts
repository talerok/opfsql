import { Result } from "../engine/index.js";
import { Value } from "../types.js";

// ---------------------------------------------------------------------------
// Message protocol (client side — mirrors worker.ts)
// ---------------------------------------------------------------------------

type OutPayload =
  | { type: "open"; dbName: string }
  | { type: "close" }
  | { type: "exec"; sql: string; params?: Value[] }
  | { type: "prepare"; sql: string }
  | { type: "run"; stmtId: number; params?: Value[] }
  | { type: "free"; stmtId: number };

type InMsg =
  | { id: number; ok: true }
  | { id: number; results: Result[] }
  | { id: number; stmtId: number }
  | { id: number; error: string };

// ---------------------------------------------------------------------------
// RemotePreparedStatement
// ---------------------------------------------------------------------------

export class RemotePreparedStatement {
  constructor(private readonly engine: WorkerEngine, readonly stmtId: number) {}

  run(params: Value[] = []): Promise<Result> {
    return this.engine.run(this.stmtId, params);
  }

  free(): Promise<void> {
    return this.engine.freeStmt(this.stmtId);
  }
}

// ---------------------------------------------------------------------------
// WorkerEngine — main-thread handle to the worker
// ---------------------------------------------------------------------------

export class WorkerEngine {
  private readonly worker: Worker;
  private readonly pending = new Map<
    number,
    { resolve: (v: any) => void; reject: (e: Error) => void }
  >();
  private seq = 0;

  constructor(workerUrl: string | URL) {
    this.worker = new Worker(workerUrl, { type: "module" });
    this.worker.onmessage = ({ data }: MessageEvent<InMsg>) => {
      const entry = this.pending.get(data.id);
      if (!entry) return;
      this.pending.delete(data.id);
      if ("error" in data) entry.reject(new Error(data.error));
      else entry.resolve(data);
    };
    this.worker.onerror = (e) => {
      const msg = `Worker error: ${e.message} (${e.filename}:${e.lineno})`;
      console.error(msg, e);
      for (const entry of this.pending.values()) entry.reject(new Error(msg));
      this.pending.clear();
    };
    this.worker.onmessageerror = (e) => {
      const msg = "Worker message deserialization error";
      console.error(msg, e);
      for (const entry of this.pending.values()) entry.reject(new Error(msg));
      this.pending.clear();
    };
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  open(dbName: string): Promise<void> {
    return this.rpc({ type: "open", dbName }).then(() => void 0);
  }

  close(): Promise<void> {
    return this.rpc({ type: "close" }).then(() => void 0);
  }

  exec(sql: string, params?: Value[]): Promise<Result[]> {
    return this.rpc<{ results: Result[] }>({ type: "exec", sql, params }).then(
      (r) => r.results,
    );
  }

  async prepare(sql: string): Promise<RemotePreparedStatement> {
    const { stmtId } = await this.rpc<{ stmtId: number }>({
      type: "prepare",
      sql,
    });
    return new RemotePreparedStatement(this, stmtId);
  }

  async run(stmtId: number, params: Value[]): Promise<Result> {
    const r = await this.rpc<{ results: Result[] }>({
      type: "run",
      stmtId,
      params,
    });
    return r.results[0];
  }

  async freeStmt(stmtId: number): Promise<void> {
    await this.rpc({ type: "free", stmtId });
  }

  // -------------------------------------------------------------------------
  // RPC plumbing
  // -------------------------------------------------------------------------

  private rpc<T = { ok: true }>(payload: OutPayload): Promise<T> {
    return new Promise((resolve, reject) => {
      const id = this.seq++;
      this.pending.set(id, { resolve, reject });
      this.worker.postMessage({ id, ...payload });
    });
  }
}
