import { Result } from "../engine/index.js";
import type { CatalogData } from "../store/types.js";
import { Value } from "../types.js";

// ---------------------------------------------------------------------------
// Message protocol (client side — mirrors worker.ts)
// ---------------------------------------------------------------------------

type OutPayload =
  | { type: "open"; dbName: string }
  | { type: "close" }
  | { type: "connect" }
  | { type: "disconnect"; sessionId: string }
  | { type: "exec"; sessionId: string; sql: string; params?: Value[] }
  | { type: "prepare"; sessionId: string; sql: string }
  | { type: "run"; sessionId: string; stmtId: number; params?: Value[] }
  | { type: "free"; sessionId: string; stmtId: number }
  | { type: "schema"; sessionId: string };

type InMsg =
  | { id: number; ok: true }
  | { id: number; results: Result[] }
  | { id: number; sessionId: string }
  | { id: number; stmtId: number }
  | { id: number; schema: CatalogData }
  | { id: number; error: string };

// ---------------------------------------------------------------------------
// RemotePreparedStatement
// ---------------------------------------------------------------------------

export class RemotePreparedStatement {
  constructor(
    private readonly connection: Connection,
    readonly stmtId: number,
  ) {}

  run(params: Value[] = []): Promise<Result> {
    return this.connection.run(this.stmtId, params);
  }

  free(): Promise<void> {
    return this.connection.freeStmt(this.stmtId);
  }
}

// ---------------------------------------------------------------------------
// Connection — per-session client handle
// ---------------------------------------------------------------------------

export class Connection {
  constructor(
    private readonly engine: WorkerEngine,
    readonly sessionId: string,
  ) {}

  exec(sql: string, params?: Value[]): Promise<Result[]> {
    return this.engine.rpc<{ results: Result[] }>({
      type: "exec",
      sessionId: this.sessionId,
      sql,
      params,
    }).then((r) => r.results);
  }

  async prepare(sql: string): Promise<RemotePreparedStatement> {
    const { stmtId } = await this.engine.rpc<{ stmtId: number }>({
      type: "prepare",
      sessionId: this.sessionId,
      sql,
    });
    return new RemotePreparedStatement(this, stmtId);
  }

  /** @internal used by RemotePreparedStatement */
  async run(stmtId: number, params: Value[]): Promise<Result> {
    const r = await this.engine.rpc<{ results: Result[] }>({
      type: "run",
      sessionId: this.sessionId,
      stmtId,
      params,
    });
    return r.results[0];
  }

  /** @internal used by RemotePreparedStatement */
  async freeStmt(stmtId: number): Promise<void> {
    await this.engine.rpc({
      type: "free",
      sessionId: this.sessionId,
      stmtId,
    });
  }

  async getSchema(): Promise<CatalogData> {
    const r = await this.engine.rpc<{ schema: CatalogData }>({
      type: "schema",
      sessionId: this.sessionId,
    });
    return r.schema;
  }

  disconnect(): Promise<void> {
    return this.engine.rpc({
      type: "disconnect",
      sessionId: this.sessionId,
    }).then(() => void 0);
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

  async connect(): Promise<Connection> {
    const { sessionId } = await this.rpc<{ sessionId: string }>({
      type: "connect",
    });
    return new Connection(this, sessionId);
  }

  // -------------------------------------------------------------------------
  // RPC plumbing (internal, used by Connection)
  // -------------------------------------------------------------------------

  /** @internal */
  rpc<T = { ok: true }>(payload: OutPayload): Promise<T> {
    return new Promise((resolve, reject) => {
      const id = this.seq++;
      this.pending.set(id, { resolve, reject });
      this.worker.postMessage({ id, ...payload });
    });
  }
}
