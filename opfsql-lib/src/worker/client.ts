import type { Result } from "../engine/index.js";
import type { CatalogData, Value } from "../types.js";
import type { RequestPayload, ResponseMessage } from "./protocol.js";

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

  async exec(sql: string, params?: Value[]): Promise<Result[]> {
    const type = "exec" as const;
    const sessionId = this.sessionId;
    const message = { type, sessionId, sql, params };
    const r = await this.engine.rpc<{ results: Result[] }>(message);
    return r.results;
  }

  async prepare(sql: string): Promise<RemotePreparedStatement> {
    const type = "prepare" as const;
    const sessionId = this.sessionId;
    const message = { type, sessionId, sql };
    const { stmtId } = await this.engine.rpc<{ stmtId: number }>(message);
    return new RemotePreparedStatement(this, stmtId);
  }

  /** @internal used by RemotePreparedStatement */
  async run(stmtId: number, params: Value[]): Promise<Result> {
    const type = "run" as const;
    const sessionId = this.sessionId;
    const message = { type, sessionId, stmtId, params };
    const r = await this.engine.rpc<{ results: Result[] }>(message);
    return r.results[0];
  }

  /** @internal used by RemotePreparedStatement */
  async freeStmt(stmtId: number): Promise<void> {
    const type = "free" as const;
    const sessionId = this.sessionId;
    const message = { type, sessionId, stmtId };
    await this.engine.rpc(message);
  }

  async getSchema(): Promise<CatalogData> {
    const type = "schema" as const;
    const sessionId = this.sessionId;
    const message = { type, sessionId };
    const r = await this.engine.rpc<{ schema: CatalogData }>(message);
    return r.schema;
  }

  async disconnect(): Promise<void> {
    const type = "disconnect" as const;
    const sessionId = this.sessionId;
    const message = { type, sessionId };
    await this.engine.rpc(message);
  }
}

// ---------------------------------------------------------------------------
// WorkerEngine — main-thread handle to the worker
// ---------------------------------------------------------------------------

interface Pending {
  resolve: (v: any) => void;
  reject: (e: Error) => void;
}

export class WorkerEngine {
  private readonly worker: Worker;
  private readonly pending = new Map<number, Pending>();
  private seq = 0;

  constructor(workerUrl: string | URL) {
    this.worker = new Worker(workerUrl, { type: "module" });

    this.worker.onmessage = ({ data }: MessageEvent<ResponseMessage>) => {
      const entry = this.pending.get(data.id);

      if (!entry) return;
      this.pending.delete(data.id);

      if ("error" in data) entry.reject(new Error(data.error));
      else entry.resolve(data);
    };

    this.worker.onerror = (e) => {
      const { filename, lineno } = e;
      const message = e.message ?? "unknown";

      const msg = `Worker error: ${message} (${filename}:${lineno})`;
      console.error(msg, e);

      for (const entry of this.pending.values()) {
        entry.reject(new Error(msg));
      }
      this.pending.clear();
    };

    this.worker.onmessageerror = () => {
      const msg = "Worker message deserialization error";
      console.error(msg);

      for (const entry of this.pending.values()) {
        entry.reject(new Error(msg));
      }
      this.pending.clear();
    };
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  async open(dbName: string): Promise<void> {
    const type = "open" as const;
    await this.rpc({ type, dbName });
  }

  async close(): Promise<void> {
    const type = "close" as const;
    await this.rpc({ type: "close" });
  }

  async connect(): Promise<Connection> {
    const type = "connect" as const;
    const resp = await this.rpc<{ sessionId: string }>({ type });
    return new Connection(this, resp.sessionId);
  }

  // -------------------------------------------------------------------------
  // RPC plumbing (internal, used by Connection)
  // -------------------------------------------------------------------------

  /** @internal */
  rpc<T = { ok: true }>(payload: RequestPayload): Promise<T> {
    return new Promise((resolve, reject) => {
      const id = this.seq++;
      this.pending.set(id, { resolve, reject });
      this.worker.postMessage({ id, ...payload });
    });
  }
}
