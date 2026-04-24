import { Engine, PreparedStatement } from "../engine/index.js";
import type { Session } from "../engine/session.js";
import type { CatalogData, Value } from "../types.js";
import type { RequestMessage, ResponseMessage } from "./protocol.js";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

interface SessionRecord {
  session: Session;
  stmts: Map<number, PreparedStatement>;
}

let engine: Engine | null = null;
const sessions = new Map<string, SessionRecord>();

let sessionSeq = 0;
let stmtSeq = 0;

function getSession(sessionId: string): Session {
  const rec = sessions.get(sessionId);
  if (!rec) throw new Error(`Session "${sessionId}" not found`);
  return rec.session;
}

function getStmt(sessionId: string, stmtId: number): PreparedStatement {
  const rec = sessions.get(sessionId);
  if (!rec) throw new Error(`Session "${sessionId}" not found`);
  const stmt = rec.stmts.get(stmtId);
  if (!stmt) throw new Error(`Prepared statement ${stmtId} not found`);
  return stmt;
}

function closeSession(sessionId: string): void {
  sessions.get(sessionId)?.session.close();
  sessions.delete(sessionId);
}

function closeAllSessions(): void {
  for (const rec of sessions.values()) rec.session.close();
  sessions.clear();
}

// ---------------------------------------------------------------------------
// Individual message handlers
// ---------------------------------------------------------------------------

async function handleOpen(
  dbName: string,
): Promise<ResponseMessage & { ok: true }> {
  closeAllSessions();
  engine?.close();

  engine = await Engine.open(dbName);
  return { id: 0, ok: true };
}

function handleClose(): ResponseMessage & { ok: true } {
  closeAllSessions();
  engine?.close();
  engine = null;
  return { id: 0, ok: true };
}

function handleConnect(): ResponseMessage & { sessionId: string } {
  if (!engine) throw new Error("Engine not opened");
  const sessionId = `s${sessionSeq++}`;
  const session = engine.createSession();
  const stmts = new Map();

  sessions.set(sessionId, { session, stmts });
  return { id: 0, sessionId };
}

function handleDisconnect(sessionId: string): ResponseMessage & { ok: true } {
  closeSession(sessionId);
  return { id: 0, ok: true };
}

function handleDisconnectAll(): ResponseMessage & { ok: true } {
  closeAllSessions();
  return { id: 0, ok: true };
}

async function handleExec(
  sessionId: string,
  sql: string,
  params?: Value[],
): Promise<ResponseMessage> {
  const session = getSession(sessionId);
  const results = await session.execute(sql, params);
  return { id: 0, results };
}

function handlePrepare(
  sessionId: string,
  sql: string,
): ResponseMessage & { stmtId: number } {
  const stmtId = stmtSeq++;
  const stmt = getSession(sessionId).prepare(sql);
  sessions.get(sessionId)!.stmts.set(stmtId, stmt);
  return { id: 0, stmtId };
}

async function handleRun(
  sessionId: string,
  stmtId: number,
  params?: Value[],
): Promise<ResponseMessage> {
  const stmt = getStmt(sessionId, stmtId);
  const results = [await stmt.run(params)];
  return { id: 0, results };
}

function handleFree(
  sessionId: string,
  stmtId: number,
): ResponseMessage & { ok: true } {
  const session = sessions.get(sessionId);
  session?.stmts.delete(stmtId);
  return { id: 0, ok: true };
}

function handleSchema(
  sessionId: string,
): ResponseMessage & { schema: CatalogData } {
  const session = getSession(sessionId);
  const schema = session.getSchema();
  return { id: 0, schema };
}

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

function isValidMessage(data: unknown): data is RequestMessage {
  return (
    !!data &&
    typeof (data as RequestMessage).id === "number" &&
    typeof (data as RequestMessage).type === "string"
  );
}

async function dispatch(data: RequestMessage): Promise<ResponseMessage> {
  switch (data.type) {
    case "open":
      return handleOpen(data.dbName);
    case "close":
      return handleClose();
    case "connect":
      return handleConnect();
    case "disconnect":
      return handleDisconnect(data.sessionId);
    case "disconnect-all":
      return handleDisconnectAll();
    case "exec":
      return handleExec(data.sessionId, data.sql, data.params);
    case "prepare":
      return handlePrepare(data.sessionId, data.sql);
    case "run":
      return handleRun(data.sessionId, data.stmtId, data.params);
    case "free":
      return handleFree(data.sessionId, data.stmtId);
    case "schema":
      return handleSchema(data.sessionId);
    default:
      throw new Error(
        `Unknown message type: ${(data as { type: string }).type}`,
      );
  }
}

async function handleMessage(
  data: RequestMessage,
  reply: (msg: ResponseMessage) => void,
): Promise<void> {
  if (!isValidMessage(data)) {
    console.error("[opfsql worker] malformed message", data);
    return;
  }

  try {
    const result = await dispatch(data);
    result.id = data.id;
    reply(result);
  } catch (err) {
    const id = data.id;
    const isError = err instanceof Error;
    const error = isError ? err.message : String(err);
    reply({ id, error });
  }
}

// ---------------------------------------------------------------------------
// Message queue + bootstrap
// ---------------------------------------------------------------------------

let queue: Promise<void> = Promise.resolve();

function enqueue(
  data: RequestMessage,
  reply: (msg: ResponseMessage) => void,
): void {
  queue = queue.then(() => handleMessage(data, reply));
}

const scope = self as unknown as Worker;
scope.onmessage = ({ data }: MessageEvent<RequestMessage>) => {
  enqueue(data, (msg) => scope.postMessage(msg));
};
