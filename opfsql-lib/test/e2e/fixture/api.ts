// What the fixture page offers the specs, as plain data that page.evaluate
// can carry back to Playwright.

export interface PlainResult {
  columns: string[];
  rows: unknown[][];
  /** The rows as objects, or null when column names repeat. */
  objects: Record<string, unknown>[] | null;
  changes: number;
  lastInsertRowid: number | null;
}

export interface PlainError {
  name: string;
  code: string | null;
  message: string;
  /** `instanceof OpfsqlError` held in the page. */
  isOpfsqlError: boolean;
  /** `instanceof` the class its name says held in the page. */
  isItsClass: boolean;
  fields: Record<string, unknown>;
}

export type Outcome =
  { ok: true; results: PlainResult[] } | { ok: false; error: PlainError };

export interface FixtureApi {
  /** Opens the database in a new worker, with a connection to run SQL on. */
  open(name: string, options?: { busyTimeout?: number }): Promise<void>;
  /** `open`, reporting how it went. */
  attemptOpen(name: string): Promise<Outcome>;
  /** Runs SQL on the connection; rejects with the error's message. */
  exec(sql: string, parameters?: unknown[]): Promise<PlainResult[]>;
  /** Runs SQL on the connection and reports how it went. */
  attempt(sql: string, parameters?: unknown[]): Promise<Outcome>;
  /** Starts running SQL without waiting for it; `settle` waits. */
  start(id: string, sql: string): void;
  settle(id: string): Promise<Outcome>;
  /** Closes the connection only, as a component unmounting would. */
  disconnect(): Promise<void>;
  close(): Promise<void>;
  /** Opens a database in a worker that runs `url`, reporting the outcome. */
  openIn(url: string): Promise<Outcome>;
  cleanOpfs(name: string): Promise<void>;
}
