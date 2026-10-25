# API

opfsql runs in a dedicated worker: synchronous file access exists only
there. The page talks to it through an asynchronous client. This document is
the contract of that client, of the worker that serves it, and of the engine
underneath for code that already runs in a worker.

| Where             | Entry point                   | Use                                     |
| ----------------- | ----------------------------- | --------------------------------------- |
| the page          | `openDatabase(name, options)` | everything, normally                    |
| your worker file  | `serveDatabase()`             | one line that makes the worker a server |
| inside any worker | `Engine.open(name, options)`  | the engine without messages (advanced)  |

## Quick start

```ts
// db-worker.ts — the worker's script
import { serveDatabase } from "opfsql";
serveDatabase();
```

```ts
// main.ts
import { openDatabase } from "opfsql";

const db = await openDatabase("app", {
  worker: new Worker(new URL("./db-worker.ts", import.meta.url), {
    type: "module",
  }),
});

await db.execute(`
  CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
  CREATE INDEX users_name ON users (name);
`);

const { lastInsertRowid } = await db.query(
  "INSERT INTO users (name) VALUES ($1)",
  ["Ada"],
);
const result = await db.query("SELECT id, name FROM users WHERE id = $1", [
  lastInsertRowid,
]);
result.columns; // ["id", "name"]
result.rows; // [[1, "Ada"]]
result.objects(); // [{ id: 1, name: "Ada" }]

await db.transaction(async (tx) => {
  await tx.query("UPDATE users SET name = $1 WHERE id = $2", ["Ada L.", 1]);
  await tx.query("INSERT INTO users (name) VALUES ($1)", ["Grace"]);
});

await db.close();
```

## Opening a database

`openDatabase(name, options): Promise<Database>`

| Option                | Default        | Meaning                                                                                                         |
| --------------------- | -------------- | --------------------------------------------------------------------------------------------------------------- |
| `worker`              | required       | a dedicated module worker whose script calls `serveDatabase()` (any `WorkerHandle`)                             |
| `busyTimeout`         | `5000`         | milliseconds a write waits for the write lock before failing with `BUSY`; `Infinity` waits for good             |
| `pageSize`            | `32768`        | page size of a new database, a power of two; an existing database keeps its own                                 |
| `checkpointThreshold` | `1000`         | WAL frames after which a commit copies the WAL into the database file                                           |
| `onCheckpointError`   | `console.warn` | called with the error when a background checkpoint fails; the commit that triggered it has succeeded regardless |

The database owns the worker: closing the database, or failing to open it,
terminates the worker. A worker serves one database — handing one worker to a
second `openDatabase` fails with `MISUSE` — so open another database with
another worker. In the worker, `serveDatabase(scope?)` serves the database on
the worker's global scope, or on `scope` (anything with `postMessage` and
`addEventListener("message")`).

Opening fails with `DATABASE_IN_USE` when another tab has the database open
in a browser that lets only one tab hold its files (all but Chromium), with
`UNSUPPORTED_FORMAT` for files of another format version, and with `INTERNAL`
when the worker cannot start.

## Database

| Member                          | Returns               | Does                                                    |
| ------------------------------- | --------------------- | ------------------------------------------------------- |
| `query(sql, params?, options?)` | `Promise<Result>`     | runs one statement                                      |
| `execute(script, options?)`     | `Promise<Result[]>`   | runs a script of statements                             |
| `prepare(sql)`                  | `Promise<Statement>`  | prepares one statement for repeated runs                |
| `transaction(work, options?)`   | `Promise<T>`          | runs `work` in a transaction on a connection of its own |
| `connect()`                     | `Promise<Connection>` | opens a connection                                      |
| `schema()`                      | `Promise<Schema>`     | describes the tables and indexes                        |
| `close()`                       | `Promise<void>`       | closes everything and terminates the worker             |
| `[Symbol.asyncDispose]()`       | `Promise<void>`       | `close()`, for `await using`                            |

The database's own `query`, `execute` and `prepare` share one connection,
which anything in the page may be using at the same time. So a call on it
must end every transaction it begins: `db.execute("BEGIN; …; COMMIT")` works,
while `db.query("BEGIN")` rolls back and fails with `MISUSE`. Transactions
that span several calls belong to `transaction()` or to a connection of your
own.

## Connections

A connection is one session in the worker, with its own transaction and its
own view of the data. Calls on one connection run one at a time, in the order
they were made, whether or not the caller awaits each one. Calls on different
connections run independently: readers never wait, writers wait only for the
write lock.

| Member                                                 | Same as on `Database`, but on this connection |
| ------------------------------------------------------ | --------------------------------------------- |
| `query`, `execute`, `prepare`, `transaction`, `schema` |                                               |
| `close()`, `[Symbol.asyncDispose]()`                   | closes the connection                         |

Closing a connection rolls back its transaction, fails the calls still
waiting on it with `ABORTED`, and closes its statements; calls made after
that fail with `MISUSE`. A connection that is garbage-collected without being
closed is closed for you, so an abandoned transaction cannot hold the write
lock or a snapshot (and with it every tab's checkpoints) forever — but when
that happens is up to the garbage collector: close connections, preferably
with `await using`.

## Statements, scripts and parameters

`query(sql, params)` runs exactly one statement; a text with several fails
with `MISUSE`. Parameters are `$1`, `$2`, … and `params[0]`, `params[1]`, …;
a statement takes exactly as many as its highest `$n`, and any other number
fails with `MISUSE` (a missing parameter is never silently NULL).

`execute(script)` runs several statements separated by semicolons and returns
one result per statement. It takes no parameters. Its statements behave as
follows:

1. The whole script is parsed first: a syntax error anywhere runs nothing.
2. Outside a transaction, a script that has no `BEGIN`, `COMMIT` or
   `ROLLBACK` is one transaction: every statement takes effect or none does.
   If it writes, it takes the write lock before its first statement.
3. A script that controls transactions itself runs its statements in order;
   outside `BEGIN … COMMIT` each statement commits on its own. When a
   statement fails, the rest are skipped and a transaction the script began
   is rolled back, so the call leaves no transaction and no lock behind.
4. Inside a transaction begun by an earlier call, a failing statement aborts
   that transaction (see [Transactions](#transactions)).

The error of a statement that failed in `execute` has `statementIndex`, the
position of that statement in the script, counting from zero.

## Prepared statements

`prepare(sql)` parses and binds the statement once, so an unknown table or
column fails at `prepare`, not at the first run.

| Member                     | Does                                                |
| -------------------------- | --------------------------------------------------- |
| `columns`                  | the names of the result columns (empty for a write) |
| `query(params?, options?)` | runs the statement and returns its `Result`         |
| `close()`                  | releases it; later runs fail with `MISUSE`          |
| `[Symbol.asyncDispose]()`  | `close()`                                           |

A statement runs on the connection it was prepared on, in that connection's
order. When the schema changes — in this tab or another — the statement binds
itself again before its next run; it never goes stale. If the change made it
invalid (its table was dropped, say), that run fails with the error binding
would give.

## Results

```ts
class Result {
  readonly columns: readonly string[];
  readonly rows: readonly (readonly Value[])[];
  readonly changes: number;
  readonly lastInsertRowid: number | null;
  objects(): Record<string, Value>[];
}
```

| Field             | Holds                                                                                       |
| ----------------- | ------------------------------------------------------------------------------------------- |
| `columns`         | one name per result column, in order, also when there are no rows; names may repeat         |
| `rows`            | one array per row, its values in the order of `columns`                                     |
| `changes`         | rows the statement inserted, updated or deleted; 0 for other statements                     |
| `lastInsertRowid` | the rowid of the last row this statement inserted, or `null` if it inserted none            |
| `objects()`       | the rows as objects keyed by column name; fails with `MISUSE` when two columns share a name |

A column is named by its alias, else by the column it reads, else after the
expression (`column2`, `count_star`). Write `SELECT a.id AS a_id, b.id AS b_id`
to get distinct names for `objects()`; `rows` never loses a column.

Values are `null`, booleans, numbers, strings, `Uint8Array` (BLOB) and JSON
objects and arrays, as [semantics.md](semantics.md) defines them. INTEGER
values are safe integers, so they are plain numbers.

## Transactions

| Statement                 | Takes the write lock          | Snapshot taken at       |
| ------------------------- | ----------------------------- | ----------------------- |
| `BEGIN`, `BEGIN DEFERRED` | at the first write            | `BEGIN`                 |
| `BEGIN IMMEDIATE`         | at `BEGIN`, waiting if needed | `BEGIN`, after the lock |

A transaction reads one snapshot throughout — data and schema alike — so it
never sees commits made after it began. One writer at a time holds the write
lock, across every connection and every tab. A deferred transaction that
writes after another writer has committed since its snapshot fails with
`SERIALIZATION_FAILURE`: its reads may be stale. `BEGIN IMMEDIATE` never
does, because it waits for the lock before taking its snapshot.

When a statement in a transaction fails, the transaction is aborted: its
changes are discarded and its lock and snapshot released at once, and every
statement but `ROLLBACK` fails with `TRANSACTION_ABORTED`. `COMMIT` ends an
aborted transaction too, failing with `TRANSACTION_ABORTED` so that the loss
is never silent. `COMMIT` and `ROLLBACK` outside a transaction do nothing;
`BEGIN` inside one fails with `MISUSE`.

`transaction(work, options?)` begins a transaction, runs `work`, and commits;
if `work` throws, it rolls back and rethrows. A statement error that `work`
catches and swallows still aborts the transaction, so the commit then fails
with `TRANSACTION_ABORTED`. On a `Database` it runs on a fresh connection,
which `work` receives and which is closed afterwards, so that other code using
the database is never drawn into the transaction. On a `Connection`, `work`
receives that connection. Transactions do not nest.

| Option   | Default       | Meaning                                                                     |
| -------- | ------------- | --------------------------------------------------------------------------- |
| `mode`   | `"immediate"` | `"immediate"` takes the write lock first; `"deferred"` suits read-only work |
| `signal` |               | aborts waiting for the lock                                                 |

Retry `BUSY` and `SERIALIZATION_FAILURE` by running the whole transaction
again.

## Waiting and cancelling

Readers never wait for writers. A write waits for the write lock, which is
granted in the order it was requested, the same within a tab and across tabs.
A write that waits longer than `busyTimeout` fails with `BUSY`.

Every call takes `options.signal`, an `AbortSignal`. Aborting it cancels the
call while it waits — behind earlier calls on its connection, or for the
write lock — and the call fails with `ABORTED` (an `AbortError`). A call that
is already running finishes, and its real outcome is what it reports.

## Schema

`schema()` describes the schema the connection sees: inside a transaction,
the transaction's snapshot with its own uncommitted changes; otherwise the
latest commit.

```ts
interface Schema {
  readonly tables: readonly TableInfo[];
}
interface TableInfo {
  readonly name: string;
  readonly columns: readonly ColumnInfo[];
  readonly indexes: readonly IndexInfo[];
}
interface ColumnInfo {
  readonly name: string;
  readonly type: string; // the declared type: "INTEGER", "TEXT", …
  readonly nullable: boolean;
  readonly primaryKey: boolean;
  readonly unique: boolean;
  readonly autoIncrement: boolean;
  readonly defaultValue: Value;
}
interface IndexInfo {
  readonly name: string;
  readonly unique: boolean;
  readonly expressions: readonly string[]; // "name", "lower(email)", …
}
```

Indexes opfsql creates for primary keys are not listed: the columns say
`primaryKey`.

## Errors

Every error opfsql throws is an `OpfsqlError` with a stable `code`; branch on
the code, the message is for people. Errors cross the worker boundary intact:
the same class (so `instanceof` works in the page), its code, message and
fields, and its `cause`.

| Code                    | Class                     | Fields             | Means                                                             |
| ----------------------- | ------------------------- | ------------------ | ----------------------------------------------------------------- |
| `SYNTAX_ERROR`          | `SqlSyntaxError`          | `offset`, `length` | the SQL does not parse                                            |
| `SEMANTIC_ERROR`        | `SemanticError`           |                    | unknown names, wrong types or clauses                             |
| `NOT_SUPPORTED`         | `NotSupportedError`       | `feature`          | valid SQL opfsql does not implement                               |
| `CONSTRAINT_VIOLATION`  | `OpfsqlError`             |                    | a UNIQUE, PRIMARY KEY or NOT NULL constraint                      |
| `DATATYPE_MISMATCH`     | `DatatypeMismatchError`   |                    | a value does not fit a type                                       |
| `INTEGER_OVERFLOW`      | `IntegerOverflowError`    |                    | an integer result is not a safe integer                           |
| `TOO_LARGE`             | `OpfsqlError`             |                    | a row, key or value is larger than opfsql stores                  |
| `BUSY`                  | `BusyError`               | `timeout`          | the write lock was not granted within `busyTimeout`               |
| `SERIALIZATION_FAILURE` | `SerializationError`      |                    | another writer committed since this transaction's snapshot        |
| `TRANSACTION_ABORTED`   | `TransactionAbortedError` |                    | an earlier statement failed; only `ROLLBACK` works                |
| `DATABASE_IN_USE`       | `DatabaseInUseError`      | `database`         | another tab has the database open, and this browser allows one    |
| `ABORTED`               | `AbortError`              |                    | the call's signal was aborted, or its connection closed           |
| `IO_ERROR`              | `OpfsqlError`             |                    | reading or writing the files failed                               |
| `STORAGE_FULL`          | `OpfsqlError`             |                    | the browser refused to store more                                 |
| `CORRUPT`               | `OpfsqlError`             |                    | the files are damaged                                             |
| `UNSUPPORTED_FORMAT`    | `OpfsqlError`             |                    | the files have another format version                             |
| `MISUSE`                | `MisuseError`             |                    | the API was used wrongly: a closed connection, several statements |
| `INTERNAL`              | `InternalError`           |                    | a bug in opfsql, or the worker failed                             |

Errors of statements run by `execute` also have `statementIndex`. Where a
code has no class of its own yet, the error is an `OpfsqlError` whose `cause`
holds the details.

## Closing

`db.close()` rejects new calls with `MISUSE`, fails calls still waiting with
`ABORTED`, lets a running call finish, rolls back open transactions, releases
the locks, closes the files and terminates the worker. Calling it again
waits for the same close. A tab that disappears without closing loses its
locks with it, and the next writer recovers whatever was committed.

| Object       | States                                    |
| ------------ | ----------------------------------------- |
| `Database`   | opening → open → closing → closed; failed |
| `Connection` | open → closing → closed                   |
| `Statement`  | open → closed                             |

A database whose worker fails (it cannot load, throws outside a request, or
sends a reply that cannot be read) moves to failed: every pending call and
every later call fails with `INTERNAL`, and the worker is terminated.

`await using` needs a runtime with explicit resource management, or a
polyfill of `Symbol.asyncDispose` loaded before opfsql.

## The engine inside a worker

Code that already runs in a worker can use the engine directly and skip the
messages. It has the same semantics under other names:

| Client       | Engine              |
| ------------ | ------------------- |
| `Database`   | `Engine`            |
| `Connection` | `Session`           |
| `Statement`  | `PreparedStatement` |

```ts
import { Engine } from "opfsql";

const engine = await Engine.open("app", { busyTimeout: 1000 });
const session = engine.connect();
await session.query("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
await session.transaction(async () => {
  await session.query("INSERT INTO t (v) VALUES ($1)", ["a"]);
});
await session.close();
await engine.close();
```

`Engine.open(name, options)` takes the options of `openDatabase` but
`worker`. `engine.connect()` returns a `Session`, which also has
`begin(options)` (deferred unless `mode` says otherwise), `commit()`,
`rollback()` and `inTransaction`; `PreparedStatement.close()` is synchronous.
A session that is garbage-collected unclosed is closed by its engine.
`engine.checkpoint()` copies the WAL into the database file as far as open
snapshots allow; `engine.checkIntegrity()` returns the problems it finds, an
empty list for a sound database. `serveDatabase()` is built from these and
nothing else.

The engine reads every statement's schema from the snapshot the statement
reads its data from, and a writer pins that snapshot only once it holds the
write lock; so DDL in one tab and writes in another can never interleave on
an outdated schema.
