# Contracts between layers

What each layer guarantees to the one above it, what the caller must do, who
owns what passes between them and for how long, and which errors come from
where. The pipeline's own contract — bind, optimize, execute — is
docs/planning.md; how tabs share the files is docs/concurrency.md; the
public API is docs/api.md; what values do is docs/semantics.md.

```
page ──messages──▶ worker host ──▶ engine ──▶ sql (parse, bind, optimize) ──▶ executor ──▶ table ──▶ storage
                                     │                                         expressions
                                     └── sessions, transactions, schema cache, results
```

## The rule

A contract is kept by the module that would be hurt if it broke, never by
the caller's habit. Each one below is enforced one of three ways:

- **by type** — a wrong call does not compile;
- **by check** — breaking it throws where the damage would happen, an
  `InternalError` naming the broken contract;
- **by construction** — the module makes the wrong state impossible (it
  catches up itself, it copies what it hands out).

A comment is not one of them. `InternalError` always means a contract
inside opfsql was broken: never a caller's mistake, never the environment.

Everything below the engine is synchronous. The engine's checks of "this
engine holds the write lock" rely on it: nothing runs between them and what
they guard.

## Page ↔ worker

| Contract                                                                                   | Enforced                                                   |
| ------------------------------------------------------------------------------------------ | ---------------------------------------------------------- |
| Calls on one connection run in the order they were made                                    | by construction: the host queues a message when it arrives |
| Every request gets exactly one reply; if the worker dies, every pending call fails         | by check, in the channel                                   |
| Values cross by structured clone: each side owns its copy                                  | by construction                                            |
| Errors cross as data and come back as their own class with their fields                    | by check: a test for every class                           |
| Aborting a call cancels it only while it waits; a running call finishes                    | by construction                                            |
| A call on the shared connection (`db.query`) ends with no transaction open, success or not | by check, in a `finally`                                   |

## Engine ↔ storage

| Contract                                                                                                                          | Enforced                                                                                                                                                                                                         |
| --------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pager operations act on the latest commit in the files                                                                            | by construction: each catches up first (docs/concurrency.md)                                                                                                                                                     |
| Only the write-lock holder appends, checkpoints or changes the intent                                                             | by check: an append reads other tabs' commits first; one made since its transaction began means the lock was not held                                                                                            |
| A page is read only at a pinned snapshot                                                                                          | by check: `Pager.read`                                                                                                                                                                                           |
| A transaction is used only while open                                                                                             | by check: MISUSE after it ends                                                                                                                                                                                   |
| A page read is shared and never changed; only pages from `write()` or `allocate()` are written                                    | by construction: pages are read through read-only views (B-tree pages, the map and overflow readers), only `write()` and `allocate()` pages reach an editor, and the WAL keeps its own copies of committed pages |
| A cursor, a row or an overflow reference read from a tree lives until that tree is changed or rolled back in the same transaction | by check: a version per tree, which every change and rollback moves and cursors and rows compare (`TreeGuard`)                                                                                                   |
| The allocation map never frees a free page nor hands out one in use                                                               | by check: CORRUPT                                                                                                                                                                                                |
| Each statement of a write transaction runs in a savepoint; a failure rolls back to it (docs/planning.md, Execution)               | by construction, in the engine                                                                                                                                                                                   |
| The schema cache holds committed schemas only                                                                                     | by check: a transaction with changes of its own (`hasChanges()`) is refused                                                                                                                                      |

## Engine ↔ SQL pipeline

| Contract                                                                                              | Enforced                                                                                                                                                                         |
| ----------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A statement is planned for one schema and runs only against it                                        | by construction: a transaction plans what it runs, against its own schema                                                                                                        |
| A prepared statement is planned again when the schema version moves                                   | by construction: the plan cache is keyed by schema                                                                                                                               |
| Parameters are values as docs/semantics.md defines them, normalized, and as many as the statement has | by check at the call (`parametersOf`): anything else is MISUSE, and BLOBs and JSON are copied; by type below it: a run takes only `ParameterValues`, which only that check makes |
| BEGIN, COMMIT and ROLLBACK never reach the binder                                                     | by construction                                                                                                                                                                  |
| Binding and optimizing see the same schema; index keys are bound one way for both                     | by construction: one call site, one key binder                                                                                                                                   |

## Executor ↔ table

| Contract                                                                                                                                                       | Enforced                                                        |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| Rows change only through the table's editor, with every index of the table                                                                                     | by construction: `WritableTables.editor` is the only way to one |
| The editor decides a change in full before it writes anything                                                                                                  | by construction                                                 |
| What the editor writes agrees with what it decided: an entry it found absent is inserted, one it found present is deleted                                      | by check: a contradiction is CORRUPT                            |
| A statement reads every row it needs from a tree before it writes that tree; an INSERT streams its source only when the source reads no tree the insert writes | by check: the version per tree                                  |
| `Tables` serve one statement; index keys are compiled once per schema version                                                                                  | by construction                                                 |

The trees an INSERT writes are its table's and its indexes', and
`opfsql_sequence` for a table with AUTOINCREMENT.

## Table ↔ storage

| Contract                                                                                                              | Enforced                                                            |
| --------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| A row or key too large is refused before a byte is written                                                            | by check                                                            |
| The catalog leaves the schema loadable: no column dropped that a key reads, column or expression                      | by check, in the catalog itself, whatever the binder checked before |
| A record's values leave the page as it is read; an overflow value is read when asked for, under the tree-version rule | by construction                                                     |

## Expressions ↔ executor

| Contract                                                                          | Enforced                                     |
| --------------------------------------------------------------------------------- | -------------------------------------------- |
| A run has its parameters and its outer rows; compiled operators belong to one run | by construction                              |
| A subquery marked uncorrelated reads no outer row                                 | by check: it runs with the outer rows sealed |
| A row has a value for every binding of its layout                                 | by check                                     |

## Values in and out of the engine

A value the engine returns is never shared with anything the engine keeps,
and a value it is given is checked and copied where it comes in. The
sources make that so, so results need no copying of their own. Within one
result, one value may fill several cells as the same object (a joined row
in each of its matches): only sharing with what outlives the result is
ruled out (docs/api.md, Results).

| Source                      | Makes its value the caller's by                              |
| --------------------------- | ------------------------------------------------------------ |
| a record                    | decoding it: text and JSON parsed, BLOBs copied off the page |
| a constant of a cached plan | copying a BLOB or JSON value each time it is evaluated       |
| a column default            | copying a BLOB or JSON default each time a row takes it      |
| the schema description      | copying defaults                                             |
| a parameter                 | being checked and copied at the call                         |

## Errors

| Layer          | Raises                                                                                                           |
| -------------- | ---------------------------------------------------------------------------------------------------------------- |
| storage        | CORRUPT, IO_ERROR, STORAGE_FULL, UNSUPPORTED_FORMAT, DATABASE_IN_USE, TOO_LARGE                                  |
| table          | CONSTRAINT_VIOLATION, DATATYPE_MISMATCH (with table and column), CORRUPT                                         |
| parser, binder | SYNTAX_ERROR, SEMANTIC_ERROR; while loading the schema, a stored statement's error is CORRUPT                    |
| expressions    | DATATYPE_MISMATCH, SEMANTIC_ERROR (a scalar subquery with several rows, a malformed JSON path), MISUSE           |
| engine         | BUSY, SERIALIZATION_FAILURE, ABORTED, MISUSE; IO_ERROR when a lock request fails                                 |
| worker         | WORKER_ERROR (a worker that crashed or did not start, a message that could not be read or sent), MISUSE, ABORTED |
| any            | INTERNAL: a broken contract                                                                                      |

Errors from the platform — a DOMException from a file or a lock — become
opfsql errors in `errors.ts`: `toOpfsqlError` at the boundaries of the
engine and the worker host, and `fileFailure` — the same translation, with a
message naming the file — where storage reports a failed write itself. Two
places translate what only they can tell: opening, where a refused file
handle means another tab holds the files (DATABASE_IN_USE), and the write
lock, where a failed lock request is the environment's failure (IO_ERROR).

An error object is never shared between calls: one kept to be thrown again
is thrown as the cause of a new one.
