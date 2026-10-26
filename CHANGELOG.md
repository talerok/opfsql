# Changelog

## 0.1.0 — unreleased

A rebuild. 0.1.0 has a new file format, a new API, and SQL semantics that
are written down ([docs/semantics.md](opfsql-lib/docs/semantics.md)) and
enforced. Read "Upgrading from 0.0.2" before you install it: it cannot open
databases that 0.0.2 wrote.

### Upgrading from 0.0.2

- **Old databases do not open.** 0.0.2 wrote file format 3; 0.1.0 reads only
  format 5. Opening a 0.0.2 database fails with `UnsupportedFormatError`
  (`code` `UNSUPPORTED_FORMAT`, `version` 3), and the file is left as it was.
  Nothing converts it. To keep the data, read it out with 0.0.2 and insert
  it into a new database with 0.1.0. Both versions use the same file names,
  `name.opfsql` and `name.opfsql-wal` (0.1.0 adds a third,
  `name.opfsql-shm`), so delete the old files or open the new database
  under another name.
- **The page opens the database through a worker.** The worker's script is
  one line, `serveDatabase()` from `"opfsql/worker"`. The page calls
  `openDatabase(name, { worker })` from `"opfsql"` and gets a `Database`
  with `query`, `execute`, `prepare`, `transaction`, `connect`, `schema` and
  `close` ([docs/api.md](opfsql-lib/docs/api.md)). The page's bundle does
  not include the engine.
- **The engine moved and changed.** `Engine` is exported from
  `"opfsql/worker"`, not from `"opfsql"`.
  - `engine.createSession()` is now `engine.connect()`.
  - A session's `execute(sql, params)` is split in two: `query(sql, params)`
    runs one statement, and `execute(script)` runs a script and takes no
    parameters.
  - `getSchema()` is now `schema()`.
  - `PreparedStatement.run(params)` is now `query(params)`.
  - `engine.close()` is now asynchronous.
  - `Engine.create(backend)`, `OPFSSyncStorage` and `EngineError` are gone.
- **Results changed shape.** 0.0.2 returned `{ type: "rows", rows }`, with
  each row an object, or `{ type: "ok", rowsAffected }`. Every statement now
  returns a `Result`, which has:
  - `columns`;
  - `rows`, one array of values per row;
  - `changes`;
  - `lastInsertRowid`;
  - `objects()`, which gives the rows as objects.
- **Every error is an `OpfsqlError` with a stable `code`.** 0.0.2 threw
  `EngineError` and unexported classes, so callers could only tell errors
  apart by their message.

### File format 5

- The pages hold bytes, and the engine searches and edits them in place.
  0.0.2 encoded each page as CBOR objects. The `cbor-x` dependency is gone,
  and the package has no runtime dependencies.
- A row larger than a page moves its largest values to overflow pages. In
  0.0.2, a row over about 32 KB failed at checkpoint with "exceeds page
  capacity".
- The page size of a new database is a power of two from 4096 to 65536,
  with 8192 by default (0.0.2 used 32768). Set it with the `pageSize`
  option.
- Each commit is one append to the WAL and one flush. A checkpoint copies
  the WAL into the database file after `checkpointThreshold` frames (default
  1000).
- The layout is documented in
  [docs/file-format.md](opfsql-lib/docs/file-format.md).
  `Engine.checkIntegrity()` verifies it.

### Tabs

- In Chromium, up to 64 tabs share one database. Each opens the files
  itself (`readwrite-unsafe` handles), and the tabs coordinate through Web
  Locks and a third file, `name.opfsql-shm`. A tab's open snapshot holds
  back checkpoints that would overwrite what it reads. A 65th tab gets
  `DatabaseInUseError` with `reason` `"too-many-tabs"`.
- Firefox and Safari let one tab hold the files. A second tab gets
  `DatabaseInUseError` with `reason` `"one-tab-only"`.
- One writer at a time holds the write lock, across all tabs. A write waits
  for it for up to `busyTimeout` (default 5000 ms), then fails with
  `BusyError`. In 0.0.2, a second writer in the same engine failed at once
  with "database is locked".
- A tab that disappears loses its locks, and the next writer recovers what
  was committed.
- [docs/concurrency.md](opfsql-lib/docs/concurrency.md) states the protocol
  and its invariants. Tests check it by interleaving the tabs' file
  operations.

### Transactions

- **A failed statement is rolled back alone, as in SQLite.** The
  transaction stays open with everything done before the statement, and
  keeps its lock and snapshot. In 0.0.2, a failed statement rolled back the
  whole transaction. Later statements then failed with "current transaction
  is aborted", and `COMMIT` rolled back.
- A transaction reads one snapshot of data and schema throughout.
  `BEGIN IMMEDIATE` takes the write lock first. A deferred transaction that
  writes after another writer has committed fails with `SERIALIZATION_FAILURE`;
  roll it back and run it again.
- `execute(script)` without `BEGIN`, `COMMIT` or `ROLLBACK` is one
  transaction: every statement takes effect, or none does.
- `transaction(work)` runs `work` in a transaction on a connection of its
  own, commits it, and rolls it back if `work` throws.
- A prepared statement plans itself again after a schema change, in any
  tab. In 0.0.2, it failed with "prepared statement is stale".
- Every call takes an `AbortSignal`. Aborting it cancels the call while it
  waits, and the call fails with `ABORTED`.
- A connection that is garbage-collected without being closed is closed for
  you.

### SQL semantics

[docs/semantics.md](opfsql-lib/docs/semantics.md) is the contract. Its
"Departures from SQLite" table lists every place where opfsql differs from
SQLite with STRICT tables. Compared with 0.0.2, the changes below were each
checked by running the statement on both versions.

- **Types are enforced when a value is stored.**
  - A conversion is applied only when it is lossless: `'42'` stored in an
    INTEGER column becomes `42`, `5` in TEXT becomes `'5'`, and `1` in
    BOOLEAN becomes `true`.
  - Anything else fails with `DATATYPE_MISMATCH`: `2.5` in INTEGER, or
    `'yes'` in BOOLEAN. 0.0.2 stored all of these as given.
  - The new `ANY` type stores any value as it is.
- **INTEGER is a safe integer.** A result outside ±(2⁵³ − 1) fails with
  `INTEGER_OVERFLOW`, where 0.0.2 returned an imprecise number.
- **Arithmetic follows SQLite.** `7 / 2` is `3`, where 0.0.2 gave `3.5`.
  `5.5 % 2` is `1`, `round(-2.5)` is `-3`, and `substr('hello', 0, 2)` is
  `'h'`.
- **NULLs sort first in ascending order and last in descending order**, as
  in SQLite, unless `NULLS FIRST` or `NULLS LAST` says otherwise. 0.0.2 did
  the opposite.
- **Text sorts and compares by Unicode code point**, the order of its UTF-8
  bytes. 0.0.2 used JavaScript's UTF-16 order, which puts `'😀'` before
  `'｡'`.
- **Comparisons still convert only parameters**, as in 0.0.2: `id = $1`
  with `'5'` finds id 5, and `id = '5'` written in the SQL is a bind error.
  An ANY column compares its values as they are stored.
- **WHERE, HAVING and ON need a BOOLEAN.** `WHERE 1` is a bind error;
  0.0.2 accepted it and returned no rows.
- **LIKE ignores the case of ASCII letters**, as in SQLite. In 0.0.2 it was
  case-sensitive.
- **`CAST` never invents a value.** `CAST('12abc' AS INTEGER)` fails, where
  0.0.2 returned `12`.
- **Mistakes fail when the statement is bound.**
  - An unknown function, or a call with the wrong number of arguments, fails
    with `SEMANTIC_ERROR`. 0.0.2 returned NULL.
  - A statement must get exactly as many parameters as it uses: a missing
    parameter is never NULL, and an extra one is never ignored.
  - A parameter that is not a SQL value, such as `undefined` or a `Date`,
    fails with `MISUSE`.
- **Result columns are named as written.**
  - A column is named by its alias, then by the column it reads, then by
    the expression's text: `count(*)` and `id + 1`, where 0.0.2 used
    `count_star` and `column0`.
  - Columns with the same name are all kept in `rows`. `objects()` fails
    with `MISUSE` rather than drop one.
- **LIMIT and OFFSET take a constant or a parameter.**
  - `'5'` counts as 5.
  - A negative LIMIT means no limit, and a negative OFFSET skips nothing.
  - NULL or `2.5` fails with `DATATYPE_MISMATCH`.
  - 0.0.2 took only a non-negative integer literal, and accepted `2.5` as 2.
- **`typeof` returns `'integer'` and `'real'`**, where 0.0.2 returned
  `'number'`.
- **SQL that opfsql does not implement fails with `NOT_SUPPORTED`.**
  Nothing is parsed and then ignored. A table-level `FOREIGN KEY`, which
  0.0.2 accepted and did not enforce, now fails, and so does a column's
  `REFERENCES`.
- **`EXPLAIN` shows the physical plan** that the statement runs: its scans,
  index lookups and join algorithms.

New in SQL:

- `RIGHT JOIN`;
- the `ANY` column type;
- `instr`, `concat_ws`, `hex`, `sign`, `random`, `ifnull`, `iif`, `total`
  and `group_concat`;
- `json_extract`, `json_type`, `json_array_length` and `json_valid`;
- LIMIT and OFFSET from parameters.

Fixed:

- A multi-row INSERT now enforces UNIQUE between its own rows. 0.0.2 let
  `INSERT INTO t VALUES (1, 'a'), (2, 'a')` into a UNIQUE column.
- `INTERSECT` and `EXCEPT` return the right rows, each once, under the
  first SELECT's column names. In 0.0.2 they returned wrong rows.

### Errors

| Code                    | Class                                       |
| ----------------------- | ------------------------------------------- |
| `SYNTAX_ERROR`          | `SqlSyntaxError`                            |
| `SEMANTIC_ERROR`        | `SemanticError`                             |
| `NOT_SUPPORTED`         | `NotSupportedError`                         |
| `CONSTRAINT_VIOLATION`  | `ConstraintError`                           |
| `DATATYPE_MISMATCH`     | `DatatypeMismatchError`                     |
| `INTEGER_OVERFLOW`      | `IntegerOverflowError`                      |
| `TOO_LARGE`             | `IndexKeyTooLargeError`, `RowTooLargeError` |
| `BUSY`                  | `BusyError`                                 |
| `SERIALIZATION_FAILURE` | `SerializationError`                        |
| `DATABASE_IN_USE`       | `DatabaseInUseError`                        |
| `ABORTED`               | `AbortError`                                |
| `IO_ERROR`              | `IoError`                                   |
| `STORAGE_FULL`          | `StorageFullError`                          |
| `CORRUPT`               | `CorruptError`                              |
| `UNSUPPORTED_FORMAT`    | `UnsupportedFormatError`                    |
| `MISUSE`                | `MisuseError`                               |
| `WORKER_ERROR`          | `WorkerError`                               |
| `INTERNAL`              | `InternalError`                             |

All of these classes are new. They are all exported from `"opfsql"` and
keep their class, code and fields across the worker boundary.
[docs/api.md](opfsql-lib/docs/api.md#errors) lists their fields.

- `WORKER_ERROR` means the worker crashed, did not start (including not
  answering within 10 seconds), or sent a message that could not be read.
  The database then fails every call.
- `STORAGE_FULL` means the browser refused to store more data because the
  origin's quota is used up. Other storage failures are `IO_ERROR`.
- There is no `TRANSACTION_ABORTED`. It never shipped in a release, but
  earlier drafts of these docs described it. A failed statement no longer
  aborts its transaction.

### Performance

These figures come from the scenarios in
[bench/scenarios.ts](opfsql-lib/bench/scenarios.ts), run in Node on memory
files: 0.0.2 (the `v0.0.2` sources, through an adapter to the current API)
and then 0.1.0, back to back. Each is the median of 15 runs after 5 warmup
runs. They were taken on an Apple M4 on 2026-10-26, under a load average of
14–26 from other work, so only large ratios mean much.

| Scenario                                    | 0.0.2 ms | 0.1.0 ms |      Change |
| ------------------------------------------- | -------: | -------: | ----------: |
| keyset pages by primary key, 20 a page      |   2167.5 |     13.8 | 157× faster |
| keyset pages by an indexed column           |   3245.7 |     44.4 |  73× faster |
| join: a user's orders ×20                   |    170.9 |      2.9 |  59× faster |
| join: an order with its user ×200           |     71.5 |      4.6 |  16× faster |
| delete by primary key, one commit each ×100 |    177.3 |     19.3 |   9× faster |
| update by primary key, one commit each ×100 |    186.0 |     21.3 |   9× faster |
| secondary index: account by email ×1 000    |     28.4 |     17.5 | 1.6× faster |
| prepared point query ×10 000                |    117.3 |     82.1 | 1.4× faster |
| full scan ×10                               |     42.6 |     51.8 | 1.2× slower |
| GROUP BY + COUNT + AVG ×10                  |     52.6 |     69.9 | 1.3× slower |
| insert 100 rows, one commit each            |     16.1 |     22.4 | 1.4× slower |
| update all 10 000 rows in one statement     |     65.1 |    118.4 | 1.8× slower |
| select range (BETWEEN) on no index ×100     |    209.2 |    533.0 | 2.5× slower |
| JSON: insert 1 000 documents                |     10.4 |     27.2 | 2.6× slower |
| JSON: read back 1 000 documents ×10         |      3.5 |     16.2 | 4.6× slower |
| JSON: filter on a path ×10                  |      3.4 |     18.0 | 5.3× slower |

- **Faster.** A join looks up the other table by its key or an index for
  each row, where 0.0.2 hashed a full scan of it. A keyset page reads 20
  rows backward from the cursor, where 0.0.2 read the whole range and
  sorted it. `EXPLAIN` shows both plans.
- **Slower.** Reading many rows decodes each value from bytes on every
  query. 0.0.2 kept every page committed since the last checkpoint in
  memory as decoded JavaScript objects. 0.1.0 has no page cache across
  transactions, by design (see Known limits). JSON objects and arrays are
  stored as text, and parsed when they are read.
- **Bulk inserts** in one transaction varied too much from run to run under
  this load to compare: 0.1.0's median was between 12 and 94 ms in three
  runs, and 0.0.2's between 11 and 19 ms in two.
- **In Chromium on real OPFS,** a point query by primary key took 35 µs and
  a commit about 1 ms. [bench/README.md](opfsql-lib/bench/README.md) has
  the full table.

### Known limits

- **The planner has no statistics.** Every table counts as 10 000 rows. So
  the planner ranks access paths well (a rowid, a unique key, an equality, a
  range, then a full read), but it cannot tell a small table from a large
  one when it picks the join order.
- **No page cache across transactions, by design.** Reads go to the file
  every time. A cache that outlived a transaction could go stale behind
  another tab's back.
- **Some work is held in memory.** Sorts, hash joins, hash aggregates,
  DISTINCT and set operations keep their rows in memory. A write
  transaction keeps every page it changes in memory until it commits.
- **A running statement cannot be interrupted.** An `AbortSignal` cancels a
  call only while it waits. `close()` lets a running call finish and
  terminates the worker after 10 seconds.
- **Index keys do not overflow.** A key longer than about a quarter of a
  page fails with `TOO_LARGE`.
- **Several tabs share a database only in Chromium.**
- **Not supported**, each failing with `NOT_SUPPORTED` or a syntax error:
  - window functions;
  - `FULL OUTER JOIN` and `NATURAL JOIN`;
  - `FOREIGN KEY`, `REFERENCES` and `CHECK`;
  - a table-level `UNIQUE`;
  - views and triggers;
  - `SAVEPOINT`, `ATTACH`, `PRAGMA`, `VACUUM`, `REPLACE` and `INSERT OR …`;
  - `RETURNING` and `UPDATE … FROM`;
  - `ALTER TABLE … RENAME`;
  - `CREATE TABLE … AS`, `WITHOUT ROWID`, `STRICT`, generated columns and
    temporary tables;
  - `VALUES` as a query and `DEFAULT VALUES`;
  - `COLLATE`, `GLOB`, `LIKE … ESCAPE`, `IS [NOT] DISTINCT FROM`, and `IS`
    with anything but NULL;
  - `FILTER` on aggregates;
  - date and time functions, `group_concat` and `printf`.
