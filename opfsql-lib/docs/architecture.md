# Architecture

opfsql is an embedded SQL database for the browser: it runs in a worker,
stores its data in the origin's private file system, and lets several tabs
share one database where the browser allows it. This document is the map of
the code and the rules that keep it that way. [semantics.md](semantics.md)
defines what values mean, [file-format.md](file-format.md) what the files
hold.

## Constraints first

The design follows from what the web platform allows:

| Constraint                                                                                  | Consequence                                                                                                      |
| ------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Synchronous file access (`createSyncAccessHandle`) exists only in dedicated workers         | the database runs in a worker; the main thread talks to it through messages, so the public API is asynchronous   |
| Several handles on one file only in Chromium (`readwrite-unsafe`); one per origin elsewhere | every tab opens the files itself in Chromium; elsewhere a second tab gets `DATABASE_IN_USE` — no leader election |
| Tabs share no memory (SharedArrayBuffer needs cross-origin isolation)                       | tabs coordinate only through the files and Web Locks                                                             |
| `flush()` costs 10–240 µs; reading a page costs 1–10 µs                                     | one WAL append and one flush per commit; reads go to the file freely                                             |
| Turning bytes into JavaScript objects costs far more than reading them                      | pages are searched and edited as bytes; values are decoded one at a time, on demand                              |
| `new Function` breaks under a strict Content Security Policy                                | expressions compile to closures, never to source code                                                            |
| One thread per worker, no parallel query execution                                          | a pull-based executor that stops as soon as LIMIT or the caller does                                             |
| A tab can disappear at any moment                                                           | every state on disk is recoverable; locks die with their tab                                                     |

## Modules

```
values ──────────────────────────────────────────┐
errors ──────────────────────────────────────────┤ used by everything
                                                 │
sql/parser    text → syntax tree                 │
sql/binder    syntax tree → bound statement      │
              (names, scopes, types)             │
sql/plan      the types between the stages:      │
              bound expressions, logical and     │
              physical plans, bound statements   │
sql/optimizer logical plan → physical plan       │
expressions   bound expressions → closures       │
functions     the function registry              │
executor      physical plan → rows               │
table         rows ↔ storage: constraints,       │
              indexes, rowids                    │
storage       pages, B-trees, records, keys,     │
              WAL, snapshots, read marks         │
schema        the schema model                   │
engine        sessions, transactions, statement  │
              cache, schema loading, DDL         │
worker        the worker host and the client     │
```

`docs/planning.md` is the contract between binder, optimizer and executor.

Dependencies point down the request path and never back:

| Module             | May use                                                                       |
| ------------------ | ----------------------------------------------------------------------------- |
| `values`, `errors` | nothing else                                                                  |
| `schema`           | `values`, `errors`                                                            |
| `storage`          | `values`, `errors`                                                            |
| `functions`        | `values`, `errors`                                                            |
| `sql/parser`       | `values`, `errors`                                                            |
| `sql/plan`         | `schema`, `functions`, `values`, `errors`                                     |
| `expressions`      | `sql/plan`, `functions`, `values`, `errors`                                   |
| `sql/binder`       | `sql/parser`, `sql/plan`, `schema`, `functions`, `values`, `errors`           |
| `sql/optimizer`    | `sql/plan`, `expressions`, `schema`, `values`, `errors`                       |
| `table`            | `storage`, `schema`, `functions`, `values`, `errors`                          |
| `executor`         | `sql/plan`, `expressions`, `table`, `functions`, `schema`, `values`, `errors` |
| `engine`           | all of the above                                                              |
| `worker`           | `engine`, `errors`, `values`                                                  |

ESLint enforces the table for every module already rebuilt
(`import-x/no-restricted-paths` in `eslint.config.js`); the rest join as they
are rebuilt.

Everything a statement does below `engine` is synchronous and
deterministic: given the same files and statements, it does the same thing.
Waiting — for a lock, for a message — happens only in `engine` and `worker`.
Opening and closing a database are the exception, since the browser's file
and lock APIs are asynchronous.

## Rules for the code

**One meaning, one place.** Comparison, equality, NULL logic, conversions
and ordering come from `values`; nothing compares SQL values with `<` or
`===`. Functions and their types come from `functions`. Errors are
`OpfsqlError` subclasses with a code from `errors.ts`; nothing throws a plain
`Error`.

**Data, not mutation.** Syntax trees, plans, expressions and schemas are
`readonly` and never changed after they are built: a pass returns a new tree.
That is what lets a prepared statement keep its plan.

**Exhaustive dispatch.** A union is handled with a `switch` that covers every
case, so adding a case fails to compile until every consumer handles it. No
`Map<string, handler>` with casts, no `any`.

**Names are words.** `index`, `context`, `expression`, not `idx`, `ctx`,
`expr`; no `I` or `Sync` prefixes; files in kebab-case. A module's public
surface is its `index.ts`; other modules import from there.

**Unsupported means an error.** SQL that opfsql does not implement fails with
`NOT_SUPPORTED`; nothing is parsed and silently ignored.

**Small units.** Cognitive complexity at most 11, functions under 50 lines,
at most 4 parameters (pass an object beyond that).

**Strict everywhere.** Lint errors in code written before these rules are
listed in `eslint-suppressions.json`, which only shrinks. Rebuilt modules
join `tsconfig.strict.json`, which adds `noUncheckedIndexedAccess`,
`exactOptionalPropertyTypes` and the other strict flags; when it covers
everything, it becomes the only configuration.

## How code reads

Lint keeps units small; it does not make them pleasant to read. New code is
reviewed against these as well.

**Flat control flow.** Guard clauses return early and the main path runs
down the left margin. A loop inside a loop, or a branch inside a loop inside
a branch, is a function waiting for a name: extract the inner part and call
it by what it does (`claimSlot`, `largestMovable`, `readFrame`).

**One level per function.** A function either strings named steps together
or does one step's arithmetic, not both: `checkpoint()` is three calls, and
the details live in `backfill()` and `restartWal()`.

**Named intermediate results.** A condition of more than one clause gets a
boolean name (`isComplete`, `isGroupStart`); a magic number gets a constant
(`FRAME_EPOCH_OFFSET`, not `8`); a repeated idiom gets a helper
(`readUint32(bytes, offset)`, which gets `byteOffset` right in one place).

**Ternaries pick values.** A ternary chooses between two values and fits on
one line. No nested ternaries, no ternary followed by an `if` on the same
condition: when the branches do not fit one line, write `if` and `return`.

**Decide, then act.** Work out what to do before changing anything, so a
failure leaves nothing half done: a row that cannot fit is rejected before
any overflow page is written (`valuesToMove`).

**Data says what it is.** A discriminated union with named fields
(`PreparedField`) rather than numbers whose meaning is in a comment; one
function translates to and from the numbers on disk.

**Walks are generators.** Code that walks a structure (`framesFrom`,
`chainPages`, `BTree.ascending`) yields results, and callers loop over them
instead of driving the walk themselves.

**Comments say why.** Names say what code does. A comment states a
constraint, a choice or an invariant the code cannot show: "the browser
holds the lock until this promise settles".

## Where the rebuild stands

| Module             | State                                                                                           |
| ------------------ | ----------------------------------------------------------------------------------------------- |
| `values`, `errors` | rebuilt                                                                                         |
| `schema`           | built: the model and name rules                                                                 |
| `storage`          | built: format 5 — pages, WAL, B-trees, rows, index entries, the schema table, integrity check   |
| `table`            | built: one write path for rows, constraints, rowids, AUTOINCREMENT and index entries            |
| `sql/parser`       | rebuilt: Pratt expressions, one name rule, NOT_SUPPORTED for what is parsed but not implemented |
| `functions`        | built: one registry for the binder and the compiler                                             |
| `expressions`      | built as `executor/evaluate`: the one compiler; moves to its own module with the plan types     |
| `sql/plan`         | contract written; binder, optimizer and executor move to it                                     |
| `sql/binder`       | to rebuild on `sql/plan`: grouping model, scopes, DDL to the schema model                       |
| `sql/optimizer`    | to rebuild on `sql/plan`: pure passes, access paths with residuals, physical plan               |
| `executor`         | to rebuild on `sql/plan` and `table`: operators with rescan, DML through the planner            |
| `engine`, `worker` | rebuilt on the old storage (branch `rebuild/engine`); to move to format 5 and the schema table  |
