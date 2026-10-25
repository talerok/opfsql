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
sql/parser   text → syntax tree                  │
sql/binder   syntax tree → logical plan (types,  │
             names, output columns)              │
sql/optimizer logical plan → physical plan       │
functions    the function registry               │
executor     physical plan → rows                │
table        rows ↔ storage: constraints,        │
             indexes, rowids                     │
storage      pages, B-trees, records, keys, WAL, │
             snapshots, read marks               │
schema       the schema model                    │
engine       sessions, transactions, statement   │
             cache                               │
worker       the worker host and the client      │
```

Dependencies point down the request path and never back:

| Module             | May use                                                 |
| ------------------ | ------------------------------------------------------- |
| `values`, `errors` | nothing else                                            |
| `schema`           | `values`, `errors`                                      |
| `storage`          | `values`, `errors`                                      |
| `functions`        | `values`, `errors`                                      |
| `sql/parser`       | `values`, `errors`                                      |
| `sql/binder`       | `sql/parser`, `schema`, `functions`, `values`, `errors` |
| `sql/optimizer`    | `sql/binder`'s plan types, `schema`, `values`, `errors` |
| `table`            | `storage`, `schema`, `functions`, `values`, `errors`    |
| `executor`         | plan types, `table`, `functions`, `values`, `errors`    |
| `engine`           | all of the above                                        |
| `worker`           | `engine`, `errors`, `values`                            |

ESLint enforces the table for every module already rebuilt
(`import-x/no-restricted-paths` in `eslint.config.js`); the rest join as they
are rebuilt.

Everything below `engine` is synchronous and deterministic: given the same
files and statements, it does the same thing. Waiting — for a lock, for a
message — happens only in `engine` and `worker`.

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

## Where the rebuild stands

| Module             | State                                                                                       |
| ------------------ | ------------------------------------------------------------------------------------------- |
| `values`, `errors` | rebuilt                                                                                     |
| `storage`          | format 5 specified; implementation in progress (currently `src/store`, format 4)            |
| `sql/parser`       | to rebuild: statement boundaries, name rules, precedence, literals                          |
| `sql/binder`       | to rebuild: grouping model, scopes, types and conversions, output columns                   |
| `sql/optimizer`    | to rebuild: pure passes, one access-path choice with residual filters, physical plan        |
| `functions`        | to build: one registry for the binder and the executor                                      |
| `executor`         | to rebuild: one expression compiler, join and subquery operators, DML through the planner   |
| `table`            | to build: one write path for rows, indexes and constraints                                  |
| `schema`           | to build: the model, separate from where it is stored                                       |
| `engine`, `worker` | to rebuild: schema from the snapshot, session state machines, statement cache, result shape |
