# opfsql

Lightweight SQL engine written in TypeScript. ~40 KB gzipped, zero native dependencies. Runs in browser workers via OPFS.

## Quick Start

```ts
import { WorkerEngine } from "opfsql";

const engine = new WorkerEngine(new URL("./worker.ts", import.meta.url));
await engine.open("my-db");
const conn = await engine.connect();

await conn.exec(`
  CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE
  )
`);

await conn.exec("INSERT INTO users (name, email) VALUES ($1, $2)", [
  "Alice",
  "alice@example.com",
]);

const [result] = await conn.exec("SELECT * FROM users");
// result.type === "rows"
// result.rows === [{ id: 1, name: "Alice", email: "alice@example.com" }]

await conn.disconnect();
await engine.close();
```

## API

### `WorkerEngine`

| Method                | Returns               | Description                       |
| --------------------- | --------------------- | --------------------------------- |
| `engine.open(dbName)` | `Promise<void>`       | Open database in worker           |
| `engine.connect()`    | `Promise<Connection>` | Create a new connection (session) |
| `engine.close()`      | `Promise<void>`       | Close the database                |

### `Connection`

| Method                    | Returns                            | Description                                       |
| ------------------------- | ---------------------------------- | ------------------------------------------------- |
| `conn.exec(sql, params?)` | `Promise<Result[]>`                | Execute one or more SQL statements                |
| `conn.prepare(sql)`       | `Promise<RemotePreparedStatement>` | Prepare a single statement for repeated execution |
| `conn.getSchema()`        | `Promise<CatalogData>`             | Get current database schema                       |
| `conn.disconnect()`       | `Promise<void>`                    | Close the connection                              |

### `RemotePreparedStatement`

| Method              | Returns           | Description                      |
| ------------------- | ----------------- | -------------------------------- |
| `stmt.run(params?)` | `Promise<Result>` | Execute with optional parameters |
| `stmt.free()`       | `Promise<void>`   | Release the prepared statement   |

### `Result`

```ts
type Result =
  | { type: "rows"; rows: Row[] }
  | { type: "ok"; rowsAffected: number };

type Row = Record<string, Value>;
type Value = string | number | boolean | null | JsonValue | Uint8Array;
```

### Parameters

Use `$1`, `$2`, etc. for parameterized queries:

```ts
await conn.exec("SELECT * FROM users WHERE id = $1 AND name = $2", [1, "Alice"]);
```

## Supported SQL

### Data Types

`INTEGER`, `BIGINT`, `SMALLINT`, `FLOAT`/`REAL`, `DOUBLE`, `TEXT`/`VARCHAR`/`CHAR`, `BLOB`, `BOOLEAN`, `JSON`

### Statements

| Statement                       | Notes                                                              |
| ------------------------------- | ------------------------------------------------------------------ |
| `SELECT`                        | `DISTINCT`, column aliases, table aliases, `SELECT` without `FROM` |
| `INSERT`                        | Multi-row, `INSERT...SELECT`, `ON CONFLICT DO NOTHING/UPDATE`      |
| `UPDATE`                        | Expressions in `SET`                                               |
| `DELETE`                        | With or without `WHERE`                                            |
| `CREATE TABLE`                  | `IF NOT EXISTS`, column and table-level constraints                |
| `ALTER TABLE`                   | `ADD COLUMN`, `DROP COLUMN`                                        |
| `DROP TABLE`                    | `IF EXISTS`                                                        |
| `CREATE INDEX`                  | `UNIQUE`, multi-column, expression-based, `IF NOT EXISTS`          |
| `DROP INDEX`                    |                                                                    |
| `BEGIN` / `COMMIT` / `ROLLBACK` | Snapshot isolation, autocommit for standalone statements           |
| `EXPLAIN`                       | Shows optimized logical plan for any statement                     |

### Clauses

| Clause                | Notes                                                  |
| --------------------- | ------------------------------------------------------ |
| `WHERE`               | All comparison, logical, and arithmetic operators      |
| `ORDER BY`            | Multi-column, `ASC`/`DESC`, `NULLS FIRST`/`NULLS LAST` |
| `GROUP BY`            | Multiple columns                                       |
| `HAVING`              |                                                        |
| `LIMIT` / `OFFSET`    |                                                        |
| `JOIN`                | `INNER`, `LEFT`, `CROSS`; `ON` and `USING`             |
| `UNION` / `UNION ALL` | Chainable                                              |
| `WITH` (CTE)          | Multiple CTEs, `WITH RECURSIVE`                        |

### Expressions

| Expression  | Examples                                                 |
| ----------- | -------------------------------------------------------- |
| Arithmetic  | `+`, `-`, `*`, `/`, `%`                                  |
| Comparison  | `=`, `!=`/`<>`, `<`, `<=`, `>`, `>=`                     |
| Logical     | `AND`, `OR`, `NOT`                                       |
| String      | `\|\|` (concatenation)                                   |
| Pattern     | `LIKE`, `NOT LIKE`                                       |
| Range       | `BETWEEN`, `NOT BETWEEN`                                 |
| Set         | `IN (...)`, `NOT IN (...)`                               |
| Null check  | `IS NULL`, `IS NOT NULL`                                 |
| Conditional | `CASE WHEN ... THEN ... ELSE ... END`                    |
| Cast        | `CAST(expr AS type)`                                     |
| Subquery    | Scalar, `EXISTS`, `NOT EXISTS`, `ANY`, `ALL`, correlated |
| JSON path   | `data.name`, `items[0]`, `data.items[0].title`           |

### Functions

**Aggregate:** `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`, `SUM`, `AVG`, `MIN`, `MAX`

**String:** `UPPER`, `LOWER`, `LENGTH`, `TRIM`, `LTRIM`, `RTRIM`, `SUBSTR`/`SUBSTRING`, `REPLACE`, `CONCAT`

**Math:** `ABS`, `ROUND`, `FLOOR`, `CEIL`/`CEILING`

**Hash:** `MD5`

**Utility:** `COALESCE`, `NULLIF`, `TYPEOF`

### Constraints

`PRIMARY KEY`, `UNIQUE`, `NOT NULL`, `DEFAULT`, `AUTOINCREMENT`, `FOREIGN KEY` (parsed, not enforced)

### Other

- Comments: `-- single line`, `/* block */`
- Quoted identifiers: `"my column"`
- Blob literals: `x'DEADBEEF'`
- Boolean literals: `TRUE`, `FALSE`

## Architecture

```
Lexer --> Parser --> Binder --> Optimizer --> Executor
                       |                        |
                    Catalog              +------+------+
                                         |             |
                                     RowManager   IndexManager
                                         |             |
                                     TableBTree   IndexBTree
                                         |             |
                                         +------+------+
                                                |
                                            PageStore
                                        (WAL + page cache)
                                                |
                                              OPFS
```

- **Lexer / Parser** — SQL text to AST
- **Binder** — resolves names, types, and constraints against the catalog
- **Optimizer** — rewrites logical plan (predicate pushdown, join reordering, index selection)
- **Executor** — pull-based iterator model (volcano), executes physical operators
- **RowManager / IndexManager** — logical layer over B-trees, handles row CRUD and index maintenance
- **TableBTree / IndexBTree** — B+ trees for row storage (keyed by rowId) and secondary indexes (composite keys → rowId buckets)
- **PageStore** — page-level I/O with LRU cache and WAL for crash safety
- **OPFS** — Origin Private File System (synchronous access handle in workers)

## Not Supported (vs SQLite)

| Feature                                                      | Status                       |
| ------------------------------------------------------------ | ---------------------------- |
| Window functions (`OVER`, `PARTITION BY`, `ROW_NUMBER`, ...) | Not supported                |
| `RIGHT JOIN` / `FULL OUTER JOIN`                             | Not supported                |
| Views (`CREATE VIEW`)                                        | Not supported                |
| Triggers                                                     | Not supported                |
| Savepoints                                                   | Not supported                |
| `CHECK` constraints                                          | Not supported                |
| `ATTACH` / `DETACH`                                          | Not supported                |
| `PRAGMA`                                                     | Not supported                |
| Collation                                                    | Not configurable             |
| Type affinity                                                | Strict logical types instead |
