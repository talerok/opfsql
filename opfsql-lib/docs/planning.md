# Planning and execution

How a statement goes from text to rows, and the contract between the stages.
The types are in `src/sql/plan`; this document says what they mean and which
rules every stage keeps. `docs/architecture.md` has the module map;
`docs/semantics.md` has what values do.

## The pipeline

```
text ──parse──▶ syntax tree ──bind──▶ bound statement ──optimize──▶ physical statement ──compile──▶ operators ──run──▶ rows
                 sql/parser            sql/binder        sql/optimizer                       executor
```

| Stage    | Input → output                         | Knows                                 | Never                           |
| -------- | -------------------------------------- | ------------------------------------- | ------------------------------- |
| parse    | text → syntax tree                     | grammar                               | names, types, schema            |
| bind     | syntax tree + schema → bound statement | names, scopes, types, functions       | indexes, access paths, costs    |
| optimize | bound statement + schema → physical    | indexes, row estimates, rewrite rules | storage, rows, parameter values |
| compile  | physical statement → operator tree     | table layer, expression compiler      | SQL text, schema loading        |
| run      | operators + parameters → rows, changes | the transaction it runs in            | planning                        |

Each stage is a pure function of its inputs, except run. A plan never changes
after it is built; a stage returns a new tree. So a prepared statement keeps
its physical plan for as long as the schema version it was planned for is
current, and compiles it again for each run (compiling is cheap: it builds
closures, it does not plan).

## Column identity

Every relation in a plan — a table reference, a projection, an aggregate's
groups and its aggregates, VALUES, a CTE reference — gets a `tableIndex`
from the binder, unique within the statement. A column is a `ColumnBinding`:
`{ tableIndex, columnIndex }`. Two references to the same table in one
query have two table indexes.

Each plan node's output is an ordered list of bindings, its **layout**.
`layoutOf(node)` in `sql/plan` computes it from the node alone, and the
optimizer and the executor both use it, so they cannot disagree. Compiling
an expression resolves each binding to a position in its input's layout once;
evaluating it reads `row[position]`. Nothing resolves names or bindings per
row.

A column of an enclosing query is a reference with `depth` > 0: the number of
query levels out, where a level is a subquery inside an expression (a
subquery in FROM is not one). The expression compiler keeps a stack of outer
rows while a subquery runs, and a reference reads the row `depth` levels out.

`typesOf(node)`, beside `layoutOf`, gives the static types of a node's
output columns; nodes whose types are not derived from children — values,
set operations, CTE scans, recursive CTEs — carry them.

Negation is always a `not` node around what it negates: NOT IN, NOT EXISTS,
IS NOT NULL and NOT BETWEEN have one shape each, so no rewrite meets two.

## Bound statements

The binder produces one of:

| Statement      | Carries                                                                                 |
| -------------- | --------------------------------------------------------------------------------------- |
| `query`        | a logical plan, and the names and types of its output columns                           |
| `insert`       | the target table, its target columns, a source plan (VALUES or a query), ON CONFLICT    |
| `update`       | the target table, a source plan over it (its rows and their rowids), assignments        |
| `delete`       | the target table, a source plan over it                                                 |
| `create-table` | a table definition from the schema model, without pages yet, and its constraint indexes |
| `create-index` | an index definition, without pages yet                                                  |
| `drop`         | what to drop, and IF EXISTS                                                             |
| `alter-table`  | the table and one action                                                                |

BEGIN, COMMIT and ROLLBACK never reach the binder: the engine handles them.

UPDATE and DELETE are planned like queries: their source is a scan of the
target table with the WHERE clause as a filter, so the optimizer gives them
an access path like any SELECT. There is no second way to find rows.

## Logical plans

What to compute, never how. No logical node mentions an index, a row
estimate or an algorithm.

| Node            | Meaning                                                                             |
| --------------- | ----------------------------------------------------------------------------------- |
| `get`           | the rows of a table: some of its columns, plus the rowid when a DML source needs it |
| `values`        | literal rows (VALUES, and a SELECT without FROM as one row of no columns)           |
| `filter`        | rows of its child for which every condition is TRUE                                 |
| `project`       | one row of expressions per input row                                                |
| `aggregate`     | groups and the aggregates over each, from its child                                 |
| `join`          | inner or left; its ON condition as conjuncts, none meaning every pair (cross)       |
| `order`         | its child's rows in key order                                                       |
| `limit`         | at most `limit` rows after skipping `offset`                                        |
| `distinct`      | its child's rows without duplicates                                                 |
| `set-operation` | UNION, UNION ALL, INTERSECT or EXCEPT of two children                               |
| `cte`           | names a materialized subplan for the scope of its body                              |
| `cte-scan`      | reads a CTE                                                                         |
| `recursive-cte` | an anchor, and a step run until it adds no rows                                     |

A subquery in FROM is its plan under a `project` that gives it its own table
index. There is no derived-table form of `get`.

Join conditions are any expression. Splitting them into equality keys and
the rest is the optimizer's job, and the rest is never dropped.

## Physical plans

How to compute it: access paths and algorithms, chosen by the optimizer.

| Node                               | Algorithm                                                                         |
| ---------------------------------- | --------------------------------------------------------------------------------- |
| `table-scan`                       | every row in rowid order, or reverse                                              |
| `rowid-lookup`                     | rows by rowid: listed rowids, or one rowid range                                  |
| `index-scan`                       | entries of one index within a key range, in key order or reverse, then their rows |
| `index-union`                      | the rows of several index scans, each row once (an OR of indexable conditions)    |
| `index-extreme`                    | MIN or MAX of an indexed expression from one end of the index                     |
| `filter`                           | conditions on its child's rows                                                    |
| `project`                          | as logical                                                                        |
| `hash-join`                        | equality keys hashed on one side; residual conditions on each matching pair       |
| `nested-loop-join`                 | every pair, matched by its conditions                                             |
| `hash-aggregate`                   | groups in a hash table                                                            |
| `sort`, `top-n`                    | full sort; or the first N in key order with a bounded heap                        |
| `limit`                            | as logical                                                                        |
| `hash-distinct`                    | duplicates removed through a hash set                                             |
| `set-operation`                    | as logical, by hashing                                                            |
| `cte`, `cte-scan`, `recursive-cte` | as logical                                                                        |
| `values`                           | as logical                                                                        |

INSERT, UPDATE and DELETE are statements holding a physical source plan, not
plan nodes. Every scan carries `residual`: conditions checked on every row
it returns. A rowid lookup and an index union return each row once; a rowid
lookup returns them in rowid order.

A seek value — an equal value or a range bound — is fixed for one scan: a
constant, a parameter, or a column of an enclosing query, evaluated again
each time the scan starts. So a correlated subquery seeks on its outer row
instead of scanning for it. A NULL seek value matches nothing, as `=` and `<`
with NULL never hold.

`=` compares values as they are; the one conversion a comparison involves
is a parameter's affinity (docs/semantics.md, Comparisons), which the binder
puts on the parameter node itself. So a seek value or a hash key is matched
exactly as `=` matches it, with nothing left to convert: `id = $1` with
`'5'` seeks 5, because the parameter evaluates to 5.

## Rules every optimization keeps

1. **An access path only narrows what is read.** A key range or rowid list
   is chosen from conditions, but those conditions stay in `residual` unless
   the optimizer proves the path enforces exactly that condition for every
   value it can meet (same comparison, same conversion, no NULL difference).
   When in doubt, it stays. Checking a condition twice costs little; dropping
   one returns wrong rows.
2. **A join condition is never dropped.** Equality keys become hash keys;
   everything else in the condition becomes the join's residual, applied to
   each matching pair — for a LEFT join, a pair failing it is not a match, so
   the left row is still emitted with NULLs.
3. **Order comes from one place.** A `sort` is removed only when its child
   already delivers exactly that order: an index scan whose key order,
   direction and NULL placement match every key, or a table scan or rowid
   lookup whose rowid order matches an ORDER BY of the rowid. `top-n` replaces a `sort`
   and the `limit` above it; it never coexists with that `limit`.
4. **Rewrites follow `values`.** A rewrite that folds or simplifies an
   expression evaluates it with the expression compiler, never with its own
   JavaScript semantics, and leaves it alone if evaluating fails (it will
   fail again at run time, with the right error). A rewrite that would
   change a result for some row — NULLs included — is not made. Errors are
   not results: conditions run in any order and may run on rows a later
   step discards, so one plan may fail where another succeeds (`x AND
FALSE` never evaluates `x`; a condition pushed below a join meets rows
   the join drops), as in SQLite and PostgreSQL. Every plan of a statement
   that succeeds returns the same rows.
5. **Parameters are unknown values.** A plan never depends on a parameter's
   value, so it can be reused with others. A parameter can bound a key range;
   it cannot decide which index is used.

## Execution

Compiling turns a physical plan into a tree of operators, closures over
compiled expressions and the table layer. An operator:

```ts
interface Operator {
  /**
   * The next batch of at most `max` rows, or null when there are no more.
   * `max` is what the caller can still use: Infinity when it takes every row.
   */
  next(max: number): Tuple[] | null;
  /** Starts over: for a subquery's next outer row, or a join's inner side. */
  rescan(): void;
}
```

Rows are arrays of values in the operator's layout. Operators pull from their
children (the Volcano model) in batches, and ask for no more rows than they
can use. So an operator evaluates no more of its input than the rows it
returns need — unless its algorithm needs all of it: a sort, an aggregate, a
hash join's build side, a set operation. A row past a LIMIT, or past the one
row an EXISTS needs, is never evaluated and cannot fail the statement, as in
SQLite. Nothing outside can stop a running statement — JavaScript runs it to
the end — so operators call the run's `checkpoint` once per batch, where the
engine can end it.

A subquery is compiled once, with the plan it is part of. A correlated
subquery is rescanned for each outer row, with that row pushed on the outer
stack. A subquery is correlated when it, or a subquery nested in it, reads a
column of an enclosing query or a CTE defined inside an enclosing subquery:
both change from one outer row to the next. Any other subquery runs once and
keeps its result.

UPDATE and DELETE read every target row from their source, and UPDATE
computes every new row, before changing any — each change made by the table layer, which keeps indexes and
constraints with the row — so a change never moves a row into or out of the
scan that is still finding rows. An INSERT whose source reads the target
table reads all of it first, for the same reason; the binder, which resolved
every table the source names, says so on the statement. Any other INSERT
writes each row as its source produces it.

An UPDATE's source produces every column of the table, then the rowid; no
pass prunes them. A DELETE's source produces the rows to delete, the rowid
among their columns: the binder reads every column, as for UPDATE, under the
WHERE that reads them, and the optimizer may narrow it to the rowid alone
once the WHERE is in a scan's residual. ON CONFLICT's
existing and refused rows are each every column of the table, in order.

Each statement of a transaction runs inside a savepoint of its write
transaction. A statement that fails is rolled back to it and the
transaction goes on, as in SQLite: a caller can catch a UNIQUE violation
and carry on. Only a failure that cannot be rolled back — the commit
itself failing — ends the transaction. Savepoints live in one transaction's
memory; nothing of them reaches the WAL or other tabs.

A CTE's rows are computed as far as they are read, so a CTE nothing reads
never runs, and cannot fail the statement.

A recursive CTE is a `cte` whose definition is a `recursive-cte`. The
`recursive-cte`'s own index names its working set, the rows the last step
added, which its step reads; the `cte`'s index names the full result, which
the body reads. The step reads the working set in its FROM clause only, never
from a subquery (as in SQLite): a subquery would run once and keep the first
working set.

## Schema text

`opfsql_schema` stores each table's and index's CREATE statement. It is
generated from the schema model, never edited as text: ALTER TABLE builds the
new model and writes its statement. Parsing and binding a stored statement
gives back the model it was generated from; tests hold the two to it.

A table's constraint indexes are named `opfsql_autoindex_<table>_<n>` in one
order, the order its statement writes them: each column's constraints in
column order, PRIMARY KEY before UNIQUE, then a table-level PRIMARY KEY.
Loading binds stored statements in a mode that accepts the reserved
`opfsql_` names a user statement may not use.
