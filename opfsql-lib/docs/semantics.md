# SQL semantics

How opfsql values behave. This is the contract that the `values` module,
the storage key encoding, the binder, the functions and the executor all
implement; when they disagree, this document is right and the code is a bug.

The reference is SQLite with STRICT tables. opfsql departs from it only
where JavaScript makes another choice clearly better, and every departure is
listed under [Departures from SQLite](#departures-from-sqlite).

## Values

A value is one of:

| Kind    | JavaScript            | Notes                                           |
| ------- | --------------------- | ----------------------------------------------- |
| NULL    | `null`                | `undefined` is never a value                    |
| BOOLEAN | `boolean`             |                                                 |
| NUMBER  | `number`              | never `NaN`; `-0` is stored and returned as `0` |
| TEXT    | `string`              | well-formed UTF-16 (no lone surrogates)         |
| BLOB    | `Uint8Array`          | values returned to callers are copies           |
| JSON    | plain object or array | in JSON and ANY columns, and JSON expressions   |

`NaN` coming in (a parameter, or a function result) becomes NULL, as in
SQLite. A string with a lone surrogate is rejected: it cannot be stored as
UTF-8 without changing it.

## Names

Names of tables, columns and indexes are kept as written and compared
without regard to the case of ASCII letters, as in SQLite: `Users` and
`USERS` are the same table, while `Été` and `été` are two. Tables and indexes
share one namespace. Names starting with `opfsql_` are reserved: the
tables opfsql keeps under them can be read, never written, created or dropped
by a statement.

## Column types

| Declared type                                     | Type    | Holds                                                        |
| ------------------------------------------------- | ------- | ------------------------------------------------------------ |
| `INTEGER`, `INT`, `BIGINT`, `SMALLINT`, `TINYINT` | INTEGER | safe integers, -(2^53 − 1) … 2^53 − 1                        |
| `REAL`, `FLOAT`, `DOUBLE`                         | REAL    | numbers, including ±Infinity                                 |
| `TEXT`, `VARCHAR(n)`, `CHAR(n)`                   | TEXT    | strings; `n` is not enforced                                 |
| `BLOB`                                            | BLOB    | byte arrays                                                  |
| `BOOLEAN`                                         | BOOLEAN | `true` / `false`                                             |
| `JSON`                                            | JSON    | any JSON value: object, array, string, number, boolean, null |
| `ANY` or no type                                  | ANY     | any value, stored as given                                   |

Every column also allows NULL unless it is `NOT NULL` or a `PRIMARY KEY`.

### Assignment

Storing a value in a column (INSERT, UPDATE, DEFAULT) converts it to the
column type when the conversion is lossless and unambiguous, and fails with
`DATATYPE_MISMATCH` otherwise:

| Into    | Accepts                            | Converts                                                                             | Rejects                                                       |
| ------- | ---------------------------------- | ------------------------------------------------------------------------------------ | ------------------------------------------------------------- |
| INTEGER | safe integers                      | numbers with an integral value; text that is exactly a safe integer (`'42'`, `'-7'`) | fractions, unsafe integers, other text, booleans, BLOBs, JSON |
| REAL    | numbers                            | text that is exactly a number (`'1.5'`, `'1e3'`)                                     | other text, booleans, BLOBs, JSON                             |
| TEXT    | strings                            | numbers, as JavaScript prints them                                                   | booleans, BLOBs, JSON                                         |
| BLOB    | byte arrays                        | —                                                                                    | everything else                                               |
| BOOLEAN | booleans                           | `0` → false, `1` → true                                                              | other numbers, text, BLOBs, JSON                              |
| JSON    | objects, arrays, numbers, booleans | text is parsed as JSON text                                                          | invalid JSON text, BLOBs                                      |
| ANY     | everything                         | —                                                                                    | —                                                             |

NULL is accepted by every type; NOT NULL is a separate constraint.

### Primary keys

A table whose primary key is a single INTEGER column stores its rows under
that column's value (the rowid, as in SQLite). Inserting NULL there assigns
the largest rowid in use plus one; with `AUTOINCREMENT`, one more than the
largest ever used, so ids are never reused. Every other primary key is a
UNIQUE NOT NULL constraint over its columns.

## Order and equality

One total order serves ORDER BY, index keys, MIN/MAX, DISTINCT, GROUP BY,
UNION and the comparison operators:

1. NULL
2. BOOLEAN: `false` < `true`
3. NUMBER, numerically
4. TEXT, by Unicode code point (the order of their UTF-8 bytes, which is
   not JavaScript's `<` on UTF-16 strings)
5. BLOB, byte by byte, a shorter prefix first
6. JSON object or array, by its canonical text (keys sorted, no spaces)

Two values are equal when neither precedes the other: `1` equals `1.0`,
`'a'` never equals `'A'`, `true` never equals `1`. DISTINCT, GROUP BY and
UNION treat NULLs as equal to each other; joins, `=` and `IN` never match a
NULL.

ORDER BY puts NULLs first when ascending and last when descending, unless
`NULLS FIRST` / `NULLS LAST` says otherwise.

## Comparisons

`=`, `<>`, `<`, `<=`, `>`, `>=` return NULL when either side is NULL, and
otherwise compare the two values as they are, by the total order above.

The operands must be comparable: numbers with numbers, a type with itself,
and anything with a value whose type only the row tells — a parameter, an
ANY column, a JSON value. `id = '5'` for an INTEGER `id` is a bind error;
write `id = 5`.

A parameter is converted to the type of what it is compared with, as SQLite
applies affinity:

- compared with an INTEGER or REAL operand, text that is exactly a number
  becomes that number, so `id = $1` with `'5'` finds id 5 and with `'five'`
  finds nothing;
- compared with a TEXT operand, a number becomes text, so `name = $1` with
  `5` finds `'5'`.

Compared with several operands at once — the items of `IN`, the bounds of
`BETWEEN`, the `WHEN` values of `CASE x` — it is converted only when they
all agree on the type. Only a parameter that is itself an operand is
converted, not one inside an expression or a subquery (`$1 + 0`,
`(SELECT $1)`). Nothing else is converted: an ANY column's values compare as
they were stored, so `x = 1` does not find `'1'`. Values of
different kinds then compare by the total order, so `1 < 'a'` is true.

## Logic

WHERE, HAVING, ON, CASE WHEN, AND, OR and NOT take BOOLEAN operands; any
other type is a bind error, and a value whose type only a row tells (a
parameter, a JSON value) must be a boolean when it runs. NULL is "unknown":

| AND       | true  | false | NULL  |     | OR        | true | false | NULL |
| --------- | ----- | ----- | ----- | --- | --------- | ---- | ----- | ---- |
| **true**  | true  | false | NULL  |     | **true**  | true | true  | true |
| **false** | false | false | false |     | **false** | true | false | NULL |
| **NULL**  | NULL  | false | NULL  |     | **NULL**  | true | NULL  | NULL |

`NOT NULL` is NULL. A row passes WHERE only when the condition is `true`.

- `x BETWEEN a AND b` is `x >= a AND x <= b`.
- `x IN (list)` is true if some element equals `x`; otherwise NULL if `x` or
  any element is NULL; otherwise false. An empty list (an empty subquery) is
  false whatever `x` is, and `NOT IN` of it is true.
- `x IS y` / `x IS NOT y` compare like `=` / `<>` but treat two NULLs as
  equal and never return NULL.

## Queries

- A query with GROUP BY, HAVING or an aggregate call computes one row per
  group; without GROUP BY, all rows are one group, even when there are none
  (`SELECT count(*) FROM empty` is one row, 0). Above the grouping — the
  select list, HAVING, ORDER BY — an expression may read the GROUP BY
  expressions (matched as written, after binding) and aggregates; any other
  column of the query is an error, as in PostgreSQL.
- Aggregates are not allowed in WHERE, ON, GROUP BY or inside another
  aggregate.
- In ORDER BY, an integer literal is a 1-based position in the select list,
  and a bare name that is an alias (`AS`) of the select list means that
  column; anything else is an expression. After DISTINCT or a compound
  operator (UNION, INTERSECT, EXCEPT), ORDER BY can only name result columns.
  GROUP BY takes positions too, and an alias that no column of the FROM has.
  An alias names one item; two items with the same alias make it ambiguous.
- LIMIT and OFFSET: see [LIMIT and OFFSET](#limit-and-offset).

## Arithmetic

`+`, `-`, `*`, `/`, `%` return NULL when either operand is NULL.

- INTEGER `/` INTEGER truncates toward zero (`7 / 2` is `3`); with a REAL
  operand, division is exact. Operands without a static type (ANY, JSON)
  divide as integers when both hold integral values.
- `%` takes the sign of the dividend; as in SQLite, REAL operands are
  truncated to integers first (`5.5 % 2` is `1`).
- Division or remainder by zero is NULL.
- A result that should be an INTEGER but is not a safe integer fails with
  `INTEGER_OVERFLOW`; so does SUM over integers.
- `||` concatenates text; NULL gives NULL; numbers print as JavaScript
  prints them; booleans as `true` / `false`.

## Text matching

`LIKE` matches `%` (any run of characters) and `_` (one character), with an
optional `ESCAPE` character. It ignores case for ASCII letters only, as in
SQLite.

## Casts

`CAST(x AS type)` converts as assignment does, plus:

- numbers to INTEGER truncate toward zero;
- BOOLEAN to INTEGER or REAL is 0 / 1; to TEXT is `'true'` / `'false'`;
- TEXT `'true'` / `'false'` (any case) to BOOLEAN;
- BLOB to TEXT is lowercase hex; TEXT to BLOB parses hex;
- JSON to TEXT is its JSON text with object keys sorted, so equal JSON values
  cast to equal text.

Anything else fails with `DATATYPE_MISMATCH`. Unlike SQLite, a cast never
invents a value: `CAST('12abc' AS INTEGER)` is an error, not `12`.

## Functions

Functions are resolved when a statement is bound, through one registry
(`src/functions`) that holds each function's arguments, result type and
implementation. An unknown name, a wrong number of arguments, or an
argument whose static type the function does not take fails with
`SEMANTIC_ERROR` before anything runs: `nosuchfn(1)`, `UPPER()`,
`SUBSTR('abc')`, `ABS(text_column)` and `SUM(text_column)` are all bind
errors.

Unless stated otherwise a function returns NULL when an argument is NULL.
Text arguments may be TEXT, numbers (printed as JavaScript prints them)
or booleans (`true` / `false`); numeric arguments INTEGER or REAL. A value
whose type is only known at run time (a parameter) that does not fit fails
with `DATATYPE_MISMATCH` — except text that is exactly a number, which
numeric arguments accept. Lengths and positions count characters (code
points) in text and bytes in BLOBs.

| Function                                     | Result                                                                                                                                                                                          |
| -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `upper(x)`, `lower(x)`                       | `x` in upper / lower case                                                                                                                                                                       |
| `length(x)`                                  | characters of text, bytes of a BLOB                                                                                                                                                             |
| `trim(x [, chars])`, `ltrim`, `rtrim`        | `x` without the characters of `chars` (spaces by default) at both ends / the start / the end                                                                                                    |
| `substr(x, start [, count])`, `substring`    | `count` characters from `start`: 1-based; negative counts from the end; 0 is just before the first, so `substr('hello', 0, 2)` is `'h'`; a negative `count` takes the characters before `start` |
| `replace(x, from, to)`                       | every `from` in `x` replaced by `to`; an empty `from` changes nothing                                                                                                                           |
| `instr(x, y)`                                | 1-based position of the first `y` in `x`, or 0                                                                                                                                                  |
| `concat(x, …)`, `concat_ws(separator, x, …)` | the arguments' text joined; NULL arguments are skipped (a NULL separator gives NULL)                                                                                                            |
| `hex(x)`                                     | uppercase hex of a BLOB's bytes, or of the UTF-8 of `x`'s text; `hex(NULL)` is `''`                                                                                                             |
| `abs(x)`, `sign(x)`                          | absolute value (same type as `x`); -1, 0 or 1                                                                                                                                                   |
| `random()`                                   | a random INTEGER from -2^52 to 2^52, new for every row (never folded into a constant)                                                                                                           |
| `round(x [, digits])`                        | REAL, rounded half away from zero at `digits` (0 to 30) decimals                                                                                                                                |
| `floor(x)`, `ceil(x)`, `ceiling(x)`          | rounded down / up; an INTEGER stays INTEGER                                                                                                                                                     |
| `coalesce(x, y, …)`, `ifnull(x, y)`          | the first non-NULL argument; later arguments are not evaluated                                                                                                                                  |
| `nullif(x, y)`                               | NULL if `x = y`, else `x`                                                                                                                                                                       |
| `iif(condition, x [, y])`                    | `CASE WHEN condition THEN x ELSE y END`                                                                                                                                                         |
| `typeof(x)`                                  | `'null'`, `'integer'`, `'real'`, `'text'`, `'blob'`, `'boolean'` or `'json'`                                                                                                                    |
| `md5(x)`                                     | lowercase hex MD5 of a BLOB's bytes, or of the UTF-8 of `x`'s text (canonical text for JSON)                                                                                                    |
| `json_extract(json, path, …)`                | the value at a SQLite JSON path (`$`, `.key`, `."key"`, `[n]`, `[#-n]`); JSON null and a missing value are NULL; several paths give an array                                                    |
| `json_type(json [, path])`                   | `'object'`, `'array'`, `'integer'`, `'real'`, `'text'`, `'true'`, `'false'` or `'null'`; NULL if the path finds nothing                                                                         |
| `json_array_length(json [, path])`           | the length of an array, 0 for anything else                                                                                                                                                     |
| `json_valid(x)`                              | whether `x` is JSON (TEXT is parsed)                                                                                                                                                            |
| `like(pattern, x [, escape])`                | `x LIKE pattern` (see Text matching)                                                                                                                                                            |

The result of `coalesce`, `ifnull`, `iif` and CASE, and each column of a
UNION, INTERSECT, EXCEPT or VALUES, has the type its values share; INTEGER
with REAL is REAL; any other mix has no static type, and arithmetic on it
decides by the value. So `CASE WHEN x THEN 1 ELSE 'a' END` is allowed, and
`SELECT 1 UNION SELECT '1'` gives two rows.

A JSON argument is a JSON value when its static type is JSON, and JSON
text to parse when it is TEXT; malformed text fails with
`DATATYPE_MISMATCH`, a malformed path with `SEMANTIC_ERROR`.

### Aggregates

`count(*)` counts rows. `count(x)`, `sum`, `total`, `avg`, `min`, `max`
and `group_concat` skip rows whose `x` is NULL; with `DISTINCT` — which
takes exactly one argument — they see each value once, equality being that
of [Order and equality](#order-and-equality).

- `sum` over INTEGERs is an INTEGER, failing with `INTEGER_OVERFLOW` when
  the total leaves the safe range; over REALs it is a REAL, summed with
  Kahan-Babuska-Neumaier compensation as SQLite does. Over no rows it is
  NULL.
- `total` is `sum` as a REAL, as in SQLite: 0, not NULL, over no rows or
  only NULLs, and never an integer overflow.
- `avg` is a REAL; over no rows, NULL.
- `min` and `max` follow the total order, so they work on any type.
- `sum`, `total` and `avg` take numbers only: over TEXT, BOOLEAN, BLOB or
  JSON they are a bind error.
- `group_concat(x [, separator])` is TEXT: the text of the values, as `||`
  converts them (so not BLOBs), joined by the separator — `,` by default,
  a NULL one joining with nothing, each value after the first preceded by
  its own row's separator; over no rows, NULL. The values come in the
  order the rows reach the aggregate, which, as in SQLite, nothing in the
  query fixes; `ORDER BY` inside the call is not supported.

## LIMIT and OFFSET

LIMIT and OFFSET take a constant or a parameter (not an expression, as
SQLite allows), holding an integer, converted as an INTEGER column converts
it: `'5'` is 5. No LIMIT, or a negative one, is no limit; a negative
OFFSET skips nothing. Any other value — NULL, `2.5`, `'five'` — fails with
`DATATYPE_MISMATCH`: a constant when the statement is prepared, anything
else (a parameter, say) each time the query starts, before it reads a row. `LIMIT 0`
returns no rows and reads none.

## Limits

A statement nests at most 200 levels deep (parentheses, subqueries, CASE
inside CASE), and a compound SELECT joins at most 500 SELECTs, as SQLite's
default `SQLITE_MAX_COMPOUND_SELECT`. Past either, the statement fails to
compile with a `SYNTAX_ERROR` or a `SEMANTIC_ERROR`, before it runs.

## Departures from SQLite

| opfsql                                                                               | SQLite                                   | Why                                                                                           |
| ------------------------------------------------------------------------------------ | ---------------------------------------- | --------------------------------------------------------------------------------------------- |
| BOOLEAN is its own type; `true` ≠ `1`                                                | booleans are the integers 1 and 0        | JavaScript has booleans, and callers get back what they stored                                |
| JSON is a column type holding parsed values                                          | JSON is text handled by functions        | JavaScript works with objects                                                                 |
| INTEGER is limited to safe integers                                                  | 64-bit integers                          | a JavaScript number cannot hold more without losing precision                                 |
| `7.0 / 2` with untyped operands is `3`                                               | `3.5`                                    | JavaScript cannot tell `7.0` from `7`; typed REAL operands divide exactly                     |
| numbers print as JavaScript prints them (`1`, not `1.0`)                             | `%!.15g`                                 | round-trips through JavaScript exactly                                                        |
| invalid casts fail                                                                   | invalid casts give 0 or a prefix         | a guessed value hides a bug                                                                   |
| WHERE requires a BOOLEAN                                                             | any value, by truthiness                 | as in PostgreSQL; catches mistakes at bind time                                               |
| `upper` / `lower` convert all of Unicode                                             | ASCII letters only (without ICU)         | JavaScript's case mapping is complete and free                                                |
| `typeof` also answers `'boolean'` and `'json'`                                       | no such types                            | they are types here                                                                           |
| JSON functions return JSON values                                                    | JSON text                                | as JSON columns do                                                                            |
| `md5` exists                                                                         | no `md5`                                 | kept from earlier opfsql versions                                                             |
| a scalar subquery with several rows fails                                            | the first row is used                    | a guessed value hides a bug                                                                   |
| `id = '5'` for an INTEGER `id` is a bind error                                       | the text is converted to a number        | catches mistakes at bind time; a parameter is still converted                                 |
| an ANY column compared with a typed column compares values as stored                 | the ANY side takes the other's affinity  | only parameters are converted, so an index seek or a hash join always finds what a scan finds |
| UPDATE computes every row's new values from the table as it was before the statement | each row sees the rows updated before it | the result does not depend on the order rows are found in, as in PostgreSQL                   |
| a column neither grouped nor aggregated is an error                                  | a "bare column" of some row              | as in PostgreSQL; a guessed value hides a bug                                                 |
| assigning one column twice in an UPDATE (or DO UPDATE) is a bind error               | the rightmost assignment wins            | a repeated assignment is a mistake                                                            |
