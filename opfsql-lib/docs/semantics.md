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
| JSON    | plain object or array | only in JSON-typed columns and expressions      |

`NaN` coming in (a parameter, or a function result) becomes NULL, as in
SQLite. A string with a lone surrogate is rejected: it cannot be stored as
UTF-8 without changing it.

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
otherwise compare by the total order above.

Before comparing, the binder converts operands with different types as
SQLite does, by affinity:

- a numeric column (INTEGER, REAL) compared with TEXT converts the text side
  to a number when it is exactly a number, so `id = '5'` matches id 5 and
  `id = 'five'` matches nothing;
- a TEXT column compared with an untyped literal or parameter converts that
  side to text.

Comparing values of different kinds after that falls back to the total
order, so `1 < 'a'` is true.

## Logic

WHERE, HAVING, ON, CASE WHEN, AND, OR and NOT take BOOLEAN operands; any
other type is a bind error. NULL is "unknown":

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

## Arithmetic

`+`, `-`, `*`, `/`, `%` return NULL when either operand is NULL.

- INTEGER `/` INTEGER truncates toward zero (`7 / 2` is `3`); with a REAL
  operand, division is exact. Operands without a static type (ANY, JSON)
  divide as integers when both hold integral values.
- `%` takes the sign of the dividend.
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
- BLOB to TEXT is lowercase hex; TEXT to BLOB parses hex.

Anything else fails with `DATATYPE_MISMATCH`. Unlike SQLite, a cast never
invents a value: `CAST('12abc' AS INTEGER)` is an error, not `12`.

## Departures from SQLite

| opfsql                                                   | SQLite                            | Why                                                                       |
| -------------------------------------------------------- | --------------------------------- | ------------------------------------------------------------------------- |
| BOOLEAN is its own type; `true` ≠ `1`                    | booleans are the integers 1 and 0 | JavaScript has booleans, and callers get back what they stored            |
| JSON is a column type holding parsed values              | JSON is text handled by functions | JavaScript works with objects                                             |
| INTEGER is limited to safe integers                      | 64-bit integers                   | a JavaScript number cannot hold more without losing precision             |
| `7.0 / 2` with untyped operands is `3`                   | `3.5`                             | JavaScript cannot tell `7.0` from `7`; typed REAL operands divide exactly |
| numbers print as JavaScript prints them (`1`, not `1.0`) | `%!.15g`                          | round-trips through JavaScript exactly                                    |
| invalid casts fail                                       | invalid casts give 0 or a prefix  | a guessed value hides a bug                                               |
| WHERE requires a BOOLEAN                                 | any value, by truthiness          | as in PostgreSQL; catches mistakes at bind time                           |
