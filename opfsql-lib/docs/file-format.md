# File format

Format version 5. A database `name` is three files in the origin's private
file system (OPFS):

| File              | Holds                                              |
| ----------------- | -------------------------------------------------- |
| `name.opfsql`     | the database: pages of a fixed size                |
| `name.opfsql-wal` | committed changes not yet copied into the database |
| `name.opfsql-shm` | the snapshots open in each tab (read marks)        |

Integers are big-endian unless stated otherwise. Every page is self-describing
by its first byte, so the integrity check and debugging tools can read any
page on its own.

## Why it looks like this

The format follows from what the browser makes cheap and what it makes
expensive. Measured per 4–32 KB page in Chromium, Firefox and WebKit (October
2026):

| Operation                                            | Cost      |
| ---------------------------------------------------- | --------- |
| read a page through an OPFS sync access handle       | 0.6–10 µs |
| `flush()` after a write                              | 10–240 µs |
| decode a page of JavaScript objects (format 4, CBOR) | 5–112 µs  |
| encode it back                                       | 15–302 µs |

Reading bytes is 25–100 times cheaper than turning them into objects and
back. So pages are bytes that the B-tree searches and edits in place: a
lookup decodes only the one record it returns, and a commit writes pages
without encoding them. Index keys are byte strings whose order is the SQL
order, so comparing two keys never decodes either. `flush()` dominates a
commit, so every commit is one append to the WAL and one flush.

## Main file

Page `n` starts at byte `n × pageSize`. The file holds exactly the pages that
exist after the last checkpoint; pages allocated since then live only in the
WAL until the next one.

| Page   | Contents                                           |
| ------ | -------------------------------------------------- |
| 0      | header                                             |
| 1      | allocation map of group 0                          |
| 2      | root of the `opfsql_schema` table                  |
| others | B-tree, overflow and allocation-map pages, or free |

### Header (page 0)

| Offset | Size | Field                                                                    |
| ------ | ---- | ------------------------------------------------------------------------ |
| 0      | 8    | magic `OPFSQLDB`                                                         |
| 8      | 4    | format version, `5`                                                      |
| 12     | 4    | page size: a power of two from 4096 to 65536                             |
| 16     | 4    | schema version: incremented by every transaction that changes the schema |
| 20     | …    | zero                                                                     |

The page count is not stored: it is the file size divided by the page size,
and every WAL commit records the page count as of that commit.

### Page types

The first byte of every page other than page 0:

| Byte   | Page                           |
| ------ | ------------------------------ |
| `0x00` | free (never written, or freed) |
| `0x01` | table B-tree, interior         |
| `0x02` | table B-tree, leaf             |
| `0x03` | index B-tree, interior         |
| `0x04` | index B-tree, leaf             |
| `0x05` | overflow                       |
| `0x06` | allocation map                 |

## Allocation maps

Pages are grouped: a group holds `G = (pageSize − 8) × 8` pages, and group `g`
covers pages `g × G` to `(g + 1) × G − 1`. Its allocation map is page
`g × G + 1`:

| Offset | Size         | Field                                       |
| ------ | ------------ | ------------------------------------------- |
| 0      | 1            | `0x06`                                      |
| 1      | 7            | zero                                        |
| 8      | pageSize − 8 | one bit per page of the group, `1` = in use |

The bit for page `g × G + i` is bit `i mod 8` (least significant first) of
byte `8 + ⌊i / 8⌋`. Page 0, allocation-map pages and page 2 are always in use.

New pages are taken lowest-numbered first, so data packs toward the start of
the file, and a checkpoint truncates the file after the highest page in use.
Freeing a page clears its bit; the page's old content is not erased.

## B-tree pages

Tables and indexes are B+trees: entries live in leaves, interior pages hold
separator keys. A tree is known by its root page, which never moves: when the
root splits, its content moves to two new pages and the root becomes their
parent.

Pages are not merged, but a delete that empties a leaf frees it: its parent
drops the separator that led to it, and the neighbouring child takes over its
keys. Every interior page keeps at least one separator and every leaf stays
at one depth, so a parent with a single separator keeps an empty child until
its other child is empty too, and then goes itself; a root left with one
child takes that child's content, and the tree is a level less deep.

### Layout

| Offset  | Size  | Field                                                |
| ------- | ----- | ---------------------------------------------------- |
| 0       | 1     | page type (`0x01`–`0x04`)                            |
| 1       | 1     | zero                                                 |
| 2       | 2     | cell count `n`                                       |
| 4       | 2     | start of the cell area; `0` stands for 65536         |
| 6       | 2     | zero                                                 |
| 8       | 4     | interior pages only: the rightmost child page        |
| 8 or 12 | 2 × n | cell pointers: the offset of each cell, in key order |

Cells fill the page from its end toward the pointers. There are no gaps in
the cell area: removing a cell shifts the cells stored before it (at lower
offsets) toward the end of the page by its size and zeroes the bytes freed,
so the free space is always the one run between the last pointer and the
start of the cell area, and it is all zero.

### Cells

| Page           | Cell                                                  |
| -------------- | ----------------------------------------------------- |
| table leaf     | rowid (zigzag varint) · record size (varint) · record |
| table interior | left child (u32) · rowid (zigzag varint)              |
| index leaf     | key size (varint) · key                               |
| index interior | left child (u32) · key size (varint) · key            |

In an interior page, the left child of cell `i` holds the keys below the
cell's key, and every key from the cell's key up to the next cell's belongs
to the next child; the rightmost child holds the keys from the last cell's key
up. In a table, the key is the rowid; in an index, the encoded key.

A varint is unsigned LEB128: seven bits per byte, least significant group
first, high bit set on every byte but the last, at most 8 bytes. A zigzag
varint maps a signed integer `x` to `2x` when `x ≥ 0` and to `−2x − 1`
otherwise, then writes it as a varint.

### Limits

A cell and its pointer may take at most a quarter of the space after the
header, so a page always holds four cells and a split always produces two
halves that fit. A row's record or an index key gets that quarter less the
rest of its cell: the rowid and size in a table leaf, the child page and size
in an index interior page.

Rows bigger than that move their largest values to overflow pages. Index
keys never overflow: a longer key fails with `TOO_LARGE`.

## Records

A table row is a record: its column values in the order of the table's
columns.

```
record := header size (varint, counting itself) · serial types · values
```

| Serial type       | Value                                              | Size             |
| ----------------- | -------------------------------------------------- | ---------------- |
| 0                 | NULL                                               | 0                |
| 1                 | false                                              | 0                |
| 2                 | true                                               | 0                |
| 3                 | integer 0                                          | 0                |
| 4                 | integer 1                                          | 0                |
| 5, 6, 7, 8, 9, 10 | integer, two's complement                          | 1, 2, 3, 4, 6, 8 |
| 11                | number, IEEE 754 double                            | 8                |
| 12, 13, 14        | TEXT, BLOB, JSON in overflow pages                 | 8                |
| 15                | reserved                                           | —                |
| `16 + 3k`         | TEXT, `k` bytes of UTF-8                           | k                |
| `17 + 3k`         | BLOB, `k` bytes                                    | k                |
| `18 + 3k`         | JSON object or array, `k` bytes of UTF-8 JSON text | k                |

- Every safe integer is stored as an integer, in the fewest bytes, even in a
  REAL column: JavaScript does not tell 2 from 2.0. Other numbers are doubles.
- A JSON column stores a string, number or boolean as that kind; only objects
  and arrays use the JSON serial types.
- A record may have fewer values than the table has columns; the missing
  ones are those added later by `ALTER TABLE ADD COLUMN`, and read as the
  column's default.
- The column that aliases the rowid (an INTEGER PRIMARY KEY) is stored as
  NULL: its value is the cell's rowid.
- A reader finds column `k` from the serial types alone, and decodes only
  the values it needs.

### Overflow values

A TEXT, BLOB or JSON value in overflow pages is stored in the record as
`length (u32) · first page (u32)`, and its bytes fill a chain of pages:

| Offset | Size         | Field                                   |
| ------ | ------------ | --------------------------------------- |
| 0      | 1            | `0x05`                                  |
| 1      | 3            | zero                                    |
| 4      | 4            | next page of the chain, `0` on the last |
| 8      | pageSize − 8 | the value's next bytes                  |

A row's values stay in the record unless the row would exceed its limit; then
its TEXT, BLOB and JSON values move to overflow pages, the largest first (the
earlier column on a tie), until the rest fits. A row that cannot fit even so (thousands of columns) fails with
`TOO_LARGE`. Reading a row without a column never touches that column's
overflow pages.

## Index keys

An index key is the indexed values, then the rowid, each encoded so that
comparing two keys byte by byte gives the order of docs/semantics.md. Keys
are compared by their bytes alone.

| Tag    | Value                | Followed by                                       |
| ------ | -------------------- | ------------------------------------------------- |
| `0x10` | NULL                 | —                                                 |
| `0x20` | false                | —                                                 |
| `0x21` | true                 | —                                                 |
| `0x30` | number               | 8 bytes, below                                    |
| `0x40` | TEXT                 | its UTF-8 bytes, escaped, then `0x00`             |
| `0x50` | BLOB                 | its bytes, escaped, then `0x00`                   |
| `0x60` | JSON object or array | its canonical text in UTF-8, escaped, then `0x00` |

- **Numbers:** the IEEE 754 double, big-endian, with the sign bit flipped for
  zero and positive numbers and every bit flipped for negative ones. `−0` is
  encoded as `0`.
- **Escaping:** a `0x00` byte inside the value is written `0x00 0xFF`.

Every encoded value ends where it is complete, so a key is the values written
one after another, and a key that is a prefix of another sorts first. The
rowid is the last nine bytes of an index entry, which makes every entry
unique.

A range over a prefix of the indexed columns is a range of bytes: the keys
starting with prefix `p` are those at least `p` and below `p · 0xFF`, since no
encoded value starts with `0xFF`.

## System tables

The schema is stored in tables, read from the same snapshot as the data.

`opfsql_schema`, rooted at page 2, has one row per table and index:

| Column     | Type    | Value                                                                     |
| ---------- | ------- | ------------------------------------------------------------------------- |
| type       | TEXT    | `table` or `index`                                                        |
| name       | TEXT    | the table's or index's name                                               |
| table_name | TEXT    | the table an index belongs to; the table's own name for a table           |
| root_page  | INTEGER | the root page of its B-tree                                               |
| sql        | TEXT    | the canonical `CREATE` statement; NULL for indexes created by constraints |

An `INTEGER PRIMARY KEY` is the table's rowid and has no index of its own.
Every other `PRIMARY KEY` and every `UNIQUE` constraint has an index named
`opfsql_autoindex_<table>_<n>`, with no `sql`: its definition is in the
table's. Every change to `opfsql_schema` increments the schema version in
the header, which is how a session sees that the schema it holds is stale.

`opfsql_sequence` exists once a table has an `AUTOINCREMENT` column, with one
row per such table: `name TEXT` (the table) and `seq INTEGER` (the largest id
issued). Its root page is in `opfsql_schema`.

Names starting with `opfsql_` are reserved.

## WAL file

| Offset | Size | Field                                            |
| ------ | ---- | ------------------------------------------------ |
| 0      | 8    | magic `OPFSQLWL`                                 |
| 8      | 4    | format version, `5`                              |
| 12     | 4    | epoch: incremented each time the WAL restarts    |
| 16     | 4    | base transaction: the last one before this epoch |
| 20     | 4    | CRC-32 of bytes 0–19                             |

Then frames, each a 16-byte header and a payload:

| Offset | Size | Field                                            |
| ------ | ---- | ------------------------------------------------ |
| 0      | 4    | page number, or `0xFFFFFFFF` for a commit record |
| 4      | 4    | transaction id                                   |
| 8      | 4    | epoch                                            |
| 12     | 4    | CRC-32 of bytes 0–11 and the payload             |

- **Page frame:** the payload is the whole page.
- **Commit record:** the payload is 8 bytes: the page count after the
  transaction, then zero.

A transaction is its page frames followed by its commit record, appended and
flushed at once. Recovery reads frames until the first whose checksum or
epoch does not match, and keeps the transactions whose commit record it read.
Transaction ids continue across epochs, so an id is a position in the
database's history: a snapshot is the latest transaction it sees.

Restarting the WAL writes the new header, then truncates the file to it;
docs/concurrency.md says when it restarts and what another tab may see
meanwhile.

## Read-mark file

| Offset | Size   | Field                                                                   |
| ------ | ------ | ----------------------------------------------------------------------- |
| 0      | 8      | magic `OPFSQLSM`                                                        |
| 8      | 4      | format version, `5`                                                     |
| 12     | 4      | zero                                                                    |
| 16     | 8      | checkpoint intent: no commit past it is in the database file (f64)      |
| 24     | 8 × 64 | one read mark per slot: the oldest snapshot a tab has open, or −1 (f64) |

A tab owns a slot while it holds the Web Lock named after it. How tabs use
the intent and the marks, and what may change them, is in
docs/concurrency.md.

## Integrity

`Engine.checkIntegrity()` verifies:

- every page is in use exactly once — reachable from one tree or overflow
  chain, or an allocation map — or marked free;
- in every B-tree page, pointers lie between the header and the end of the
  page, cells do not overlap and fill the cell area without gaps, the free
  run is zero, and keys ascend;
- every key lies within the separators of its parent, and all leaves of a
  tree are at the same depth;
- every record parses, with no more values than its table has columns, and
  every overflow chain has exactly the bytes its length says;
- every index holds exactly one entry per row, with the key the row's values
  encode to, and no unique index holds two non-NULL keys that are equal;
- every `AUTOINCREMENT` sequence is at least the largest id in its table;
- the schema table parses, and its root pages are the trees found.

## Limits

| Limit                      | Value                                                           |
| -------------------------- | --------------------------------------------------------------- |
| page size                  | 4096 to 65536, a power of two; 8192 by default                  |
| pages                      | 2³² − 1                                                         |
| a value                    | 2³² − 1 bytes                                                   |
| a row's record in its leaf | about a quarter of a page; larger values move to overflow pages |
| an index key               | about a quarter of a page                                       |
| columns in a table         | 2000                                                            |
| integers                   | safe JavaScript integers, ±(2⁵³ − 1)                            |
