import type { Index, Schema, Table } from "../schema/index.js";
import type { PageReader } from "../storage/index.js";
import {
  decodeKey,
  IndexReader,
  keyRowid,
  TableReader,
} from "../storage/index.js";
import { compareBytes, rowKey } from "../values/index.js";
import type { CompileExpression } from "./keys.js";
import { entryOf, keyedIndex } from "./keys.js";
import { TableRows } from "./rows.js";
import { lastIssued, SEQUENCE_TABLE_NAME } from "./sequences.js";

// ---------------------------------------------------------------------------
// The table part of the integrity check (docs/file-format.md, Integrity):
// what rows mean to their tables. checkStorage has already found the trees
// sound; this checks that each index holds exactly the entries its table's
// rows give it, that no unique index holds a key twice, that no record has
// more values than its table has columns, and that every AUTOINCREMENT
// counter is at least its table's largest rowid.
// ---------------------------------------------------------------------------

/** Enough differences to see what is wrong, without listing every row. */
const MAX_DIFFERENCES = 5;

export function checkTables(
  pages: PageReader,
  schema: Schema,
  compile: CompileExpression,
): string[] {
  return [
    ...schema.problems(),
    ...schema.tables.flatMap((table) => [
      ...recordProblems(pages, table),
      ...sequenceProblems(pages, schema, table),
      ...schema
        .indexesOf(table.name)
        .flatMap((index) => indexProblems(pages, table, index, compile)),
    ]),
  ];
}

function recordProblems(pages: PageReader, table: Table): string[] {
  const problems: string[] = [];
  const rows = new TableReader(pages, table.rootPage).scan();
  for (const row of rows) {
    if (row.length > table.columns.length) {
      problems.push(
        `table ${table.name}: row ${String(row.rowid)} has ${String(row.length)} values for ${String(table.columns.length)} columns`,
      );
    }
  }
  return problems.slice(0, MAX_DIFFERENCES);
}

function sequenceProblems(
  pages: PageReader,
  schema: Schema,
  table: Table,
): string[] {
  if (!table.isAutoincrement) return [];
  const sequences = schema.table(SEQUENCE_TABLE_NAME);
  if (sequences === null) {
    return [
      `table ${table.name}: AUTOINCREMENT without ${SEQUENCE_TABLE_NAME}`,
    ];
  }
  const largest = new TableReader(pages, table.rootPage).lastRowid() ?? 0;
  const issued = lastIssued(pages, sequences.rootPage, table.name);
  if (issued >= largest) return [];
  return [
    `table ${table.name}: ${SEQUENCE_TABLE_NAME} says ${String(issued)}, below rowid ${String(largest)}`,
  ];
}

/** The index must hold exactly the entries the rows give it, unique keys once. */
function indexProblems(
  pages: PageReader,
  table: Table,
  index: Index,
  compile: CompileExpression,
): string[] {
  const keyed = keyedIndex(index, table, compile);
  const rows = [...new TableRows(pages, table).scan()];
  const expected = rows
    .map((row) => entryOf(keyed, row.values(), row.rowid))
    .toSorted(compareBytes);
  const stored = [...new IndexReader(pages, index.rootPage).keysFrom()];
  const duplicates = index.isUnique ? duplicateKeys(stored) : [];
  return [...differences(expected, stored), ...duplicates]
    .slice(0, MAX_DIFFERENCES)
    .map((problem) => `index ${index.name}: ${problem}`);
}

/** Entries one sorted list has and the other lacks. */
function differences(
  expected: readonly Uint8Array[],
  stored: readonly Uint8Array[],
): string[] {
  const storedKeys = new Set(stored.map((entry) => entryKey(entry)));
  const expectedKeys = new Set(expected.map((entry) => entryKey(entry)));
  const missing = expected
    .filter((entry) => !storedKeys.has(entryKey(entry)))
    .map((entry) => `no entry for row ${String(keyRowid(entry))}`);
  const extra = stored
    .filter((entry) => !expectedKeys.has(entryKey(entry)))
    .map(
      (entry) =>
        `an entry for row ${String(keyRowid(entry))} that no row gives`,
    );
  return [...missing, ...extra];
}

/** Keys without NULLs that more than one row has, in a unique index. */
function duplicateKeys(stored: readonly Uint8Array[]): string[] {
  const seen = new Set<string>();
  const problems: string[] = [];
  for (const entry of stored) {
    const key = decodeKey(entry).slice(0, -1);
    // NULLs are distinct: a key with one never repeats another.
    const hasNull = key.includes(null);
    const keyText = rowKey(key);
    if (!hasNull && seen.has(keyText)) {
      problems.push(
        `unique key held twice, again by row ${String(keyRowid(entry))}`,
      );
    }
    seen.add(keyText);
  }
  return problems;
}

function entryKey(entry: Uint8Array): string {
  return entry.join(",");
}
