import type {
  Result,
  ResultRow,
  Schema,
  TableInfo,
} from "../../src/engine/api/index.js";
import type { Value } from "../../src/values/index.js";

// Shorthands for what a test reads off a result it awaits.

export async function rowsOf(
  work: Promise<Result>,
): Promise<readonly ResultRow[]> {
  const result = await work;
  return result.rows;
}

export async function objectsOf(
  work: Promise<Result>,
): Promise<Record<string, unknown>[]> {
  const result = await work;
  return result.objects();
}

export async function columnsOf(
  work: Promise<Result>,
): Promise<readonly string[]> {
  const result = await work;
  return result.columns;
}

export async function lastRowidOf(
  work: Promise<Result>,
): Promise<number | null> {
  const result = await work;
  return result.lastInsertRowid;
}

export async function tableNamesOf(work: Promise<Schema>): Promise<string[]> {
  const schema = await work;
  return schema.tables.map((table) => table.name);
}

/** The value of `column` in row `row` of a result; undefined past its rows. */
export function cellOf(
  result: Result,
  column: string,
  row = 0,
): Value | undefined {
  return result.objects()[row]?.[column];
}

/** The values of `column`, one per row, in row order. */
export function columnValues(result: Result, column: string): Value[] {
  return result.objects().map((row) => row[column] ?? null);
}

/** The table `name` of a schema description; fails the test without one. */
export function tableOf(schema: Schema, name: string): TableInfo {
  const table = schema.tables.find((candidate) => candidate.name === name);
  if (table === undefined) throw new Error(`no table ${name} in the schema`);
  return table;
}
