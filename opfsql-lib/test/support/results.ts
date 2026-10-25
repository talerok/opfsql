import type { Result, ResultRow, Schema } from "../../src/engine/api/index.js";

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
