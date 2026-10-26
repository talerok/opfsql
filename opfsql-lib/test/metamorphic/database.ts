import { createEngine, type Engine } from "../../src/engine/engine.js";
import type { Session } from "../../src/engine/session.js";
import { OpfsqlError } from "../../src/errors.js";
import { MemoryFile } from "../../src/storage/file/sync-file.js";
import { generateRows, insertSql, type Row } from "./data.js";
import type { Random } from "./random.js";
import {
  createIndexSql,
  createTableSql,
  type DatabaseSpec,
  type TableSpec,
} from "./schema.js";

// ---------------------------------------------------------------------------
// The two copies of a random database: the same tables and rows, and only
// the indexed one with the random indexes — some made before its rows were
// inserted, the rest after.
// ---------------------------------------------------------------------------

export interface Copy {
  readonly engine: Engine;
  readonly session: Session;
}

export interface Copies {
  readonly plain: Copy;
  readonly indexed: Copy;
  /** The rows each table holds: the generated ones UNIQUE accepted. */
  readonly rows: ReadonlyMap<string, readonly Row[]>;
  readonly pageSize: number;
}

const PAGE_SIZES = [4096, 8192];

export async function openCopies(
  random: Random,
  spec: DatabaseSpec,
): Promise<Copies> {
  const pageSize = random.pick(PAGE_SIZES);
  const generated = new Map(
    spec.tables.map((table) => [table.name, generateRows(random, table)]),
  );
  return loadCopies(spec, generated, pageSize);
}

/** Both copies, holding `generated` rows: a reducer loads fewer. */
export async function loadCopies(
  spec: DatabaseSpec,
  generated: ReadonlyMap<string, readonly Row[]>,
  pageSize: number,
): Promise<Copies> {
  const plain = await openCopy(pageSize);
  const indexed = await openCopy(pageSize);
  const rows = new Map<string, Row[]>();
  for (const table of spec.tables) {
    await plain.session.query(createTableSql(table));
    rows.set(
      table.name,
      await insertAccepted(
        plain.session,
        table,
        generated.get(table.name) ?? [],
      ),
    );
  }
  await buildIndexed(indexed.session, spec, rows);
  return { plain, indexed, rows, pageSize };
}

async function openCopy(pageSize: number): Promise<Copy> {
  const files = {
    database: new MemoryFile(),
    wal: new MemoryFile(),
    marks: null,
  };
  const engine = await createEngine(files, { pageSize });
  return { engine, session: engine.connect() };
}

/** Inserts each row on its own, and returns those a UNIQUE constraint let in. */
async function insertAccepted(
  session: Session,
  table: TableSpec,
  rows: readonly Row[],
): Promise<Row[]> {
  const accepted: Row[] = [];
  for (const row of rows) {
    const isInserted = await succeeds(session.query(insertSql(table, row)));
    if (isInserted) accepted.push(row);
  }
  return accepted;
}

async function succeeds(work: Promise<unknown>): Promise<boolean> {
  try {
    await work;
    return true;
  } catch (error) {
    if (error instanceof OpfsqlError && error.code === "CONSTRAINT_VIOLATION")
      return false;
    throw error;
  }
}

/** The tables, the early indexes, every accepted row, then the late indexes. */
async function buildIndexed(
  session: Session,
  spec: DatabaseSpec,
  rows: ReadonlyMap<string, readonly Row[]>,
): Promise<void> {
  for (const table of spec.tables) await session.query(createTableSql(table));
  const early = spec.indexes.filter((index) => index.isEarly);
  const late = spec.indexes.filter((index) => !index.isEarly);
  for (const index of early) await session.query(createIndexSql(index));
  for (const table of spec.tables) {
    const tableRows = rows.get(table.name) ?? [];
    for (const row of tableRows) await session.query(insertSql(table, row));
  }
  for (const index of late) await session.query(createIndexSql(index));
}

export async function closeCopies(copies: Copies): Promise<void> {
  for (const copy of [copies.plain, copies.indexed]) {
    await copy.session.close();
    await copy.engine.close();
  }
}
