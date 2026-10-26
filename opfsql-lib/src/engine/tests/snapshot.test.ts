import { describe, expect, it, vi } from "vitest";
import { MemoryFile } from "../../storage/file/sync-file.js";
import { createEngine } from "../engine.js";
import type { Session } from "../session.js";
import { SerializationError } from "../../errors.js";
import { Model, type Row } from "../../../test/support/model.js";
import { mulberry32, pick, type Random } from "../../../test/support/random.js";

// Snapshot isolation between sessions of one engine, on memory files, with
// the checkpoint threshold each test needs.

async function openDatabase(checkpointThreshold?: number) {
  const wal = new MemoryFile();
  const files = { database: new MemoryFile(), wal, marks: null };
  const engine = await createEngine(files, {
    ...(checkpointThreshold !== undefined && { checkpointThreshold }),
  });
  const writer = engine.connect();
  await writer.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
  /**
   * WAL restarts: a checkpoint that brought everything into the file. Only a
   * restart truncates the WAL here; no append fails in these tests.
   */
  const restarts = vi.spyOn(wal, "truncate");
  return { restarts, engine, writer };
}

async function rows(session: Session): Promise<unknown[]> {
  const result = await session.query("SELECT id, v FROM t ORDER BY id");
  return result.objects();
}

async function insert(session: Session, id: number, v = 0): Promise<void> {
  await session.query("INSERT INTO t (id, v) VALUES ($1, $2)", [id, v]);
}

/** A session that opens and closes read transactions. */
class Reader {
  /** The rows its open transaction saw at BEGIN; null outside one. */
  private snapshot: Row[] | null = null;

  constructor(readonly session: Session) {}

  get isReading(): boolean {
    return this.snapshot !== null;
  }

  /** What its reads must return: its snapshot, or the latest rows. */
  expected(model: Model): Row[] {
    return this.snapshot ?? model.rows();
  }

  async begin(model: Model): Promise<void> {
    await this.session.execute("BEGIN");
    this.snapshot = model.rows();
  }

  async commit(): Promise<void> {
    await this.session.execute("COMMIT");
    this.snapshot = null;
  }
}

/** An insert, update or delete by `roll`, writing `v`; inserts if t is empty. */
async function randomWrite(
  writer: Session,
  model: Model,
  random: Random,
  { roll, v }: { readonly roll: number; readonly v: number },
): Promise<void> {
  if (roll < 0.35 || model.isEmpty) {
    const id = model.newId();
    await insert(writer, id, v);
    model.set(id, v);
  } else if (roll < 0.5) {
    const id = model.anyId(random);
    await writer.query("UPDATE t SET v = $1 WHERE id = $2", [v, id]);
    model.set(id, v);
  } else {
    const id = model.anyId(random);
    await writer.query("DELETE FROM t WHERE id = $1", [id]);
    model.delete(id);
  }
}

describe("snapshot isolation", () => {
  it("a transaction does not see commits made after it began", async () => {
    const { engine, writer } = await openDatabase();
    const reader = engine.connect();
    await insert(writer, 1);

    await reader.execute("BEGIN");
    const before = await rows(reader);
    await insert(writer, 2);
    await writer.execute("UPDATE t SET v = 9 WHERE id = 1");
    expect(await rows(reader)).toEqual(before);
    await reader.execute("COMMIT");

    expect(await rows(reader)).toEqual([
      { id: 1, v: 9 },
      { id: 2, v: 0 },
    ]);
  });

  it("a statement outside a transaction sees every earlier commit", async () => {
    const { engine, writer } = await openDatabase();
    const reader = engine.connect();
    await insert(writer, 1);
    expect(await rows(reader)).toEqual([{ id: 1, v: 0 }]);
    await insert(writer, 2);
    expect(await rows(reader)).toHaveLength(2);
  });

  it("an open snapshot survives checkpoints and holds back the WAL restart", async () => {
    const { restarts, engine, writer } = await openDatabase(4);
    const reader = engine.connect();
    await insert(writer, 1);

    await reader.execute("BEGIN");
    const before = await rows(reader);
    restarts.mockClear();
    for (let id = 2; id <= 40; id++) await insert(writer, id);
    expect(await rows(reader)).toEqual(before);
    expect(restarts).not.toHaveBeenCalled();
    await reader.execute("COMMIT");

    await insert(writer, 41);
    expect(restarts).toHaveBeenCalled();
    expect(await rows(reader)).toHaveLength(41);
    expect(await engine.checkIntegrity()).toEqual([]);
  });

  it("writing after another session's commit fails and can be retried", async () => {
    const { engine, writer } = await openDatabase();
    const other = engine.connect();
    await insert(writer, 1);

    await writer.execute("BEGIN");
    await rows(writer);
    await insert(other, 2);
    await expect(insert(writer, 3)).rejects.toThrow(SerializationError);
    // The transaction goes on reading its snapshot; it cannot write.
    expect(await rows(writer)).toEqual([{ id: 1, v: 0 }]);
    await expect(insert(writer, 4)).rejects.toThrow(SerializationError);
    await writer.execute("ROLLBACK");

    await writer.execute("BEGIN");
    await insert(writer, 3);
    await writer.execute("COMMIT");
    expect(await rows(other)).toHaveLength(3);
  });

  it("a writing transaction with no concurrent commits succeeds", async () => {
    const { engine, writer } = await openDatabase();
    const reader = engine.connect();
    await reader.execute("BEGIN");
    await rows(reader);

    await writer.execute("BEGIN");
    await insert(writer, 1);
    await insert(writer, 2);
    await writer.execute("COMMIT");

    expect(await rows(reader)).toEqual([]);
    await reader.execute("COMMIT");
  });

  // Several readers hold transactions open across random writes and
  // checkpoints; each must keep seeing exactly the model as of its BEGIN.
  for (const seed of [1, 2, 3]) {
    it(
      `readers keep their snapshots under random writes (seed ${String(seed)})`,
      { timeout: 30_000 },
      async () => {
        const random = mulberry32(seed);
        const { engine, writer } = await openDatabase(8);
        const model = new Model();
        const readers = Array.from(
          { length: 3 },
          () => new Reader(engine.connect()),
        );
        for (let step = 0; step < 300; step++) {
          const roll = random();
          const reader = pick(random, readers);
          if (roll < 0.6 || model.isEmpty) {
            await randomWrite(writer, model, random, { roll, v: step });
          } else if (roll < 0.75 && !reader.isReading) {
            await reader.begin(model);
          } else if (roll < 0.85 && reader.isReading) {
            await reader.commit();
          } else if (roll < 0.9) {
            await engine.checkpoint();
          }
          for (const each of readers) {
            expect(await rows(each.session)).toEqual(each.expected(model));
          }
          expect(await engine.checkIntegrity()).toEqual([]);
        }
      },
    );
  }
});
