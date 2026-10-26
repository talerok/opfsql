import { describe, expect, it, vi } from "vitest";
import { MemoryFile } from "../../storage/file/sync-file.js";
import { createEngine } from "../engine.js";
import type { Session } from "../session.js";
import { SerializationError } from "../../errors.js";
import { mulberry32 } from "../../../test/support/random.js";

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
  const [r] = await session.execute("SELECT id, v FROM t ORDER BY id");
  return r.objects();
}

async function insert(session: Session, id: number, v = 0): Promise<void> {
  await session.query("INSERT INTO t (id, v) VALUES ($1, $2)", [id, v]);
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
      `readers keep their snapshots under random writes (seed ${seed})`,
      { timeout: 30_000 },
      async () => {
        const random = mulberry32(seed);
        const { engine, writer } = await openDatabase(8);
        const model = new Map<number, number>();
        const readers = Array.from({ length: 3 }, () => ({
          session: engine.connect(),
          snapshot: null as unknown[] | null,
        }));
        const expected = () =>
          [...model.entries()]
            .sort(([a], [b]) => a - b)
            .map(([id, v]) => ({ id, v }));
        let nextId = 1;

        for (let step = 0; step < 300; step++) {
          const r = random();
          const ids = [...model.keys()];
          const reader = readers[Math.floor(random() * readers.length)];

          if (r < 0.35 || ids.length === 0) {
            const id = nextId++;
            await insert(writer, id, step);
            model.set(id, step);
          } else if (r < 0.5) {
            const id = ids[Math.floor(random() * ids.length)];
            await writer.query("UPDATE t SET v = $1 WHERE id = $2", [step, id]);
            model.set(id, step);
          } else if (r < 0.6) {
            const id = ids[Math.floor(random() * ids.length)];
            await writer.query("DELETE FROM t WHERE id = $1", [id]);
            model.delete(id);
          } else if (r < 0.75 && reader.snapshot === null) {
            await reader.session.execute("BEGIN");
            reader.snapshot = expected();
          } else if (r < 0.85 && reader.snapshot !== null) {
            await reader.session.execute("COMMIT");
            reader.snapshot = null;
          } else if (r < 0.9) {
            await engine.checkpoint();
          }

          for (const { session, snapshot } of readers) {
            expect(await rows(session)).toEqual(snapshot ?? expected());
          }
          expect(await engine.checkIntegrity()).toEqual([]);
        }
      },
    );
  }
});
