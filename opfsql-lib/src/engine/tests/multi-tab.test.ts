import { beforeEach, describe, expect, it, vi } from "vitest";
import type { StorageFiles } from "../../storage/index.js";
import { Pager } from "../../storage/pager/pager.js";
import type { Engine } from "../engine.js";
import type { Session } from "../session.js";
import { Model, type Row } from "../../../test/support/model.js";
import { mulberry32, pick, type Random } from "../../../test/support/random.js";
import type { TupleOf } from "../../../test/support/tuple.js";
import { openTab, sharedFiles } from "./tabs.js";

// ---------------------------------------------------------------------------
// Several engines ("tabs") on one database, coordinated the way real tabs
// are: Web Locks for writers, read marks for snapshots. Each tab has its own
// WAL index and schema cache; only the files are shared.
// ---------------------------------------------------------------------------

interface Tab {
  readonly engine: Engine;
  readonly session: Session;
}

let opened = 0;

async function openTabs<N extends number>(
  name: string,
  count: N,
  files: StorageFiles = sharedFiles(),
): Promise<TupleOf<Tab, N>> {
  const unique = `${name}-${String(opened++)}`;
  const tabs: Tab[] = [];
  for (let index = 0; index < count; index++) {
    const engine = await openTab(unique, files);
    tabs.push({ engine, session: engine.connect() });
  }
  // `count` of them, as the type says.
  return tabs as TupleOf<Tab, N>;
}

async function insert(
  session: Session,
  model: Model,
  v: number,
): Promise<void> {
  const id = model.newId();
  await session.query("INSERT INTO t (id, v) VALUES ($1, $2)", [id, v]);
  model.set(id, v);
}

/** One random change through `tab`: a write, a transaction or a checkpoint. */
async function randomChange(
  tab: Tab,
  model: Model,
  random: Random,
): Promise<void> {
  const roll = random();
  const v = Math.floor(random() * 1000);
  const { session } = tab;
  if (roll < 0.4 || model.isEmpty) {
    await insert(session, model, v);
  } else if (roll < 0.6) {
    const id = model.anyId(random);
    await session.query("UPDATE t SET v = $1 WHERE id = $2", [v, id]);
    model.set(id, v);
  } else if (roll < 0.75) {
    const id = model.anyId(random);
    await session.query("DELETE FROM t WHERE id = $1", [id]);
    model.delete(id);
  } else if (roll < 0.9) {
    await session.execute("BEGIN");
    for (let offset = 0; offset < 3; offset++) {
      await insert(session, model, v + offset);
    }
    await session.execute("COMMIT");
  } else {
    await tab.engine.checkpoint();
  }
}

/** A tab whose session opens and closes read transactions. */
class Reader {
  /** The rows its open transaction saw at BEGIN; null outside one. */
  private snapshot: Row[] | null = null;

  constructor(readonly tab: Tab) {}

  get isReading(): boolean {
    return this.snapshot !== null;
  }

  /** What its reads must return: its snapshot, or the latest rows. */
  expected(model: Model): Row[] {
    return this.snapshot ?? model.rows();
  }

  async begin(model: Model): Promise<void> {
    await this.tab.session.execute("BEGIN");
    this.snapshot = model.rows();
  }

  async commit(): Promise<void> {
    await this.tab.session.execute("COMMIT");
    this.snapshot = null;
  }
}

/**
 * One random step of a reader: outside a transaction it writes or begins
 * one, inside it commits; or it checkpoints.
 */
async function randomReaderStep(
  reader: Reader,
  model: Model,
  random: Random,
): Promise<void> {
  const roll = random();
  const { isReading } = reader;
  if (!isReading && roll < 0.45) {
    await insertOrDelete(reader.tab.session, model, random, roll);
  } else if (!isReading && roll < 0.65) {
    await reader.begin(model);
  } else if (isReading && roll < 0.8) {
    await reader.commit();
  } else {
    await reader.tab.engine.checkpoint();
  }
}

async function insertOrDelete(
  session: Session,
  model: Model,
  random: Random,
  roll: number,
): Promise<void> {
  if (model.isEmpty || roll < 0.25) {
    await insert(session, model, Math.floor(roll * 1000));
    return;
  }
  const id = model.anyId(random);
  await session.query("DELETE FROM t WHERE id = $1", [id]);
  model.delete(id);
}

/** Where the read-mark file keeps slot `slot`'s mark: a float64 after the header. */
function slotOffset(slot: number): number {
  return 24 + slot * 8;
}

/** The slot of a tab that died: the last, which no test tab claims. */
const ORPHAN_SLOT = 63;

/** Writes the mark a tab that died holding snapshot 1 would have left. */
function writeOrphanMark(marks: StorageFiles["marks"]): void {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setFloat64(0, 1);
  marks!.write(bytes, { at: slotOffset(ORPHAN_SLOT) });
}

function markIn(marks: StorageFiles["marks"], slot: number): number {
  const bytes = new Uint8Array(8);
  marks!.read(bytes, { at: slotOffset(slot) });
  return new DataView(bytes.buffer).getFloat64(0);
}

async function rows(session: Session): Promise<unknown[]> {
  const result = await session.query("SELECT id, v FROM t ORDER BY id");
  return result.objects();
}

beforeEach(() => {
  vi.restoreAllMocks();
});

// Every step re-reads and integrity-checks the database from each tab, so the
// property tests take seconds rather than the default 5 s budget.
describe("multiple tabs", { timeout: 30_000 }, () => {
  for (const seed of [1, 2, 3]) {
    it(`interleaved writes and checkpoints keep every tab consistent (seed ${String(seed)})`, async () => {
      const random = mulberry32(seed);
      const tabs = await openTabs(`tabs-${String(seed)}`, 2);
      const model = new Model();
      await tabs[0].session.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
      );

      for (let step = 0; step < 400; step++) {
        await randomChange(pick(random, tabs), model, random);
        for (const tab of tabs) {
          expect(await rows(tab.session)).toEqual(model.rows());
          expect(await tab.engine.checkIntegrity()).toEqual([]);
        }
      }
    });
  }

  // Tabs open and close read transactions at random while others write and
  // checkpoint; every open transaction must keep seeing the model as of its
  // BEGIN, and every other read must see the latest model.
  for (const seed of [1, 2, 3]) {
    it(`transactions in every tab keep their snapshots (seed ${String(seed)})`, async () => {
      const random = mulberry32(seed);
      const tabs = await openTabs(`tx-tabs-${String(seed)}`, 3);
      const readers = tabs.map((tab) => new Reader(tab));
      const model = new Model();
      await tabs[0].session.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
      );

      for (let step = 0; step < 300; step++) {
        await randomReaderStep(pick(random, readers), model, random);
        for (const reader of readers) {
          const { session, engine } = reader.tab;
          expect(await rows(session)).toEqual(reader.expected(model));
          expect(await engine.checkIntegrity()).toEqual([]);
        }
      }
    });
  }

  it("a tab that died with a snapshot open does not hold checkpoints back for good", async () => {
    const files = sharedFiles();
    const [writer] = await openTabs("dead-tab", 1, files);
    await writer.session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    writeOrphanMark(files.marks);
    // Only a restart truncates the WAL here: no append fails in this test.
    const restarts = vi.spyOn(files.wal, "truncate");

    for (let id = 1; id <= 40; id++) {
      await writer.session.query("INSERT INTO t (id, v) VALUES ($1, 0)", [id]);
    }
    // The orphan's mark was cleared, and a checkpoint then went all the way
    // and restarted the WAL.
    expect(markIn(files.marks, ORPHAN_SLOT)).toBe(-1);
    expect(restarts).toHaveBeenCalled();
  });

  it("close() waits for the background release of dead tabs' read marks", async () => {
    const files = sharedFiles();
    const [writer] = await openTabs("closing", 1, files);
    await writer.session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    writeOrphanMark(files.marks);
    const events: string[] = [];
    const gate = Promise.withResolvers<undefined>();
    const release = Reflect.get<Pager, "releaseAbandonedSnapshots">(
      Pager.prototype,
      "releaseAbandonedSnapshots",
    );
    vi.spyOn(Pager.prototype, "releaseAbandonedSnapshots").mockImplementation(
      async function released(this: Pager) {
        await gate.promise;
        await release.call(this);
        events.push("released");
      },
    );
    const close = Reflect.get<Pager, "close">(Pager.prototype, "close");
    vi.spyOn(Pager.prototype, "close").mockImplementation(function closed(
      this: Pager,
    ) {
      events.push("closed");
      close.call(this);
    });

    // Checkpoints the orphan holds back start releases, which wait on `gate`.
    for (let id = 1; id <= 20; id++) {
      await writer.session.query("INSERT INTO t (id, v) VALUES ($1, 0)", [id]);
    }
    const closing = writer.engine.close();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(events).toEqual([]);

    gate.resolve(undefined);
    await closing;
    expect(events.at(-1)).toBe("closed");
    expect(events.filter((event) => event === "closed")).toHaveLength(1);
    expect(events).toContain("released");
  });

  it("AUTOINCREMENT ids stay unique when tabs take turns inserting", async () => {
    const tabs = await openTabs("autoincrement", 2);
    await tabs[0].session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v INTEGER)",
    );
    for (let v = 0; v < 50; v++) {
      const { engine, session } = v % 2 === 0 ? tabs[0] : tabs[1];
      await session.query("INSERT INTO t (v) VALUES ($1)", [v]);
      if (v % 7 === 0) await engine.checkpoint();
    }

    const result = await tabs[1].session.query(
      "SELECT id, v FROM t ORDER BY id",
    );
    expect(result.objects()).toEqual(
      Array.from({ length: 50 }, (_, v) => ({ id: v + 1, v })),
    );
  });

  it("a transaction keeps its snapshot while another tab writes and checkpoints", async () => {
    const [writer, reader] = await openTabs("snapshot", 2);
    await writer.session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    for (let id = 1; id <= 100; id++) {
      await writer.session.query("INSERT INTO t (id, v) VALUES ($1, 0)", [id]);
    }
    await writer.engine.checkpoint();

    await reader.session.execute("BEGIN");
    const snapshot = await rows(reader.session);
    for (let id = 101; id <= 300; id++) {
      await writer.session.query("INSERT INTO t (id, v) VALUES ($1, 0)", [id]);
    }
    await writer.engine.checkpoint();

    expect(await rows(reader.session)).toEqual(snapshot);
    await reader.session.execute("COMMIT");
  });
});
