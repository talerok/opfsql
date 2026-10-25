import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, expect, it } from "vitest";
import { RowTooLargeError } from "../../store/errors.js";
import type { Engine } from "../engine.js";
import type { Session } from "../session.js";
import { mulberry32 } from "../../../test/support/random.js";
import { openTestEngine } from "./test-engine.js";

// ---------------------------------------------------------------------------
// Model-based fuzz test.
//
// Random INSERT / UPDATE / DELETE with realistic value sizes (bytes to hundreds
// of KB), plus checkpoints and reopens at random points, checked against a
// plain Map and engine.checkIntegrity() after every step. A statement must
// either succeed and be reflected in the model, or fail with RowTooLargeError
// and leave the database exactly as it was. Seeds are fixed so any failure
// reproduces.
// ---------------------------------------------------------------------------

interface Row {
  tag: number;
  body: string;
}

const STEPS = 300;
const SEEDS = [1, 2, 3];

/** [weight, min length, max length] */
const BODY_SIZES: Array<[number, number, number]> = [
  [0.6, 0, 200],
  [0.25, 200, 4_000],
  [0.1, 4_000, 40_000],
  [0.05, 40_000, 300_000],
];

class Fuzzer {
  private readonly model = new Map<number, Row>();
  private nextId = 1;
  private engine!: Engine;
  private session!: Session;
  private readonly log: string[] = [];

  constructor(
    private readonly random: () => number,
    private readonly dbName: string,
  ) {}

  async run(): Promise<void> {
    await this.open();
    await this.session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, tag INTEGER, body TEXT)",
    );
    await this.session.execute("CREATE INDEX t_tag ON t (tag)");

    try {
      for (let step = 0; step < STEPS; step++) {
        await this.step();
        await this.verify();
      }
    } catch (err) {
      const recent = this.log.slice(-5).join("\n  ");
      throw new Error(`${(err as Error).message}\nlast steps:\n  ${recent}`, {
        cause: err,
      });
    } finally {
      this.close();
    }
  }

  private async step(): Promise<void> {
    const r = this.random();
    const ids = [...this.model.keys()];
    if (r < 0.55 || ids.length === 0) return this.insert();
    if (r < 0.8) return this.update(this.pick(ids));
    if (r < 0.9) return this.delete(this.pick(ids));
    if (r < 0.95) {
      this.log.push("checkpoint");
      await this.engine.checkpoint();
      return;
    }
    this.log.push("reopen");
    this.close();
    await this.open();
  }

  private async insert(): Promise<void> {
    const id = this.nextId++;
    const row = this.randomRow();
    await this.write(
      `insert ${id} (${row.body.length} B)`,
      () =>
        this.session.execute(
          "INSERT INTO t (id, tag, body) VALUES ($1, $2, $3)",
          [id, row.tag, row.body],
        ),
      () => this.model.set(id, row),
    );
  }

  private async update(id: number): Promise<void> {
    const row = this.randomRow();
    await this.write(
      `update ${id} (${row.body.length} B)`,
      () =>
        this.session.execute("UPDATE t SET tag = $1, body = $2 WHERE id = $3", [
          row.tag,
          row.body,
          id,
        ]),
      () => this.model.set(id, row),
    );
  }

  private async delete(id: number): Promise<void> {
    await this.write(
      `delete ${id}`,
      () => this.session.execute("DELETE FROM t WHERE id = $1", [id]),
      () => this.model.delete(id),
    );
  }

  private async write(
    description: string,
    statement: () => Promise<unknown>,
    applyToModel: () => void,
  ): Promise<void> {
    try {
      await statement();
      applyToModel();
      this.log.push(description);
    } catch (err) {
      if (!(err instanceof RowTooLargeError)) throw err;
      this.log.push(`${description} → rejected: ${err.message}`);
    }
  }

  private async verify(): Promise<void> {
    expect(this.engine.checkIntegrity()).toEqual([]);

    const [all] = await this.session.execute(
      "SELECT id, tag, body FROM t ORDER BY id",
    );
    const expected = [...this.model.entries()]
      .sort(([a], [b]) => a - b)
      .map(([id, row]) => ({ id, ...row }));
    expect(all.type === "rows" ? all.rows : all).toEqual(expected);

    const tag = Math.floor(this.random() * 10);
    const [byTag] = await this.session.execute(
      "SELECT id FROM t WHERE tag = $1 ORDER BY id",
      [tag],
    );
    expect(byTag.type === "rows" ? byTag.rows : byTag).toEqual(
      expected.filter((row) => row.tag === tag).map(({ id }) => ({ id })),
    );
  }

  private randomRow(): Row {
    let r = this.random();
    let [, min, max] = BODY_SIZES[BODY_SIZES.length - 1];
    for (const [weight, lo, hi] of BODY_SIZES) {
      if (r < weight) {
        [min, max] = [lo, hi];
        break;
      }
      r -= weight;
    }
    const length = min + Math.floor(this.random() * (max - min));
    const ch = String.fromCharCode(97 + Math.floor(this.random() * 26));
    return { tag: Math.floor(this.random() * 10), body: ch.repeat(length) };
  }

  private pick<T>(items: T[]): T {
    return items[Math.floor(this.random() * items.length)];
  }

  private async open(): Promise<void> {
    this.engine = await openTestEngine(this.dbName);
    this.session = this.engine.createSession();
  }

  private close(): void {
    this.session.close();
    this.engine.close();
  }
}

// Every step re-reads and integrity-checks the whole database, so the run takes
// seconds rather than the default 5 s budget under a loaded parallel run.
describe("fuzz: storage against a model", { timeout: 30_000 }, () => {
  beforeEach(() => {
    resetMockOPFS();
  });

  for (const seed of SEEDS) {
    it(`seed ${seed}`, async () => {
      await new Fuzzer(mulberry32(seed), `fuzz-${seed}`).run();
    });
  }
});
