import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, expect, it } from "vitest";
import type { Engine } from "../engine.js";
import type { Session } from "../session.js";
import { mulberry32, pick, type Random } from "../../../test/support/random.js";
import { openTestEngine } from "./test-engine.js";

// ---------------------------------------------------------------------------
// Model-based fuzz test.
//
// Random INSERT / UPDATE / DELETE with realistic value sizes (bytes to hundreds
// of KB), plus checkpoints and reopens at random points, checked against a
// plain Map and engine.checkIntegrity() after every step. Every statement
// succeeds and is reflected in the model: values larger than a page move to
// overflow pages. Seeds are fixed so any failure reproduces.
// ---------------------------------------------------------------------------

interface Row {
  readonly tag: number;
  readonly body: string;
}

const STEPS = 300;
const SEEDS = [1, 2, 3];

/** A body: mostly short, sometimes pages long, now and then hundreds of KB. */
function randomBody(random: Random): string {
  const length = randomBodyLength(random);
  return randomLetter(random).repeat(length);
}

/** A lowercase ASCII letter, a to z. */
function randomLetter(random: Random): string {
  return String.fromCodePoint(0x61 + Math.floor(random() * 26));
}

function randomBodyLength(random: Random): number {
  const roll = random();
  if (roll < 0.6) return between(random, 0, 200);
  if (roll < 0.85) return between(random, 200, 4000);
  if (roll < 0.95) return between(random, 4000, 40_000);
  return between(random, 40_000, 300_000);
}

/** An integer from `min` up to, not including, `max`. */
function between(random: Random, min: number, max: number): number {
  return min + Math.floor(random() * (max - min));
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

class Fuzzer {
  private readonly model = new Map<number, Row>();
  private nextId = 1;
  private engine!: Engine;
  private session!: Session;
  private readonly log: string[] = [];

  constructor(
    private readonly random: Random,
    private readonly databaseName: string,
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
    } catch (error) {
      const recent = this.log.slice(-5).join("\n  ");
      throw new Error(`${messageOf(error)}\nlast steps:\n  ${recent}`, {
        cause: error,
      });
    } finally {
      await this.close();
    }
  }

  private async step(): Promise<void> {
    const roll = this.random();
    const ids = [...this.model.keys()];
    if (roll < 0.55 || ids.length === 0) return this.insert();
    if (roll < 0.8) return this.update(pick(this.random, ids));
    if (roll < 0.9) return this.delete(pick(this.random, ids));
    if (roll < 0.95) {
      this.log.push("checkpoint");
      await this.engine.checkpoint();
      return;
    }
    this.log.push("reopen");
    await this.close();
    await this.open();
  }

  private async insert(): Promise<void> {
    const id = this.nextId++;
    const row = this.randomRow();
    await this.session.query(
      "INSERT INTO t (id, tag, body) VALUES ($1, $2, $3)",
      [id, row.tag, row.body],
    );
    this.model.set(id, row);
    this.log.push(`insert ${String(id)} (${String(row.body.length)} B)`);
  }

  private async update(id: number): Promise<void> {
    const row = this.randomRow();
    await this.session.query("UPDATE t SET tag = $1, body = $2 WHERE id = $3", [
      row.tag,
      row.body,
      id,
    ]);
    this.model.set(id, row);
    this.log.push(`update ${String(id)} (${String(row.body.length)} B)`);
  }

  private async delete(id: number): Promise<void> {
    await this.session.query("DELETE FROM t WHERE id = $1", [id]);
    this.model.delete(id);
    this.log.push(`delete ${String(id)}`);
  }

  private async verify(): Promise<void> {
    expect(await this.engine.checkIntegrity()).toEqual([]);

    const all = await this.session.query(
      "SELECT id, tag, body FROM t ORDER BY id",
    );
    const expected = [...this.model]
      .toSorted(([a], [b]) => a - b)
      .map(([id, row]) => ({ id, ...row }));
    expect(all.objects()).toEqual(expected);

    const tag = Math.floor(this.random() * 10);
    const byTag = await this.session.query(
      "SELECT id FROM t WHERE tag = $1 ORDER BY id",
      [tag],
    );
    expect(byTag.objects()).toEqual(
      expected.filter((row) => row.tag === tag).map(({ id }) => ({ id })),
    );
  }

  private randomRow(): Row {
    const body = randomBody(this.random);
    return { tag: Math.floor(this.random() * 10), body };
  }

  private async open(): Promise<void> {
    this.engine = await openTestEngine(this.databaseName);
    this.session = this.engine.connect();
  }

  private async close(): Promise<void> {
    await this.session.close();
    await this.engine.close();
  }
}

// Every step re-reads and integrity-checks the whole database, so the run takes
// seconds rather than the default 5 s budget under a loaded parallel run.
describe("fuzz: storage against a model", { timeout: 30_000 }, () => {
  beforeEach(() => {
    resetMockOPFS();
  });

  for (const seed of SEEDS) {
    it(`seed ${String(seed)}`, async () => {
      await new Fuzzer(mulberry32(seed), `fuzz-${String(seed)}`).run();
    });
  }
});
