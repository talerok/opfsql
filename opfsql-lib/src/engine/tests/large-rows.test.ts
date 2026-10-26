import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { IndexKeyTooLargeError } from "../../errors.js";
import type { Engine } from "../engine.js";
import type { Session } from "../session.js";
import type { Value } from "../../values/index.js";
import { columnValues } from "../../../test/support/results.js";
import { openTestEngine } from "./test-engine.js";

// Rows larger than a page move their largest values to overflow pages; an
// index key never overflows, so one too large for a page fails with
// TOO_LARGE (docs/file-format.md).

let engine: Engine;
let session: Session;
let seq = 0;

beforeEach(async () => {
  resetMockOPFS();
  engine = await openTestEngine(`large-rows-${String(seq++)}`);
  session = engine.connect();
  await session.execute(`
    CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT);
    CREATE TABLE keyed (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT);
    CREATE INDEX keyed_body ON keyed (body);
  `);
});

afterEach(async () => {
  await session.close();
  await engine.close();
});

/** Larger than any page, so larger than any index key can be. */
const BIG = "x".repeat(80 * 1024);

async function bodies(table: string): Promise<Value[]> {
  const result = await session.query(`SELECT body FROM ${table} ORDER BY id`);
  return columnValues(result, "body");
}

describe("large rows", () => {
  it("stores rows larger than a page", async () => {
    await session.query("INSERT INTO t (body) VALUES ($1)", [BIG]);
    await session.query("INSERT INTO t (body) VALUES ('small')");
    expect(await bodies("t")).toEqual([BIG, "small"]);
    await session.query("UPDATE t SET body = 'shrunk' WHERE id = 1");
    expect(await bodies("t")).toEqual(["shrunk", "small"]);
    await expect(engine.checkpoint()).resolves.toBeUndefined();
  });

  it("stores many medium rows in one table", async () => {
    const medium = "y".repeat(500);
    for (let count = 0; count < 300; count++) {
      await session.query("INSERT INTO t (body) VALUES ($1)", [medium]);
    }
    expect(await bodies("t")).toEqual(
      Array.from({ length: 300 }, () => medium),
    );
  });
});

describe("index keys too large", () => {
  it("are rejected with TOO_LARGE and leave no trace", async () => {
    const failure = session.query("INSERT INTO keyed (body) VALUES ($1)", [
      BIG,
    ]);
    await expect(failure).rejects.toMatchObject({ code: "TOO_LARGE" });
    await expect(failure).rejects.toBeInstanceOf(IndexKeyTooLargeError);
    expect(await bodies("keyed")).toEqual([]);

    await session.execute("INSERT INTO keyed (body) VALUES ('small')");
    expect(await bodies("keyed")).toEqual(["small"]);
  });

  it("fail their statement alone in a transaction", async () => {
    await session.execute("BEGIN");
    await session.execute("INSERT INTO keyed (body) VALUES ('a')");
    await expect(
      session.query("INSERT INTO keyed (body) VALUES ($1)", [BIG]),
    ).rejects.toMatchObject({ code: "TOO_LARGE" });
    await session.execute("INSERT INTO keyed (body) VALUES ('b')");
    await session.execute("COMMIT");

    expect(await bodies("keyed")).toEqual(["a", "b"]);
  });

  it("leave other tables intact", async () => {
    await expect(
      session.query("INSERT INTO keyed (body) VALUES ($1)", [BIG]),
    ).rejects.toMatchObject({ code: "TOO_LARGE" });

    for (let count = 0; count < 200; count++) {
      await session.execute("INSERT INTO t (body) VALUES ('t')");
      await session.execute("INSERT INTO keyed (body) VALUES ('w')");
    }
    expect(await bodies("t")).toEqual(Array.from({ length: 200 }, () => "t"));
    expect(await bodies("keyed")).toEqual(
      Array.from({ length: 200 }, () => "w"),
    );
  });
});
