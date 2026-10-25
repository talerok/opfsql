import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { RowTooLargeError } from "../../store/errors.js";
import type { Engine } from "../engine.js";
import type { Session } from "../session.js";
import { openTestEngine } from "./test-engine.js";

let engine: Engine;
let session: Session;
let seq = 0;

beforeEach(async () => {
  resetMockOPFS();
  engine = await openTestEngine(`large-rows-${seq++}`);
  session = engine.createSession();
  await session.execute(
    "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT)",
  );
});

afterEach(() => {
  session.close();
  engine.close();
});

const BIG = "x".repeat(40 * 1024);

async function bodies(table: string): Promise<unknown[]> {
  const [r] = await session.execute(`SELECT body FROM ${table} ORDER BY id`);
  return r.type === "rows" ? r.rows.map((row) => row.body) : [];
}

describe("large rows", () => {
  it("an oversized row is rejected and leaves no trace", async () => {
    await expect(
      session.execute("INSERT INTO t (body) VALUES ($1)", [BIG]),
    ).rejects.toThrow(RowTooLargeError);
    expect(await bodies("t")).toEqual([]);

    await session.execute("INSERT INTO t (body) VALUES ('small')");
    expect(await bodies("t")).toEqual(["small"]);
    await expect(engine.checkpoint()).resolves.toBeUndefined();
  });

  it("a failed statement leaves other tables intact", async () => {
    await session.execute(
      "CREATE TABLE w (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT)",
    );
    await expect(
      session.execute("INSERT INTO t (body) VALUES ($1)", [BIG]),
    ).rejects.toThrow(RowTooLargeError);

    for (let i = 0; i < 200; i++) {
      await session.execute("INSERT INTO t (body) VALUES ('t')");
      await session.execute("INSERT INTO w (body) VALUES ('w')");
    }
    expect(await bodies("t")).toEqual(Array(200).fill("t"));
    expect(await bodies("w")).toEqual(Array(200).fill("w"));
  });

  it("an oversized row aborts the transaction it is in", async () => {
    await session.execute("BEGIN");
    await session.execute("INSERT INTO t (body) VALUES ('a')");
    await expect(
      session.execute("INSERT INTO t (body) VALUES ($1)", [BIG]),
    ).rejects.toThrow(RowTooLargeError);
    await expect(session.execute("COMMIT")).rejects.toThrow(
      "COMMIT treated as ROLLBACK",
    );

    expect(await bodies("t")).toEqual([]);
    await session.execute("INSERT INTO t (body) VALUES ('b')");
    expect(await bodies("t")).toEqual(["b"]);
  });

  it("stores many medium rows in one table", async () => {
    const medium = "y".repeat(500);
    for (let i = 0; i < 300; i++) {
      await session.execute("INSERT INTO t (body) VALUES ($1)", [medium]);
    }
    expect(await bodies("t")).toEqual(Array(300).fill(medium));
  });

  // A known limit, lifted by overflow pages. it.fails passes while the limit
  // holds and flags the test once it is lifted.
  it.fails("stores rows larger than a page", async () => {
    await session.execute("INSERT INTO t (body) VALUES ($1)", [BIG]);
    expect(await bodies("t")).toEqual([BIG]);
  });
});
