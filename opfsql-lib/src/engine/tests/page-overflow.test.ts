import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { PageOverflowError } from "../../store/errors.js";
import type { Engine } from "../engine.js";
import type { Session } from "../session.js";
import { openTestEngine } from "./test-engine.js";

let engine: Engine;
let session: Session;
let seq = 0;

beforeEach(async () => {
  resetMockOPFS();
  engine = await openTestEngine(`page-overflow-${seq++}`);
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

describe("page overflow", () => {
  it("an oversized row is rejected and leaves no trace", async () => {
    await expect(
      session.execute("INSERT INTO t (body) VALUES ($1)", [BIG]),
    ).rejects.toThrow(PageOverflowError);
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
    ).rejects.toThrow(PageOverflowError);

    // Enough rows to split leaves in both tables after the failure.
    for (let i = 0; i < 200; i++) {
      await session.execute("INSERT INTO t (body) VALUES ('t')");
      await session.execute("INSERT INTO w (body) VALUES ('w')");
    }
    expect(await bodies("t")).toEqual(Array(200).fill("t"));
    expect(await bodies("w")).toEqual(Array(200).fill("w"));
  });

  it("a failed COMMIT rolls back the whole transaction", async () => {
    await session.execute("BEGIN");
    await session.execute("INSERT INTO t (body) VALUES ('a')");
    await session.execute("INSERT INTO t (body) VALUES ($1)", [BIG]);
    await expect(session.execute("COMMIT")).rejects.toThrow(PageOverflowError);

    expect(await bodies("t")).toEqual([]);
    // The session is out of the transaction and usable again.
    await session.execute("INSERT INTO t (body) VALUES ('b')");
    expect(await bodies("t")).toEqual(["b"]);
  });

  it("schema from a failed commit is never published", async () => {
    await session.execute("BEGIN");
    await session.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, body TEXT)");
    await session.execute("INSERT INTO u (id, body) VALUES (1, $1)", [BIG]);
    await expect(session.execute("COMMIT")).rejects.toThrow(PageOverflowError);

    const other = engine.createSession();
    expect(other.getSchema().tables.map((t) => t.name)).toEqual(["t"]);
    await expect(other.execute("SELECT * FROM u")).rejects.toThrow();
    other.close();
  });

  // Known limits, lifted by the next steps of the page-overflow work.
  // it.fails passes while the limit holds and flags the test once it is lifted.

  it.fails(
    "stores many medium rows in one table (byte-aware splits)",
    async () => {
      for (let i = 0; i < 300; i++) {
        await session.execute("INSERT INTO t (body) VALUES ($1)", [
          "y".repeat(500),
        ]);
      }
      expect(await bodies("t")).toHaveLength(300);
    },
  );

  it.fails("stores rows larger than a page (overflow pages)", async () => {
    await session.execute("INSERT INTO t (body) VALUES ($1)", [BIG]);
    expect(await bodies("t")).toEqual([BIG]);
  });
});
