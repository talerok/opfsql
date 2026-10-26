import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { BATCH_SIZE } from "../../executor/operator.js";
import type { Engine } from "../engine.js";
import type { Session } from "../session.js";
import type { Value } from "../../values/index.js";
import { columnValues } from "../../../test/support/results.js";
import { openTestEngine } from "./test-engine.js";

// The AUTOINCREMENT sequence lives with the table's data, not in the catalog,
// so inserts never change the schema.

let engine: Engine;
let session: Session;
let databaseName: string;
let seq = 0;

beforeEach(async () => {
  resetMockOPFS();
  databaseName = `autoincrement-${String(seq++)}`;
  engine = await openTestEngine(databaseName);
  session = engine.connect();
  await session.execute(
    "CREATE TABLE a (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
  );
  await session.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, name TEXT)");
});

afterEach(async () => {
  await session.close();
  await engine.close();
});

/** The ids in table a, in order. */
async function idsInA(): Promise<Value[]> {
  const result = await session.query("SELECT id FROM a ORDER BY id");
  return columnValues(result, "id");
}

describe("AUTOINCREMENT", () => {
  it("leaves prepared statements valid, including on other tables", async () => {
    const insert = await session.prepare("INSERT INTO a (name) VALUES ($1)");
    const select = await session.prepare("SELECT name FROM b WHERE id = $1");

    for (const name of ["x", "y", "z"]) await insert.query([name]);
    const selected = await select.query([1]);
    expect(selected.rows).toEqual([]);
    expect(await idsInA()).toEqual([1, 2, 3]);
  });

  it("does not change the schema", async () => {
    const before = await session.schema();
    await session.execute("INSERT INTO a (name) VALUES ('x')");
    await session.execute("INSERT INTO a (id, name) VALUES (10, 'y')");
    expect(await session.schema()).toEqual(before);
  });

  it("keeps the sequence across reopening the database", async () => {
    await session.execute("INSERT INTO a (name) VALUES ('x')");
    await session.execute("INSERT INTO a (name) VALUES ('y')");
    await session.execute("DELETE FROM a WHERE id = 2");
    await session.close();
    await engine.close();

    engine = await openTestEngine(databaseName);
    session = engine.connect();
    await session.execute("INSERT INTO a (name) VALUES ('z')");
    expect(await idsInA()).toEqual([1, 3]);
  });

  it("reads a source that reads opfsql_sequence whole before it inserts", async () => {
    // A batch of rows of b, then opfsql_sequence: streamed, the source would
    // read opfsql_sequence after the first batch was inserted into a, and
    // meet a's own counter there.
    const names = Array.from(
      { length: BATCH_SIZE },
      (_, index) => `('b${String(index)}')`,
    );
    await session.execute(`INSERT INTO b (name) VALUES ${names.join(", ")}`);
    await session.execute(
      "INSERT INTO a (name) SELECT name FROM b UNION ALL SELECT name FROM opfsql_sequence",
    );
    const result = await session.query(
      "SELECT count(*) AS copied, count(CASE WHEN name = 'a' THEN 1 END) AS own FROM a",
    );
    expect(result.objects()).toEqual([{ copied: BATCH_SIZE, own: 0 }]);
  });
});
