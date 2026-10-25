import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { type Database, openDatabase } from "../client.js";
import { failureOf } from "../../../test/support/failure.js";
import { startTestWorker } from "./harness.js";

// Regression test for F11 of the review: the database's own calls ran on one
// session, so a read waited behind a write waiting for the lock.

let seq = 0;
const opened: Database[] = [];
beforeEach(() => {
  resetMockOPFS();
});
afterEach(async () => {
  await Promise.all(opened.splice(0).map(async (d) => d.close()));
});

describe("the database's own connection", () => {
  it("F11: a db.query read does not wait behind a db.query write that waits for the lock", async () => {
    const database = await openDatabase(`shared-${String(seq++)}`, {
      worker: startTestWorker(),
      busyTimeout: 1000,
    });
    opened.push(database);
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const holder = await database.connect();
    await holder.query("BEGIN IMMEDIATE");
    const write = failureOf(database.query("INSERT INTO t VALUES (1)"));
    const started = performance.now();
    await database.query("SELECT count(*) FROM t");
    expect(performance.now() - started).toBeLessThan(200); // ~1000: the busy timeout
    await write;
    await holder.query("ROLLBACK");
  });
});
