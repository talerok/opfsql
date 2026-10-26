import { describe, expect, it } from "vitest";
import { MisuseError } from "../../../errors.js";
import { openPager } from "../../open.js";
import {
  beginRead,
  beginWrite,
  memoryFiles,
} from "../../tests/pager-support.js";
import type { Pager } from "../pager.js";
import type { ReadTransaction, WriteTransaction } from "../transaction.js";

// A savepoint lives in one write transaction's memory: rolling back to it
// restores every page it saw change, and nothing of it reaches the WAL.

async function newPager(): Promise<Pager> {
  return openPager(memoryFiles(), { name: "savepoints", pageSize: 4096 });
}

/** Every page the transaction sees, as text, to compare states. */
function fingerprint(transaction: ReadTransaction): string {
  const pages = Array.from({ length: transaction.pageCount }, (_, pageNo) =>
    transaction.read(pageNo).join(","),
  );
  return `${String(transaction.pageCount)}:${pages.join("|")}`;
}

/** Writes `value` into the page, so a later fingerprint tells the change. */
function mark(
  transaction: WriteTransaction,
  pageNo: number,
  value: number,
): void {
  const page = transaction.write(pageNo);
  page[100] = value;
}

/** A database with three pages of its own committed: 3, 4 and 5. */
async function withPages(): Promise<{
  pager: Pager;
  transaction: WriteTransaction;
}> {
  const pager = await newPager();
  const setup = beginWrite(pager);
  for (let count = 0; count < 3; count++) setup.allocate();
  setup.commit();
  return { pager, transaction: beginWrite(pager) };
}

describe("savepoints", () => {
  it("undo writes, allocations, frees and the schema version made since", async () => {
    const { pager, transaction } = await withPages();
    mark(transaction, 3, 1);
    const before = fingerprint(transaction);

    const savepoint = transaction.savepoint();
    mark(transaction, 3, 2);
    mark(transaction, 4, 2);
    transaction.allocate();
    transaction.free(5);
    transaction.bumpSchemaVersion();
    transaction.rollbackTo(savepoint);

    expect(fingerprint(transaction)).toBe(before);
    transaction.commit();
    const reader = beginRead(pager);
    expect(reader.read(3)[100]).toBe(1);
    expect(reader.read(4)[100]).toBe(0);
    expect(reader.schemaVersion).toBe(0);
    reader.end();
  });

  it("give back the pages a rolled back statement allocated", async () => {
    const { transaction } = await withPages();
    const savepoint = transaction.savepoint();
    const taken = Array.from({ length: 20 }, () => transaction.allocate());
    transaction.rollbackTo(savepoint);
    const again = Array.from({ length: 20 }, () => transaction.allocate());
    expect(again).toEqual(taken);
  });

  it("restore a page changed before the savepoint and freed after it", async () => {
    const { transaction } = await withPages();
    mark(transaction, 5, 7);
    const savepoint = transaction.savepoint();
    transaction.free(5);
    transaction.rollbackTo(savepoint);
    expect(transaction.read(5)[100]).toBe(7);
  });

  it("restore a freed page's content when the statement that reused it fails", async () => {
    const { pager, transaction: setup } = await withPages();
    mark(setup, 4, 9);
    setup.commit();
    const freeing = beginWrite(pager);
    freeing.free(4);
    freeing.commit();

    const transaction = beginWrite(pager);
    const before = fingerprint(transaction);
    const savepoint = transaction.savepoint();
    expect(transaction.allocate()).toBe(4);
    transaction.rollbackTo(savepoint);
    expect(fingerprint(transaction)).toBe(before);
  });

  it("nest: an inner rollback keeps the outer's changes, an outer one undoes both", async () => {
    const { transaction } = await withPages();
    const start = fingerprint(transaction);
    const outer = transaction.savepoint();
    mark(transaction, 3, 1);
    const afterOuter = fingerprint(transaction);

    const inner = transaction.savepoint();
    mark(transaction, 3, 2);
    mark(transaction, 4, 2);
    transaction.rollbackTo(inner);
    expect(fingerprint(transaction)).toBe(afterOuter);

    mark(transaction, 4, 3);
    transaction.release(inner);
    transaction.rollbackTo(outer);
    expect(fingerprint(transaction)).toBe(start);
  });

  it("refuse a savepoint that is no longer open", async () => {
    const { transaction } = await withPages();
    const savepoint = transaction.savepoint();
    transaction.release(savepoint);
    expect(() => {
      transaction.rollbackTo(savepoint);
    }).toThrow(MisuseError);
  });
});
