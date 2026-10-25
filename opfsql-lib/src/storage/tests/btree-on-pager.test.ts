import { describe, expect, it } from "vitest";
import { BTreeWriter } from "../btree/btree.js";
import { checkTree } from "../btree/check.js";
import { tableLeafCell } from "../format/btree-page.js";
import { MemoryFile } from "../file/sync-file.js";
import type { StorageFiles } from "../open.js";
import { openPager } from "../open.js";

describe("B-trees on the pager", () => {
  it("survive commits, a checkpoint and reopening", async () => {
    const files: StorageFiles = {
      database: new MemoryFile(),
      wal: new MemoryFile(),
      marks: null,
    };
    const pager = await openPager(files, { name: "tree", pageSize: 4096 });
    const create = pager.beginWrite();
    if (create === null) throw new Error("no snapshot");
    const root = BTreeWriter.create(create, "table");
    create.commit();

    for (let batch = 0; batch < 10; batch++) {
      const transaction = pager.beginWrite();
      if (transaction === null) throw new Error("no snapshot");
      const tree = new BTreeWriter(transaction, root, "table");
      for (let rowid = batch * 500; rowid < (batch + 1) * 500; rowid++) {
        tree.insert(rowid, tableLeafCell(rowid, new Uint8Array(40)));
      }
      transaction.commit();
    }
    pager.checkpoint();
    pager.close();

    const reopened = await openPager({ ...files }, { name: "tree" });
    const reader = reopened.beginRead();
    if (reader === null) throw new Error("no snapshot");
    const report = checkTree(reader, root, "table");
    expect(report.problems).toEqual([]);
    expect(report.entries).toBe(5000);
    reader.end();
  });
});
