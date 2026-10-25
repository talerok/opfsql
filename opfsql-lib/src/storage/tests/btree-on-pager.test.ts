import { describe, expect, it } from "vitest";
import { BTreeWriter } from "../btree/btree.js";
import { checkTree } from "../btree/check.js";
import { tableLeafCell } from "../format/btree-page.js";
import { openPager } from "../open.js";
import { memoryFiles, read, write } from "./pager-support.js";

describe("B-trees on the pager", () => {
  it("survive commits, a checkpoint and reopening", async () => {
    const files = memoryFiles();
    const pager = await openPager(files, { name: "tree", pageSize: 4096 });
    const root = write(pager, (transaction) =>
      BTreeWriter.create(transaction, "table"),
    );
    for (let batch = 0; batch < 10; batch++) {
      write(pager, (transaction) => {
        const tree = new BTreeWriter(transaction, root, "table");
        for (let rowid = batch * 500; rowid < (batch + 1) * 500; rowid++) {
          tree.insert(rowid, tableLeafCell(rowid, new Uint8Array(40)));
        }
      });
    }
    pager.checkpoint();
    pager.close();

    const reopened = await openPager({ ...files }, { name: "tree" });
    const report = read(reopened, (transaction) =>
      checkTree(transaction, root, "table"),
    );
    expect(report.problems).toEqual([]);
    expect(report.entries).toBe(5000);
  });
});
