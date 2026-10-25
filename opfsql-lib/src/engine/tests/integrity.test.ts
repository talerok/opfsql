import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, expect, it } from "vitest";
import { OPFSSyncStorage } from "../../store/backend/opfs-storage.js";
import type { BTreeMeta, BTreeNode } from "../../store/index-btree/types.js";
import { encodePage, type EncodedPage } from "../../store/page-codec.js";
import type { TableBTreeMeta, TableNode } from "../../store/table-btree.js";
import type { CatalogData } from "../../types.js";
import { Engine } from "../engine.js";

// Each test builds a healthy database, closes it, damages its file the way a
// storage bug would, reopens it and expects checkIntegrity() to name the damage.

const CATALOG_PAGE = 1;
const FREELIST_PAGE = 2;

let seq = 0;
let dbName: string;

beforeEach(async () => {
  resetMockOPFS();
  dbName = `integrity-${seq++}`;
  const engine = await Engine.create(new OPFSSyncStorage(dbName));
  const s = engine.createSession();
  await s.execute(
    "CREATE TABLE t (id INTEGER PRIMARY KEY, tag INTEGER, body TEXT)",
  );
  await s.execute("CREATE INDEX t_tag ON t (tag)");
  for (let id = 1; id <= 300; id++) {
    await s.execute("INSERT INTO t (id, tag, body) VALUES ($1, $2, $3)", [
      id,
      id % 7,
      `row ${id}`,
    ]);
  }
  await s.execute("DELETE FROM t WHERE id % 5 = 0");
  await s.execute(
    "CREATE TABLE a (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
  );
  for (const name of ["x", "y", "z"]) {
    await s.execute("INSERT INTO a (name) VALUES ($1)", [name]);
  }
  s.close();
  engine.close();
});

async function check(): Promise<string[]> {
  const engine = await Engine.create(new OPFSSyncStorage(dbName));
  try {
    return engine.checkIntegrity();
  } finally {
    engine.close();
  }
}

interface RawFile {
  read<T>(pageNo: number): T;
  write(pageNo: number, value: unknown): void;
  nextPageId: number;
  tableMetaPage(name: string): number;
  indexMetaPage(name: string): number;
  leftmostLeaf(metaPageNo: number): number;
}

/** Edits pages of the closed database file directly, bypassing the engine. */
async function damage(edit: (file: RawFile) => void): Promise<void> {
  const storage = new OPFSSyncStorage(dbName);
  await storage.open();
  const pages = new Map<number, EncodedPage>();
  const read = <T>(pageNo: number): T => {
    const value = storage.readPage<T>(pageNo);
    if (value === null) throw new Error(`page ${pageNo} is empty`);
    return structuredClone(value);
  };
  const catalog = read<CatalogData>(CATALOG_PAGE);
  const file: RawFile = {
    read,
    write: (pageNo, value) => pages.set(pageNo, encodePage(value)),
    nextPageId: storage.getNextPageId(),
    tableMetaPage: (name) =>
      catalog.tables.find((t) => t.name === name)!.metaPageNo!,
    indexMetaPage: (name) =>
      catalog.indexes.find((i) => i.name === name)!.metaPageNo!,
    leftmostLeaf(metaPageNo) {
      let pageNo = read<BTreeMeta>(metaPageNo).rootNodeId;
      for (;;) {
        const node = read<BTreeNode>(pageNo);
        if (node.kind === "leaf") return pageNo;
        pageNo = node.children[0];
      }
    },
  };
  edit(file);
  storage.commit(pages, file.nextPageId);
  storage.close();
}

describe("checkIntegrity", () => {
  it("finds nothing wrong with a healthy database", async () => {
    expect(await check()).toEqual([]);
  });

  it("detects a page shared by two trees", async () => {
    await damage((file) => {
      const tableMeta = file.tableMetaPage("t");
      const indexRoot = file.read<BTreeMeta>(
        file.indexMetaPage("t_tag"),
      ).rootNodeId;
      const meta = file.read<TableBTreeMeta>(tableMeta);
      file.write(tableMeta, { ...meta, rootNodeId: indexRoot });
    });
    expect(await check()).toContainEqual(
      expect.stringMatching(/is used by table "t" and index "t_tag"/),
    );
  });

  it("detects a leaked page", async () => {
    let leaked = 0;
    await damage((file) => {
      leaked = file.nextPageId++;
    });
    expect(await check()).toContain(
      `page ${leaked} is neither used nor free (leaked)`,
    );
  });

  it("detects a freelist entry that is still in use", async () => {
    let used = 0;
    await damage((file) => {
      used = file.leftmostLeaf(file.tableMetaPage("t"));
      file.write(FREELIST_PAGE, [used]);
    });
    expect(await check()).toContainEqual(
      expect.stringMatching(
        new RegExp(`page ${used} is used by .* and freelist`),
      ),
    );
  });

  it("detects a broken leaf chain", async () => {
    await damage((file) => {
      const leaf = file.leftmostLeaf(file.tableMetaPage("t"));
      file.write(leaf, { ...file.read<TableNode>(leaf), nextLeafId: null });
    });
    expect(await check()).toContainEqual(
      expect.stringMatching(/table "t": leaf \d+ links to null, expected \d+/),
    );
  });

  it("detects keys out of order", async () => {
    await damage((file) => {
      const pageNo = file.leftmostLeaf(file.tableMetaPage("t"));
      const leaf = file.read<TableNode & { kind: "leaf" }>(pageNo);
      [leaf.keys[0], leaf.keys[1]] = [leaf.keys[1], leaf.keys[0]];
      [leaf.values[0], leaf.values[1]] = [leaf.values[1], leaf.values[0]];
      file.write(pageNo, leaf);
    });
    expect(await check()).toContainEqual(
      expect.stringMatching(/table "t": page \d+ keys out of order/),
    );
  });

  it("detects a row count that does not match the tree", async () => {
    await damage((file) => {
      const pageNo = file.tableMetaPage("t");
      const meta = file.read<TableBTreeMeta>(pageNo);
      file.write(pageNo, { ...meta, size: meta.size + 1 });
    });
    expect(await check()).toContain(
      'table "t": meta records 241 entries, tree holds 240',
    );
  });

  it("detects an AUTOINCREMENT sequence behind the ids in use", async () => {
    await damage((file) => {
      const pageNo = file.tableMetaPage("a");
      const meta = file.read<TableBTreeMeta>(pageNo);
      file.write(pageNo, { ...meta, lastAutoIncrement: 1 });
    });
    expect(await check()).toContain(
      'table "a": AUTOINCREMENT sequence 1 is behind id 2',
    );
  });

  it("detects a row whose index entry does not match it", async () => {
    let rowId = 0;
    await damage((file) => {
      const pageNo = file.leftmostLeaf(file.tableMetaPage("t"));
      const leaf = file.read<TableNode & { kind: "leaf" }>(pageNo);
      rowId = leaf.keys[0];
      leaf.values[0] = { ...leaf.values[0], tag: 99 };
      file.write(pageNo, leaf);
    });
    expect(await check()).toEqual([
      `index "t_tag" has no entry [99] for row ${rowId} of "t"`,
    ]);
  });
});
