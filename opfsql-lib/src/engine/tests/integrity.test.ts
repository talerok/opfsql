import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, expect, it } from "vitest";
import { OPFSSyncStorage } from "../../store/backend/opfs-storage.js";
import type { InternalNode, LeafNode, Node } from "../../store/btree.js";
import {
  encodePage,
  encodedSize,
  type EncodedPage,
} from "../../store/page-codec.js";
import { decodeRow, encodeRow } from "../../store/row-codec.js";
import type { TableEntry } from "../../store/table-btree.js";
import type { CatalogData, TableSchema } from "../../types.js";
import { Engine } from "../engine.js";

// Each test builds a healthy database, closes it, damages its file the way a
// storage bug would, reopens it and expects checkIntegrity() to name the damage.

const CATALOG_PAGE = 1;
const FREELIST_PAGE = 2;

let seq = 0;
let dbName: string;

/** Small pages, so that table "t" spans several leaves. */
function openStorage(): OPFSSyncStorage {
  return new OPFSSyncStorage(dbName, { pageSize: 4096 });
}

beforeEach(async () => {
  resetMockOPFS();
  dbName = `integrity-${seq++}`;
  const engine = await Engine.create(openStorage());
  const s = engine.createSession();
  await s.execute(
    "CREATE TABLE t (id INTEGER PRIMARY KEY, tag INTEGER, body TEXT)",
  );
  await s.execute("CREATE INDEX t_tag ON t (tag)");
  for (let id = 1; id <= 300; id++) {
    await s.execute("INSERT INTO t (id, tag, body) VALUES ($1, $2, $3)", [
      id,
      id % 7,
      `row ${id}`.padEnd(40, "."),
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
  const engine = await Engine.create(openStorage());
  try {
    return engine.checkIntegrity();
  } finally {
    engine.close();
  }
}

type TableLeaf = LeafNode<TableEntry>;

interface RawFile {
  read<T>(pageNo: number): T;
  write(pageNo: number, value: unknown): void;
  nextPageId: number;
  catalog: CatalogData;
  table(name: string): TableSchema;
  /** Leaf pages of table `name`, in key order. */
  leaves(name: string): number[];
}

/** Edits pages of the closed database file directly, bypassing the engine. */
async function damage(edit: (file: RawFile) => void): Promise<void> {
  const storage = openStorage();
  await storage.open();
  const pages = new Map<number, EncodedPage>();
  const read = <T>(pageNo: number): T => {
    const value = storage.readPage<T>(pageNo);
    if (value === null) throw new Error(`page ${pageNo} is empty`);
    return structuredClone(value);
  };
  const catalog = read<CatalogData>(CATALOG_PAGE);
  const table = (name: string) => catalog.tables.find((t) => t.name === name)!;
  const file: RawFile = {
    read,
    write: (pageNo, value) => pages.set(pageNo, encodePage(value)),
    nextPageId: storage.getNextPageId(),
    catalog,
    table,
    leaves(name) {
      const result: number[] = [];
      const visit = (pageNo: number) => {
        const node = read<Node<number, TableEntry>>(pageNo);
        if (node.kind === "leaf") result.push(pageNo);
        else node.children.forEach(visit);
      };
      visit(table(name).rootPageNo!);
      return result;
    },
  };
  edit(file);
  storage.commit(pages, file.nextPageId);
  storage.close();
}

/** Replaces entry `i` of a table leaf, keeping its recorded size exact. */
function withEntry(leaf: TableLeaf, i: number, entry: TableEntry): TableLeaf {
  const bytes = leaf.bytes - encodedSize(leaf.entries[i]) + encodedSize(entry);
  const entries = leaf.entries.map((e, j) => (j === i ? entry : e));
  return { ...leaf, entries, bytes };
}

describe("checkIntegrity", () => {
  it("finds nothing wrong with a healthy database", async () => {
    expect(await check()).toEqual([]);
  });

  it("detects a page shared by two trees", async () => {
    await damage((file) => {
      const index = file.catalog.indexes.find((i) => i.name === "t_tag")!;
      index.rootPageNo = file.table("t").rootPageNo;
      file.write(CATALOG_PAGE, file.catalog);
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
      used = file.leaves("t")[0];
      file.write(FREELIST_PAGE, [used]);
    });
    expect(await check()).toContainEqual(
      expect.stringMatching(
        new RegExp(`page ${used} is used by .* and freelist`),
      ),
    );
  });

  it("detects keys out of order", async () => {
    await damage((file) => {
      const pageNo = file.leaves("t")[0];
      const leaf = file.read<TableLeaf>(pageNo);
      [leaf.entries[0], leaf.entries[1]] = [leaf.entries[1], leaf.entries[0]];
      file.write(pageNo, leaf);
    });
    expect(await check()).toContainEqual(
      expect.stringMatching(/table "t": page \d+ keys out of order/),
    );
  });

  it("detects a key outside its parent's separators", async () => {
    await damage((file) => {
      const pageNo = file.leaves("t")[1];
      const leaf = file.read<TableLeaf>(pageNo);
      file.write(pageNo, withEntry(leaf, 0, [1, leaf.entries[0][1]]));
    });
    expect(await check()).toContainEqual(
      expect.stringMatching(
        /table "t": page \d+ key 1 is outside its parent's separators/,
      ),
    );
  });

  it("detects a node whose recorded size is wrong", async () => {
    let pageNo = 0;
    await damage((file) => {
      pageNo = file.table("t").rootPageNo!;
      const root = file.read<InternalNode<number>>(pageNo);
      file.write(pageNo, { ...root, bytes: root.bytes - 1 });
    });
    expect(await check()).toContainEqual(
      expect.stringMatching(
        new RegExp(
          `table "t": page ${pageNo} records \\d+ bytes of items but holds \\d+`,
        ),
      ),
    );
  });

  it("detects an index with more entries than its table has rows", async () => {
    await damage((file) => {
      const pageNo = file.leaves("t")[0];
      const leaf = file.read<TableLeaf>(pageNo);
      const bytes = leaf.bytes - encodedSize(leaf.entries[0]);
      file.write(pageNo, { ...leaf, entries: leaf.entries.slice(1), bytes });
    });
    expect(await check()).toContain(
      'index "t_tag" has 240 entries for 239 rows of "t"',
    );
  });

  it("detects an AUTOINCREMENT sequence behind the ids in use", async () => {
    await damage((file) => {
      file.write(file.table("a").sequencePageNo!, 1);
    });
    expect(await check()).toContain(
      'table "a": AUTOINCREMENT sequence 1 is behind id 3',
    );
  });

  it("detects a row whose index entry does not match it", async () => {
    let rowId = 0;
    await damage((file) => {
      const pageNo = file.leaves("t")[0];
      const leaf = file.read<TableLeaf>(pageNo);
      const [id, cell] = leaf.entries[0];
      rowId = id;
      const schema = file.table("t");
      const row = decodeRow(schema, cell);
      file.write(
        pageNo,
        withEntry(leaf, 0, [id, encodeRow(schema, { ...row, tag: 99 })]),
      );
    });
    expect(await check()).toEqual([
      `index "t_tag" has no entry [99] for row ${rowId} of "t"`,
    ]);
  });
});
