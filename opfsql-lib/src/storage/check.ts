import { CorruptError } from "../errors.js";
import { BTreeReader } from "./btree/btree.js";
import type { TreeGuard } from "./btree/guard.js";
import { checkTree } from "./btree/check.js";
import type { PageReader } from "./btree/page-store.js";
import type { TreeKind } from "./btree/types.js";
import {
  AllocationMapReader,
  MAP_PAGE_TYPE,
  mapPageNumbers,
} from "./pager/allocation.js";
import { HEADER_PAGE, SCHEMA_ROOT_PAGE } from "./pager/database-file.js";
import type { ChainReference } from "./rows/overflow.js";
import { chainPages } from "./rows/overflow.js";
import type { StoredSchemaEntry } from "./rows/schema-table.js";
import { readSchemaTable, SCHEMA_TABLE_NAME } from "./rows/schema-table.js";
import { StoredRow } from "./rows/table-store.js";

// ---------------------------------------------------------------------------
// The storage part of the integrity check (docs/file-format.md, Integrity):
// every B-tree and overflow chain is well formed, and every page is used
// once — by the header, an allocation map, a tree or a chain — exactly when
// its allocation map marks it in use. What rows mean to their table (column
// counts, index entries, constraints) is checked above storage.
// ---------------------------------------------------------------------------

export interface SnapshotPages extends PageReader {
  /** Pages in the database as of the snapshot. */
  readonly pageCount: number;
}

interface Tree {
  /** What problems are reported under: "opfsql_schema", "table users". */
  readonly name: string;
  readonly root: number;
  readonly kind: TreeKind;
}

const SCHEMA_TREE: Tree = {
  name: SCHEMA_TABLE_NAME,
  root: SCHEMA_ROOT_PAGE,
  kind: "table",
};

/** The problems found in the pages of one snapshot; none if it is sound. */
export function checkStorage(pages: SnapshotPages): string[] {
  return new StorageCheck(pages).run();
}

class StorageCheck {
  private readonly problems: string[] = [];
  /** What uses each page in the database. */
  private readonly owners = new Map<number, string>();

  constructor(private readonly pages: SnapshotPages) {}

  run(): string[] {
    this.claim(HEADER_PAGE, "the header");
    this.checkMapPages();
    // The other trees are found through the schema table, so only a sound
    // one is read.
    const isSchemaSound = this.checkTreeAndRows(SCHEMA_TREE);
    if (isSchemaSound) {
      for (const tree of this.schemaTrees()) this.checkTreeAndRows(tree);
    }
    this.checkAllocation();
    return this.problems;
  }

  private checkMapPages(): void {
    const { pageSize, pageCount } = this.pages;
    for (const pageNo of mapPageNumbers(pageSize, pageCount)) {
      this.claim(pageNo, "an allocation map");
      if (this.pages.read(pageNo)[0] !== MAP_PAGE_TYPE) {
        this.report("an allocation map", `page ${String(pageNo)} is not one`);
      }
    }
  }

  private schemaTrees(): Tree[] {
    try {
      return readSchemaTable(this.pages).map((entry) => treeOf(entry));
    } catch (error) {
      if (!(error instanceof CorruptError)) throw error;
      this.report(SCHEMA_TABLE_NAME, error.message);
      return [];
    }
  }

  /** Checks a tree, and the rows of a table tree. True if the tree is sound. */
  private checkTreeAndRows(tree: Tree): boolean {
    const report = checkTree(this.pages, tree.root, tree.kind);
    for (const problem of report.problems) this.report(tree.name, problem);
    for (const pageNo of report.pages) this.claim(pageNo, tree.name);
    const isSound = report.problems.length === 0;
    if (isSound && tree.kind === "table") this.checkRows(tree);
    return isSound;
  }

  private checkRows(tree: Tree): void {
    const rows = new BTreeReader(this.pages, tree.root, "table");
    const guard = rows.guard();
    for (const { page, index } of rows.ascending()) {
      const row = { rowid: page.rowid(index), record: page.record(index) };
      this.checkRow(tree.name, row, guard);
    }
  }

  /** The row's record must parse, and its overflow chains hold their bytes. */
  private checkRow(
    owner: string,
    { rowid, record }: { rowid: number; record: Uint8Array },
    guard: TreeGuard,
  ): void {
    try {
      const row = new StoredRow(this.pages, rowid, record, guard);
      for (const chain of row.overflowReferences())
        this.claimChain(owner, chain);
    } catch (error) {
      if (!(error instanceof CorruptError)) throw error;
      this.report(owner, `row ${String(rowid)}: ${error.message}`);
    }
  }

  private claimChain(owner: string, chain: ChainReference): void {
    for (const { pageNo } of chainPages(this.pages, chain)) {
      this.claim(pageNo, owner);
    }
  }

  /** Pages used must be marked in use, and pages marked in use must be used. */
  private checkAllocation(): void {
    const { pageCount } = this.pages;
    const map = new AllocationMapReader(this.pages);
    const marked = new Set(map.markedPages(pageCount));
    for (const [pageNo, owner] of this.owners) {
      if (!marked.has(pageNo))
        this.report(owner, `page ${String(pageNo)} is marked free`);
    }
    for (const pageNo of marked) {
      if (!this.owners.has(pageNo)) {
        this.report(
          `page ${String(pageNo)}`,
          "is marked in use, but nothing uses it",
        );
      }
    }
    if (map.hasBitsPast(pageCount)) {
      this.report("an allocation map", "marks pages past the end in use");
    }
  }

  private claim(pageNo: number, owner: string): void {
    const problem = this.claimProblem(pageNo);
    if (problem === null) this.owners.set(pageNo, owner);
    else this.report(owner, problem);
  }

  private claimProblem(pageNo: number): string | null {
    const page = `page ${String(pageNo)}`;
    if (pageNo >= this.pages.pageCount) return `${page} is past the end`;
    const previous = this.owners.get(pageNo);
    if (previous !== undefined) return `${page} is used by ${previous} too`;
    return null;
  }

  private report(owner: string, problem: string): void {
    this.problems.push(`${owner}: ${problem}`);
  }
}

function treeOf(entry: StoredSchemaEntry): Tree {
  return {
    name: `${entry.type} ${entry.name}`,
    root: entry.rootPage,
    kind: entry.type,
  };
}
