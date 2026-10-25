import { CorruptError } from "../../errors.js";
import { compareBytes } from "../../values/index.js";
import type { BTreePageType } from "../format/btree-page.js";
import { BTreePage, interiorCellKey } from "../format/btree-page.js";
import type { TreeKey, TreeKind } from "./types.js";
import type { PageReader } from "./page-store.js";

// Structural check of one B-tree: what the integrity check reports for it.

export interface TreeReport {
  /** Every page of the tree, for the page accounting. */
  readonly pages: number[];
  readonly entries: number;
  readonly problems: string[];
}

interface Bounds {
  readonly low: TreeKey | undefined;
  readonly high: TreeKey | undefined;
}

const LEAF: Record<TreeKind, BTreePageType> = {
  table: "table-leaf",
  index: "index-leaf",
};

export function checkTree(
  pages: PageReader,
  root: number,
  kind: TreeKind,
): TreeReport {
  return new TreeChecker(pages, kind).run(root);
}

function compareKeys(a: TreeKey, b: TreeKey): number {
  if (typeof a === "number" && typeof b === "number") return a - b;
  if (a instanceof Uint8Array && b instanceof Uint8Array) {
    return compareBytes(a, b);
  }
  throw new CorruptError("a tree mixes rowids and keys");
}

function describe(key: TreeKey): string {
  return typeof key === "number" ? String(key) : `[${key.join(",")}]`;
}

class TreeChecker {
  private readonly pagesSeen: number[] = [];
  private readonly problems: string[] = [];
  private entries = 0;
  private leafDepth: number | null = null;

  constructor(
    private readonly pages: PageReader,
    private readonly kind: TreeKind,
  ) {}

  run(root: number): TreeReport {
    this.visit(root, 1, { low: undefined, high: undefined });
    return {
      pages: this.pagesSeen,
      entries: this.entries,
      problems: this.problems,
    };
  }

  private visit(pageNo: number, depth: number, bounds: Bounds): void {
    this.pagesSeen.push(pageNo);
    const page = this.readPage(pageNo);
    if (page === null) return;
    for (const problem of page.layoutProblems()) this.report(pageNo, problem);
    if (page.isLeaf) this.checkLeaf(pageNo, page, depth, bounds);
    else this.checkInterior(pageNo, page, depth, bounds);
  }

  /** The page as a B-tree page, or null after reporting why it is not one. */
  private readPage(pageNo: number): BTreePage | null {
    try {
      return new BTreePage(this.pages.read(pageNo));
    } catch (error) {
      this.report(
        pageNo,
        error instanceof Error ? error.message : String(error),
      );
      return null;
    }
  }

  private checkLeaf(
    pageNo: number,
    page: BTreePage,
    depth: number,
    bounds: Bounds,
  ): void {
    if (page.type !== LEAF[this.kind]) {
      this.report(pageNo, `is a ${page.type} page in a ${this.kind} tree`);
    }
    this.leafDepth ??= depth;
    if (depth !== this.leafDepth) {
      this.report(
        pageNo,
        `leaf at depth ${String(depth)}, the first at ${String(this.leafDepth)}`,
      );
    }
    this.entries += page.cellCount;
    this.checkKeys(pageNo, this.keysOf(page), bounds);
  }

  private checkInterior(
    pageNo: number,
    page: BTreePage,
    depth: number,
    bounds: Bounds,
  ): void {
    if (page.cellCount === 0)
      this.report(pageNo, "interior page without cells");
    const keys = Array.from({ length: page.cellCount }, (_, index) =>
      interiorCellKey(page.type, page.cell(index)),
    );
    this.checkKeys(pageNo, keys, bounds);
    // Child i holds keys from separator i − 1 up to separator i; the first
    // and last children inherit the parent's own bounds.
    for (let index = 0; index <= page.cellCount; index++) {
      this.visit(page.child(index), depth + 1, {
        low: keys[index - 1] ?? bounds.low,
        high: keys[index] ?? bounds.high,
      });
    }
  }

  private keysOf(page: BTreePage): TreeKey[] {
    return Array.from({ length: page.cellCount }, (_, index) =>
      this.kind === "table" ? page.rowid(index) : page.key(index),
    );
  }

  /** Keys ascend and stay within [low, high) from the parent's separators. */
  private checkKeys(
    pageNo: number,
    keys: readonly TreeKey[],
    bounds: Bounds,
  ): void {
    for (const [index, key] of keys.entries()) {
      const previous = keys[index - 1];
      if (previous !== undefined && compareKeys(previous, key) >= 0) {
        this.report(pageNo, `keys out of order at ${describe(key)}`);
      }
    }
    for (const key of keys) {
      if (!isWithin(key, bounds)) {
        this.report(
          pageNo,
          `key ${describe(key)} is outside its parent's separators`,
        );
      }
    }
  }

  private report(pageNo: number, problem: string): void {
    this.problems.push(`page ${String(pageNo)}: ${problem}`);
  }
}

function isWithin(key: TreeKey, { low, high }: Bounds): boolean {
  const isAboveLow = low === undefined || compareKeys(key, low) >= 0;
  const isBelowHigh = high === undefined || compareKeys(key, high) < 0;
  return isAboveLow && isBelowHigh;
}
