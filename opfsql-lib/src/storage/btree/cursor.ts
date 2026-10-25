import type { BTreePage } from "../format/btree-page.js";
import type { TreeKey } from "./types.js";

// ---------------------------------------------------------------------------
// Walking a B-tree. Pages have no sibling links, so a cursor keeps the path
// from the root: moving to the next leaf climbs to the nearest ancestor with
// a next child, and descends from there.
// ---------------------------------------------------------------------------

export type PageAt = (pageNo: number) => BTreePage;

/** An interior page on the way down to a leaf, and the child taken in it. */
export interface Step {
  readonly pageNo: number;
  index: number;
  readonly cellCount: number;
}

/** The leaf where a key is or would be, and the way down to it. */
export interface Descent {
  readonly path: Step[];
  readonly leafPageNo: number;
  readonly leaf: BTreePage;
  /** Cells of the leaf with keys below the key. */
  readonly position: number;
}

export type Edge = "first" | "last";
export type Direction = "next" | "previous";

export function descendToKey(
  pageAt: PageAt,
  root: number,
  key: TreeKey,
): Descent {
  const path: Step[] = [];
  let pageNo = root;
  let page = pageAt(pageNo);
  while (!page.isLeaf) {
    const index = page.childFor(key);
    path.push({ pageNo, index, cellCount: page.cellCount });
    pageNo = page.child(index);
    page = pageAt(pageNo);
  }
  return { path, leafPageNo: pageNo, leaf: page, position: page.position(key) };
}

/** Extends `path` from `pageNo` down to its first or last leaf, and returns that leaf. */
export function descendToEdge(
  pageAt: PageAt,
  path: Step[],
  pageNo: number,
  edge: Edge,
): BTreePage {
  let current = pageNo;
  let page = pageAt(current);
  while (!page.isLeaf) {
    const index = edge === "first" ? 0 : page.cellCount;
    path.push({ pageNo: current, index, cellCount: page.cellCount });
    current = page.child(index);
    page = pageAt(current);
  }
  return page;
}

/** A cell position in a leaf, with the path that leads to it. */
export class Cursor {
  constructor(
    private readonly pageAt: PageAt,
    private readonly path: Step[],
    public page: BTreePage,
    public index: number,
  ) {}

  /** A cursor on the first or last cell under `root`. */
  static atEdge(pageAt: PageAt, root: number, edge: Edge): Cursor {
    const path: Step[] = [];
    const leaf = descendToEdge(pageAt, path, root, edge);
    const index = edge === "first" ? 0 : leaf.cellCount - 1;
    return new Cursor(pageAt, path, leaf, index);
  }

  /** A cursor on the first cell whose key is at least `key`. */
  static atKey(pageAt: PageAt, root: number, key: TreeKey): Cursor {
    const { path, leaf, position } = descendToKey(pageAt, root, key);
    return new Cursor(pageAt, path, leaf, position);
  }

  /** A cursor on the last cell whose key is below `key`. */
  static beforeKey(pageAt: PageAt, root: number, key: TreeKey): Cursor {
    const { path, leaf, position } = descendToKey(pageAt, root, key);
    return new Cursor(pageAt, path, leaf, position - 1);
  }

  /**
   * Moves to the first cell of the next leaf, or the last cell of the
   * previous one. False when there is no such leaf.
   */
  moveToLeaf(direction: Direction): boolean {
    const branch = this.climbToBranch(direction);
    if (branch === undefined) return false;
    const child = this.pageAt(branch.pageNo).child(branch.index);
    const edge = direction === "next" ? "first" : "last";
    this.page = descendToEdge(this.pageAt, this.path, child, edge);
    this.index = direction === "next" ? 0 : this.page.cellCount - 1;
    return true;
  }

  /** Pops steps until one can take its neighbouring child, and takes it. */
  private climbToBranch(direction: Direction): Step | undefined {
    const delta = direction === "next" ? 1 : -1;
    let step = this.path.at(-1);
    while (step !== undefined && !canMove(step, delta)) {
      this.path.pop();
      step = this.path.at(-1);
    }
    if (step !== undefined) step.index += delta;
    return step;
  }
}

function canMove(step: Step, delta: number): boolean {
  const index = step.index + delta;
  return index >= 0 && index <= step.cellCount;
}
