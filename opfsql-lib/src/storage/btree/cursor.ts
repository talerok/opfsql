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
  readonly index: number;
  readonly cellCount: number;
}

/** The leaf where a key is or would be, and the way down to it. */
export interface Descent {
  readonly path: readonly Step[];
  readonly leafPageNo: number;
  readonly leaf: BTreePage;
  /** The first cell of the leaf whose key is at least the key. */
  readonly index: number;
}

export type Edge = "first" | "last";
export type Direction = "next" | "previous";

const EDGE_TOWARD: Record<Direction, Edge> = {
  next: "first",
  previous: "last",
};
const STEP_TOWARD: Record<Direction, number> = { next: 1, previous: -1 };

export function descendToKey(
  pageAt: PageAt,
  root: number,
  key: TreeKey,
): Descent {
  const toKey = (page: BTreePage): number => page.childIndexFor(key);
  const { path, leafPageNo, leaf } = descend(pageAt, root, toKey);
  return { path, leafPageNo, leaf, index: leaf.lowerBound(key) };
}

/** A cell in a leaf, with the path that leads to it. */
export class Cursor {
  private constructor(
    private readonly pageAt: PageAt,
    private readonly path: Step[],
    private leaf: BTreePage,
    private cell: number,
  ) {}

  /** A cursor on the first or last cell under `root`. */
  static atEdge(pageAt: PageAt, root: number, edge: Edge): Cursor {
    const { path, leaf } = descend(pageAt, root, (page) =>
      edgeChild(page, edge),
    );
    return new Cursor(pageAt, path, leaf, edgeCell(leaf, edge));
  }

  /** A cursor on the first cell whose key is at least `key`. */
  static atKey(pageAt: PageAt, root: number, key: TreeKey): Cursor {
    const { path, leaf, index } = descendToKey(pageAt, root, key);
    return new Cursor(pageAt, [...path], leaf, index);
  }

  /** A cursor on the last cell whose key is below `key`. */
  static beforeKey(pageAt: PageAt, root: number, key: TreeKey): Cursor {
    const { path, leaf, index } = descendToKey(pageAt, root, key);
    return new Cursor(pageAt, [...path], leaf, index - 1);
  }

  get page(): BTreePage {
    return this.leaf;
  }

  get index(): number {
    return this.cell;
  }

  /**
   * Moves to the first cell of the next leaf, or the last cell of the
   * previous one. False when there is no such leaf.
   */
  moveToLeaf(direction: Direction): boolean {
    const branch = this.climbToBranch(direction);
    if (branch === undefined) return false;
    const edge = EDGE_TOWARD[direction];
    const child = this.pageAt(branch.pageNo).child(branch.index);
    const below = descend(this.pageAt, child, (page) => edgeChild(page, edge));
    this.path.push(...below.path);
    this.leaf = below.leaf;
    this.cell = edgeCell(below.leaf, edge);
    return true;
  }

  /**
   * Finds the deepest step with a neighboring child in `direction`, moves
   * it there, and drops the steps below it.
   */
  private climbToBranch(direction: Direction): Step | undefined {
    const delta = STEP_TOWARD[direction];
    const depth = this.path.findLastIndex((step) => canMove(step, delta));
    const step = this.path[depth];
    this.path.splice(Math.max(depth, 0));
    if (step === undefined) return undefined;
    const branch = { ...step, index: step.index + delta };
    this.path.push(branch);
    return branch;
  }
}

/** Goes down from `pageNo` to a leaf, taking the child `choose` picks in each interior page. */
function descend(
  pageAt: PageAt,
  pageNo: number,
  choose: (page: BTreePage) => number,
): { path: Step[]; leafPageNo: number; leaf: BTreePage } {
  const path: Step[] = [];
  let current = pageNo;
  let page = pageAt(current);
  while (!page.isLeaf) {
    const index = choose(page);
    path.push({ pageNo: current, index, cellCount: page.cellCount });
    current = page.child(index);
    page = pageAt(current);
  }
  return { path, leafPageNo: current, leaf: page };
}

function edgeChild(page: BTreePage, edge: Edge): number {
  return edge === "first" ? 0 : page.cellCount;
}

function edgeCell(leaf: BTreePage, edge: Edge): number {
  return edge === "first" ? 0 : leaf.cellCount - 1;
}

/** Whether the child next to the step's, `delta` away, exists. */
function canMove(step: Step, delta: number): boolean {
  const index = step.index + delta;
  return index >= 0 && index <= step.cellCount;
}
