import { InternalError } from "../../errors.js";
import type { BTreePage, BTreePageType } from "../format/btree-page.js";
import {
  cellFootprint,
  interiorCellChild,
  interiorCellKey,
  isInterior,
  leafCellKey,
  withInteriorChild,
} from "../format/btree-page.js";
import type { TreeKey } from "./types.js";

// ---------------------------------------------------------------------------
// Cutting a node that outgrew its page in two. A node splits where the
// halves are closest in bytes — except at the right edge of the tree, where
// only the new cell moves, so ascending inserts leave full pages behind.
// ---------------------------------------------------------------------------

/** The cells of a node about to be written: too many for one page. */
export interface NodeContent {
  readonly type: BTreePageType;
  readonly cells: readonly Uint8Array[];
  readonly rightChild: number;
}

/** Where a node splits: where the halves are closest in bytes, or just before the last cell. */
export type SplitStyle = "balanced" | "right-edge";

export interface Split {
  readonly left: NodeContent;
  readonly separator: TreeKey;
  readonly right: NodeContent;
}

export function contentOf(page: BTreePage): NodeContent {
  const cells = Array.from({ length: page.cellCount }, (_, index) =>
    page.cell(index).slice(),
  );
  return {
    type: page.type,
    cells,
    rightChild: page.isLeaf ? 0 : page.rightChild,
  };
}

/** `content` with `cell` inserted at `index`. */
export function withCell(
  content: NodeContent,
  index: number,
  cell: Uint8Array,
): NodeContent {
  return { ...content, cells: content.cells.toSpliced(index, 0, cell) };
}

/** `content` with child `index` (a cell's left child, or the rightmost) set to `child`. */
export function withChild(
  content: NodeContent,
  index: number,
  child: number,
): NodeContent {
  const cell = content.cells[index];
  const isRightmost = cell === undefined;
  if (isRightmost) return { ...content, rightChild: child };
  const cells = content.cells.with(index, withInteriorChild(cell, child));
  return { ...content, cells };
}

export function cut(content: NodeContent, style: SplitStyle): Split {
  if (isInterior(content.type)) return cutInterior(content, style);
  return cutLeaf(content, style);
}

/** Leaves split between cells; the right half's first key separates them. */
function cutLeaf(content: NodeContent, style: SplitStyle): Split {
  const { type, cells } = content;
  const at = style === "right-edge" ? cells.length - 1 : balancedCut(cells);
  const right = cells.slice(at);
  const [firstRight] = right;
  if (firstRight === undefined) {
    throw new InternalError("a leaf too small to split");
  }
  return {
    left: { type, cells: cells.slice(0, at), rightChild: 0 },
    separator: leafCellKey(type, firstRight),
    right: { type, cells: right, rightChild: 0 },
  };
}

/**
 * Interior nodes split around a cell that moves up: its key separates the
 * halves and its left child becomes the left half's rightmost child.
 */
function cutInterior(content: NodeContent, style: SplitStyle): Split {
  const { type, cells, rightChild } = content;
  // At the right edge the last cell stays as the right half's only one, and
  // the one before it moves up.
  const at =
    style === "right-edge" ? cells.length - 2 : balancedPromotion(cells);
  const promoted = cells[at];
  if (promoted === undefined) {
    throw new InternalError("an interior node too small to split");
  }
  return {
    left: {
      type,
      cells: cells.slice(0, at),
      rightChild: interiorCellChild(promoted),
    },
    separator: interiorCellKey(type, promoted),
    right: { type, cells: cells.slice(at + 1), rightChild },
  };
}

/**
 * Where to cut leaf cells so that the larger half is smallest. Cutting at
 * `at` keeps cells before it on the left.
 */
function balancedCut(cells: readonly Uint8Array[]): number {
  const bytesBefore = prefixSums(footprints(cells));
  const total = bytesBefore(cells.length);
  return cheapestCut(cells.length - 1, (at) =>
    Math.max(bytesBefore(at), total - bytesBefore(at)),
  );
}

/**
 * Which interior cell to move up so that the larger half is smallest: the
 * cells before it stay left, the cells after it go right.
 */
function balancedPromotion(cells: readonly Uint8Array[]): number {
  const bytesBefore = prefixSums(footprints(cells));
  const total = bytesBefore(cells.length);
  return cheapestCut(cells.length - 2, (at) =>
    Math.max(bytesBefore(at), total - bytesBefore(at + 1)),
  );
}

function footprints(cells: readonly Uint8Array[]): number[] {
  return cells.map((cell) => cellFootprint(cell));
}

/** sum(n): the total of the first `n` sizes. */
function prefixSums(sizes: readonly number[]): (count: number) => number {
  const sums = [0];
  for (const size of sizes) sums.push((sums.at(-1) ?? 0) + size);
  return (count) => sums[count] ?? 0;
}

/**
 * The cut from 1 to `last` with the lowest cost, the first on a tie. Cuts
 * start at 1 so the left half keeps at least one cell.
 */
function cheapestCut(last: number, cost: (at: number) => number): number {
  let best = 1;
  for (let at = 2; at <= last; at++) {
    if (cost(at) < cost(best)) best = at;
  }
  return best;
}
