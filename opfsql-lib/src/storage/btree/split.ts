import { InternalError } from "../../errors.js";
import type { BTreePage, BTreePageType } from "../format/btree-page.js";
import {
  interiorCellChild,
  interiorCellKey,
  leafCellKey,
  withInteriorChild,
} from "../format/btree-page.js";
import type { TreeKey } from "./types.js";

// ---------------------------------------------------------------------------
// Cutting a node that outgrew its page in two. A node splits where the
// halves are closest in bytes — except at the right edge of the tree, where
// only the new cell moves, so ascending inserts leave full pages behind.
// ---------------------------------------------------------------------------

const POINTER_SIZE = 2;

/** The cells of a node about to be written: too many for one page. */
export interface NodeContent {
  readonly type: BTreePageType;
  readonly cells: Uint8Array[];
  readonly rightChild: number;
}

export interface Split {
  readonly left: NodeContent;
  readonly separator: TreeKey;
  readonly right: NodeContent;
}

export function contentOf(page: BTreePage): NodeContent {
  const cells = Array.from({ length: page.cellCount }, (_, index) =>
    Uint8Array.from(page.cell(index)),
  );
  return {
    type: page.type,
    cells,
    rightChild: page.isLeaf ? 0 : page.rightChild,
  };
}

/** `content` with child `index` (a cell's left child, or the rightmost) set to `child`. */
export function withChild(
  content: NodeContent,
  index: number,
  child: number,
): NodeContent {
  const cell = content.cells[index];
  if (cell === undefined) return { ...content, rightChild: child };
  return {
    ...content,
    cells: content.cells.with(index, withInteriorChild(cell, child)),
  };
}

export function cut(content: NodeContent, isAtRightEdge: boolean): Split {
  return content.type.endsWith("leaf")
    ? cutLeaf(content, isAtRightEdge)
    : cutInterior(content, isAtRightEdge);
}

/** Leaves split between cells; the right half's first key separates them. */
function cutLeaf(content: NodeContent, isAtRightEdge: boolean): Split {
  const { type, cells } = content;
  const at = isAtRightEdge ? cells.length - 1 : balancedCut(cells);
  const right = cells.slice(at);
  return {
    left: { type, cells: cells.slice(0, at), rightChild: 0 },
    separator: leafCellKey(type, right[0] ?? new Uint8Array()),
    right: { type, cells: right, rightChild: 0 },
  };
}

/**
 * Interior nodes split around a cell that moves up: its key separates the
 * halves and its left child becomes the left half's rightmost child.
 */
function cutInterior(content: NodeContent, isAtRightEdge: boolean): Split {
  const { type, cells, rightChild } = content;
  const at = isAtRightEdge ? cells.length - 2 : balancedPromotion(cells);
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

/** Bytes each cell takes in a page, its pointer included. */
function footprints(cells: readonly Uint8Array[]): number[] {
  return cells.map((cell) => cell.length + POINTER_SIZE);
}

/** Where to cut so that the larger half is smallest; both keep a cell. */
function balancedCut(cells: readonly Uint8Array[]): number {
  const sizes = footprints(cells);
  const total = sizes.reduce((sum, size) => sum + size, 0);
  let best = 1;
  let bestLarger = Infinity;
  let left = 0;
  for (let at = 1; at < sizes.length; at++) {
    left += sizes[at - 1] ?? 0;
    const larger = Math.max(left, total - left);
    if (larger >= bestLarger) continue;
    best = at;
    bestLarger = larger;
  }
  return best;
}

/** Which cell to move up so that the larger half is smallest; both keep a cell. */
function balancedPromotion(cells: readonly Uint8Array[]): number {
  const sizes = footprints(cells);
  const total = sizes.reduce((sum, size) => sum + size, 0);
  let best = 1;
  let bestLarger = Infinity;
  let left = sizes[0] ?? 0;
  for (let at = 1; at < sizes.length - 1; at++) {
    const promoted = sizes[at] ?? 0;
    const larger = Math.max(left, total - left - promoted);
    left += promoted;
    if (larger >= bestLarger) continue;
    best = at;
    bestLarger = larger;
  }
  return best;
}
