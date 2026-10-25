import { InternalError } from "../../errors.js";
import type { BTreePageType } from "../format/btree-page.js";
import {
  BTreePage,
  BTreePageEditor,
  indexInteriorCell,
  maxCellSize,
  tableInteriorCell,
} from "../format/btree-page.js";
import type { Descent, Step } from "./cursor.js";
import { Cursor, descendToKey } from "./cursor.js";
import type { PageReader, PageWriter } from "./page-store.js";
import type { NodeContent } from "./split.js";
import { contentOf, cut, withChild } from "./split.js";
import type { Position, TreeKey, TreeKind } from "./types.js";

// ---------------------------------------------------------------------------
// B+trees over format 5 pages: tables keyed by rowid, indexes by encoded key.
//
// A tree is known by its root page, which never moves: when the root splits,
// its content moves to two new pages and the root becomes their parent.
// A node that outgrows its page splits where the halves are closest in
// bytes — except at the right edge of the tree, where it splits off just the
// new cell and leaves a full page behind, so ascending inserts fill pages.
// ---------------------------------------------------------------------------

const LEAF_TYPE: Record<TreeKind, BTreePageType> = {
  table: "table-leaf",
  index: "index-leaf",
};

const INTERIOR_TYPE: Record<TreeKind, BTreePageType> = {
  table: "table-interior",
  index: "index-interior",
};

const POINTER_SIZE = 2;

export class BTree {
  protected readonly pageAt = (pageNo: number): BTreePage =>
    new BTreePage(this.pages.read(pageNo));

  constructor(
    protected readonly pages: PageReader,
    readonly root: number,
    readonly kind: TreeKind,
  ) {}

  /** The leaf cell with `key`, or null. */
  find(key: TreeKey): Position | null {
    const { leaf, position } = this.descend(key);
    return leaf.holds(position, key) ? { page: leaf, index: position } : null;
  }

  /** Leaf cells in key order, from the first whose key is at least `from`. */
  *ascending(from?: TreeKey): Generator<Position> {
    const cursor =
      from === undefined
        ? Cursor.atEdge(this.pageAt, this.root, "first")
        : Cursor.atKey(this.pageAt, this.root, from);
    do {
      const { page } = cursor;
      for (let index = cursor.index; index < page.cellCount; index++) {
        yield { page, index };
      }
    } while (cursor.moveToLeaf("next"));
  }

  /** Leaf cells in reverse key order, from the last whose key is below `before`. */
  *descending(before?: TreeKey): Generator<Position> {
    const cursor =
      before === undefined
        ? Cursor.atEdge(this.pageAt, this.root, "last")
        : Cursor.beforeKey(this.pageAt, this.root, before);
    do {
      const { page } = cursor;
      for (let index = cursor.index; index >= 0; index--) {
        yield { page, index };
      }
    } while (cursor.moveToLeaf("previous"));
  }

  /** The cell with the largest key, or null if the tree is empty. */
  last(): Position | null {
    for (const position of this.descending()) return position;
    return null;
  }

  /** Largest leaf cell the tree accepts. */
  maxLeafCellSize(): number {
    return maxCellSize(LEAF_TYPE[this.kind], this.pages.pageSize);
  }

  /** Largest interior cell, which bounds the keys an index accepts. */
  maxInteriorCellSize(): number {
    return maxCellSize(INTERIOR_TYPE[this.kind], this.pages.pageSize);
  }

  protected descend(key: TreeKey): Descent {
    return descendToKey(this.pageAt, this.root, key);
  }
}

export class BTreeWriter extends BTree {
  constructor(
    private readonly writer: PageWriter,
    root: number,
    kind: TreeKind,
  ) {
    super(writer, root, kind);
  }

  /** Allocates the root page of a new, empty tree. */
  static create(writer: PageWriter, kind: TreeKind): number {
    const pageNo = writer.allocate();
    BTreePageEditor.initialize(writer.write(pageNo), LEAF_TYPE[kind]);
    return pageNo;
  }

  /** Adds `cell` under `key`. False, changing nothing, if the key is present. */
  insert(key: TreeKey, cell: Uint8Array): boolean {
    this.checkCellSize(cell);
    const descent = this.descend(key);
    if (descent.leaf.holds(descent.position, key)) return false;
    this.insertCell(descent, cell, isPastLastKey(descent));
    return true;
  }

  /** Replaces the cell under `key`. False, changing nothing, if there is none. */
  replace(key: TreeKey, cell: Uint8Array): boolean {
    this.checkCellSize(cell);
    const descent = this.descend(key);
    if (!descent.leaf.holds(descent.position, key)) return false;
    this.edit(descent.leafPageNo).removeCell(descent.position);
    this.insertCell(descent, cell, false);
    return true;
  }

  /** Removes the cell under `key`. False if there is none. */
  delete(key: TreeKey): boolean {
    const { leafPageNo, leaf, position } = this.descend(key);
    if (!leaf.holds(position, key)) return false;
    this.edit(leafPageNo).removeCell(position);
    return true;
  }

  /** Frees every page of the tree, the root included. */
  drop(): void {
    this.free(this.root);
  }

  private free(pageNo: number): void {
    const children = childrenOf(this.pageAt(pageNo));
    for (const child of children) this.free(child);
    this.writer.free(pageNo);
  }

  private edit(pageNo: number): BTreePageEditor {
    return new BTreePageEditor(this.writer.write(pageNo));
  }

  private checkCellSize(cell: Uint8Array): void {
    if (cell.length > this.maxLeafCellSize()) {
      throw new InternalError(
        `a ${String(cell.length)}-byte cell exceeds the B-tree limit`,
      );
    }
  }

  // --- Splits ------------------------------------------------------------------

  private insertCell(
    descent: Descent,
    cell: Uint8Array,
    isAtRightEdge: boolean,
  ): void {
    const { path, leafPageNo, position } = descent;
    const leaf = this.edit(leafPageNo);
    if (fits(cell, leaf)) {
      leaf.insertCell(position, cell);
      return;
    }
    const content = contentOf(leaf);
    content.cells.splice(position, 0, cell);
    this.split(path, leafPageNo, content, isAtRightEdge);
  }

  /** Writes `content`, too big for one page, as two, and carries the split up. */
  private split(
    path: Step[],
    pageNo: number,
    content: NodeContent,
    isAtRightEdge: boolean,
  ): void {
    const { left, separator, right } = cut(content, isAtRightEdge);
    const parent = path.pop();
    if (parent === undefined) {
      this.splitRoot(pageNo, left, separator, right);
      return;
    }
    this.edit(pageNo).rebuild(left.cells, left.rightChild);
    const halves = { left: pageNo, separator, right: this.allocateNode(right) };
    this.addSeparator(path, parent, halves, isAtRightEdge);
  }

  /**
   * Puts `separator` between the split page and its new right half in the
   * parent: a new cell points left, and the reference that pointed at the
   * split page — now just after that cell — points right.
   */
  private addSeparator(
    path: Step[],
    parent: Step,
    halves: { left: number; separator: TreeKey; right: number },
    isAtRightEdge: boolean,
  ): void {
    const editor = this.edit(parent.pageNo);
    const cell = this.interiorCell(halves.left, halves.separator);
    if (fits(cell, editor)) {
      editor.insertCell(parent.index, cell);
      editor.setChild(parent.index + 1, halves.right);
      return;
    }
    const content = contentOf(editor);
    content.cells.splice(parent.index, 0, cell);
    const updated = withChild(content, parent.index + 1, halves.right);
    this.split(path, parent.pageNo, updated, isAtRightEdge);
  }

  /** The root keeps its page: its halves move to new pages below it. */
  private splitRoot(
    rootPageNo: number,
    left: NodeContent,
    separator: TreeKey,
    right: NodeContent,
  ): void {
    const leftPageNo = this.allocateNode(left);
    const rightPageNo = this.allocateNode(right);
    const root = BTreePageEditor.initialize(
      this.writer.write(rootPageNo),
      INTERIOR_TYPE[this.kind],
      rightPageNo,
    );
    root.insertCell(0, this.interiorCell(leftPageNo, separator));
  }

  private allocateNode(content: NodeContent): number {
    const pageNo = this.writer.allocate();
    const { type, cells, rightChild } = content;
    BTreePageEditor.build(this.writer.write(pageNo), type, cells, rightChild);
    return pageNo;
  }

  private interiorCell(child: number, separator: TreeKey): Uint8Array {
    return typeof separator === "number"
      ? tableInteriorCell(child, separator)
      : indexInteriorCell(child, separator);
  }
}

/** True when the descent ends past the last key of the tree. */
function isPastLastKey({ path, leaf, position }: Descent): boolean {
  const isLastInLeaf = position === leaf.cellCount;
  return isLastInLeaf && path.every((step) => step.index === step.cellCount);
}

function fits(cell: Uint8Array, page: BTreePage): boolean {
  return cell.length + POINTER_SIZE <= page.freeSpace;
}

function childrenOf(page: BTreePage): number[] {
  if (page.isLeaf) return [];
  return Array.from({ length: page.cellCount + 1 }, (_, index) =>
    page.child(index),
  );
}
