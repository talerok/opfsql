import { InternalError } from "../../errors.js";
import type { BTreePageType } from "../format/btree-page.js";
import {
  BTreePage,
  BTreePageEditor,
  indexInteriorCell,
  maxCellSize,
  tableInteriorCell,
} from "../format/btree-page.js";
import type { PageReader, PageWriter } from "./page-store.js";
import type { NodeContent } from "./split.js";
import type { Position, TreeKey, TreeKind } from "./types.js";
import { contentOf, cut, withChild } from "./split.js";

// ---------------------------------------------------------------------------
// B+trees over format 5 pages: tables keyed by rowid, indexes by encoded key.
//
// A tree is known by its root page, which never moves: when the root splits,
// its content moves to two new pages and the root becomes their parent.
// Pages have no sibling links; cursors keep the path from the root. A node
// that outgrows its page splits where the halves are closest in bytes —
// except at the right edge of the tree, where it splits off just the new
// cell and leaves a full page behind, so ascending inserts fill pages.
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

/** An interior page on the way down, and the child taken in it. */
interface Step {
  readonly pageNo: number;
  index: number;
  readonly cellCount: number;
}

/** A cell to put into a leaf, and where. */
interface Insertion {
  readonly path: Step[];
  readonly pageNo: number;
  readonly position: number;
  readonly cell: Uint8Array;
  readonly isAtRightEdge: boolean;
}

export class BTree {
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
    const path: Step[] = [];
    let page =
      from === undefined
        ? this.descendToEdge(path, this.root, "first")
        : this.descendInto(path, from);
    let index = from === undefined ? 0 : page.position(from);
    for (;;) {
      for (; index < page.cellCount; index++) yield { page, index };
      const next = this.adjacentLeaf(path, "next");
      if (next === null) return;
      page = next;
      index = 0;
    }
  }

  /** Leaf cells in reverse key order, from the last whose key is below `before`. */
  *descending(before?: TreeKey): Generator<Position> {
    const path: Step[] = [];
    let page =
      before === undefined
        ? this.descendToEdge(path, this.root, "last")
        : this.descendInto(path, before);
    let index =
      (before === undefined ? page.cellCount : page.position(before)) - 1;
    for (;;) {
      for (; index >= 0; index--) yield { page, index };
      const previous = this.adjacentLeaf(path, "previous");
      if (previous === null) return;
      page = previous;
      index = page.cellCount - 1;
    }
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

  protected page(pageNo: number): BTreePage {
    return new BTreePage(this.pages.read(pageNo));
  }

  /** Walks down to the leaf where `key` is or would be. */
  protected descend(key: TreeKey): {
    path: Step[];
    leafPageNo: number;
    leaf: BTreePage;
    position: number;
  } {
    const path: Step[] = [];
    let pageNo = this.root;
    let page = this.page(pageNo);
    while (!page.isLeaf) {
      const index = page.childFor(key);
      path.push({ pageNo, index, cellCount: page.cellCount });
      pageNo = page.child(index);
      page = this.page(pageNo);
    }
    return {
      path,
      leafPageNo: pageNo,
      leaf: page,
      position: page.position(key),
    };
  }

  private descendInto(path: Step[], key: TreeKey): BTreePage {
    const descent = this.descend(key);
    path.push(...descent.path);
    return descent.leaf;
  }

  /** Extends `path` from `pageNo` to its first or last leaf, and returns that leaf. */
  private descendToEdge(
    path: Step[],
    pageNo: number,
    edge: "first" | "last",
  ): BTreePage {
    let current = pageNo;
    let page = this.page(current);
    while (!page.isLeaf) {
      const index = edge === "first" ? 0 : page.cellCount;
      path.push({ pageNo: current, index, cellCount: page.cellCount });
      current = page.child(index);
      page = this.page(current);
    }
    return page;
  }

  /** Moves `path` to the next or previous leaf and returns it, or null at the end. */
  private adjacentLeaf(
    path: Step[],
    direction: "next" | "previous",
  ): BTreePage | null {
    for (let step = path.at(-1); step !== undefined; step = path.at(-1)) {
      const index = step.index + (direction === "next" ? 1 : -1);
      if (index >= 0 && index <= step.cellCount) {
        step.index = index;
        const child = this.page(step.pageNo).child(index);
        return this.descendToEdge(
          path,
          child,
          direction === "next" ? "first" : "last",
        );
      }
      path.pop();
    }
    return null;
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
    const { path, leafPageNo, leaf, position } = this.descend(key);
    if (leaf.holds(position, key)) return false;
    const isAtRightEdge =
      position === leaf.cellCount &&
      path.every((step) => step.index === step.cellCount);
    this.insertCell({
      path,
      pageNo: leafPageNo,
      position,
      cell,
      isAtRightEdge,
    });
    return true;
  }

  /** Replaces the cell under `key`. False, changing nothing, if there is none. */
  replace(key: TreeKey, cell: Uint8Array): boolean {
    this.checkCellSize(cell);
    const { path, leafPageNo, leaf, position } = this.descend(key);
    if (!leaf.holds(position, key)) return false;
    this.edit(leafPageNo).removeCell(position);
    this.insertCell({
      path,
      pageNo: leafPageNo,
      position,
      cell,
      isAtRightEdge: false,
    });
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
    const page = this.page(pageNo);
    if (!page.isLeaf) {
      for (let index = 0; index <= page.cellCount; index++) {
        this.free(page.child(index));
      }
    }
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

  private insertCell(insertion: Insertion): void {
    const { path, pageNo, position, cell, isAtRightEdge } = insertion;
    const editor = this.edit(pageNo);
    if (cell.length + POINTER_SIZE <= editor.freeSpace) {
      editor.insertCell(position, cell);
      return;
    }
    const content = contentOf(editor);
    content.cells.splice(position, 0, cell);
    this.split(path, pageNo, content, isAtRightEdge);
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
    const rightPageNo = this.allocateNode(right);
    this.addSeparator(
      path,
      parent,
      { left: pageNo, separator, right: rightPageNo },
      isAtRightEdge,
    );
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
    if (cell.length + POINTER_SIZE <= editor.freeSpace) {
      editor.insertCell(parent.index, cell);
      editor.setChild(parent.index + 1, halves.right);
      return;
    }
    const content = contentOf(editor);
    content.cells.splice(parent.index, 0, cell);
    this.split(
      path,
      parent.pageNo,
      withChild(content, parent.index + 1, halves.right),
      isAtRightEdge,
    );
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
    BTreePageEditor.build(
      this.writer.write(pageNo),
      content.type,
      content.cells,
      content.rightChild,
    );
    return pageNo;
  }

  private interiorCell(child: number, separator: TreeKey): Uint8Array {
    return typeof separator === "number"
      ? tableInteriorCell(child, separator)
      : indexInteriorCell(child, separator);
  }
}
