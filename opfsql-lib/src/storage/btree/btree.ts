import { InternalError } from "../../errors.js";
import {
  BTreePage,
  BTreePageEditor,
  indexInteriorCell,
  maxCellSize,
  tableInteriorCell,
} from "../format/btree-page.js";
import type { Descent, Step } from "./cursor.js";
import { TreeGuard } from "./guard.js";
import { Cursor, descendToKey } from "./cursor.js";
import type { PageReader, PageWriter } from "./page-store.js";
import type { NodeContent, SplitStyle } from "./split.js";
import { contentOf, cut, withCell, withChild } from "./split.js";
import type { LeafStart, Position, TreeKey, TreeKind } from "./types.js";
import { INTERIOR_TYPE, LEAF_TYPE } from "./types.js";

// ---------------------------------------------------------------------------
// B+trees over format 5 pages: tables keyed by rowid, indexes by encoded key.
//
// A tree is known by its root page, which never moves: when the root splits,
// its content moves to two new pages and the root becomes their parent.
// ---------------------------------------------------------------------------

/** The two pages a node split into, and the key between them. */
interface Halves {
  readonly left: number;
  readonly separator: TreeKey;
  readonly right: number;
}

export class BTreeReader {
  protected readonly pageAt = (pageNo: number): BTreePage =>
    new BTreePage(this.pages.read(pageNo));

  constructor(
    protected readonly pages: PageReader,
    readonly root: number,
    readonly kind: TreeKind,
  ) {}

  /** The leaf cell with `key`, or null. */
  find(key: TreeKey): Position | null {
    const { leaf, index } = this.descend(key);
    return leaf.hasKeyAt(index, key) ? { page: leaf, index } : null;
  }

  /** A guard against reading on after this tree changes. */
  guard(): TreeGuard {
    return new TreeGuard(this.pages, this.root);
  }

  /** Leaf cells in key order, from the first whose key is at least `from`. */
  *ascending(from?: TreeKey): Generator<Position> {
    const guard = this.guard();
    for (const { leaf, start } of this.leavesFrom(from)) {
      for (const position of cellsUpFrom(leaf, start)) {
        yield position;
        guard.check();
      }
    }
  }

  /** Leaf cells in reverse key order, from the last whose key is below `before`. */
  *descending(before?: TreeKey): Generator<Position> {
    const guard = this.guard();
    for (const { leaf, start } of this.leavesBefore(before)) {
      for (const position of cellsDownFrom(leaf, start)) {
        yield position;
        guard.check();
      }
    }
  }

  /**
   * The leaves in key order, each with the cell to start at: the first whose
   * key is at least `from` in the first leaf, the first cell in the others.
   */
  *leavesFrom(from?: TreeKey): Generator<LeafStart> {
    const cursor = this.cursorFrom(from);
    do {
      yield { leaf: cursor.page, start: cursor.index };
    } while (cursor.moveToLeaf("next"));
  }

  /**
   * The leaves in reverse key order, each with the cell to start at: the
   * last whose key is below `before` in the first leaf, the last in the
   * others.
   */
  *leavesBefore(before?: TreeKey): Generator<LeafStart> {
    const cursor = this.cursorBefore(before);
    do {
      yield { leaf: cursor.page, start: cursor.index };
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

  protected descend(key: TreeKey): Descent {
    return descendToKey(this.pageAt, this.root, key);
  }

  private cursorFrom(from: TreeKey | undefined): Cursor {
    if (from === undefined)
      return Cursor.atEdge(this.pageAt, this.root, "first");
    return Cursor.atKey(this.pageAt, this.root, from);
  }

  private cursorBefore(before: TreeKey | undefined): Cursor {
    if (before === undefined)
      return Cursor.atEdge(this.pageAt, this.root, "last");
    return Cursor.beforeKey(this.pageAt, this.root, before);
  }
}

export class BTreeWriter extends BTreeReader {
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
    if (descent.leaf.hasKeyAt(descent.index, key)) return false;
    this.writer.noteChange(this.root);
    this.insertCell(descent, cell);
    return true;
  }

  /** Replaces the cell under `key`. False, changing nothing, if there is none. */
  replace(key: TreeKey, cell: Uint8Array): boolean {
    this.checkCellSize(cell);
    const descent = this.descend(key);
    if (!descent.leaf.hasKeyAt(descent.index, key)) return false;
    this.writer.noteChange(this.root);
    this.edit(descent.leafPageNo).removeCell(descent.index);
    this.insertCell(descent, cell);
    return true;
  }

  /** Removes the cell under `key`, freeing a leaf it empties. False if there is none. */
  delete(key: TreeKey): boolean {
    const { path, leafPageNo, leaf, index } = this.descend(key);
    if (!leaf.hasKeyAt(index, key)) return false;
    this.writer.noteChange(this.root);
    const editor = this.edit(leafPageNo);
    editor.removeCell(index);
    if (editor.cellCount === 0) this.removeEmpty(path, leafPageNo);
    return true;
  }

  /** Frees every page of the tree, the root included. */
  drop(): void {
    this.writer.noteChange(this.root);
    this.freeSubtree(this.root);
  }

  private freeSubtree(pageNo: number): void {
    const children = childrenOf(this.pageAt(pageNo));
    for (const child of children) this.freeSubtree(child);
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

  // --- Emptied pages -----------------------------------------------------------

  /**
   * Takes the emptied subtree at `pageNo` out of the tree and frees its
   * pages, as far as that keeps every interior page with a separator and
   * every leaf at one depth. A parent with other separators lets it go; the
   * root takes the place of the one child it has left; any other parent
   * with one separator keeps it until its other child is empty too, and then
   * goes itself.
   */
  private removeEmpty(path: readonly Step[], pageNo: number): void {
    const parent = path.at(-1);
    if (parent === undefined) return;
    if (parent.cellCount > 1) {
      this.detach(parent, pageNo);
      return;
    }
    const sibling = this.pageAt(parent.pageNo).child(1 - parent.index);
    if (path.length === 1) {
      this.collapseRoot(parent.pageNo, sibling, pageNo);
      return;
    }
    if (this.isEmpty(sibling)) {
      this.removeEmpty(path.slice(0, -1), parent.pageNo);
    }
  }

  /** Takes child `parent.index` out of a parent with more than one separator. */
  private detach(parent: Step, child: number): void {
    const editor = this.edit(parent.pageNo);
    const isRightmost = parent.index === parent.cellCount;
    if (isRightmost) {
      // The child left of the last separator now holds every key above it.
      const last = parent.cellCount - 1;
      editor.setRightChild(editor.child(last));
      editor.removeCell(last);
    } else {
      // The next child now holds the emptied child's keys too.
      editor.removeCell(parent.index);
    }
    this.freeSubtree(child);
  }

  /**
   * The root's other child moves into the root, a level up, as every leaf
   * does; the root becomes an empty leaf if that child is empty too.
   */
  private collapseRoot(root: number, remaining: number, emptied: number): void {
    this.freeSubtree(emptied);
    if (this.isEmpty(remaining)) {
      this.freeSubtree(remaining);
      BTreePageEditor.initialize(this.writer.write(root), LEAF_TYPE[this.kind]);
      return;
    }
    this.writer.write(root).set(this.pages.read(remaining));
    // One page, not its subtree: its children now belong to the root.
    this.writer.free(remaining);
  }

  /** True if no leaf under `pageNo` holds a cell. */
  private isEmpty(pageNo: number): boolean {
    const page = this.pageAt(pageNo);
    if (page.isLeaf) return page.cellCount === 0;
    return childrenOf(page).every((child) => this.isEmpty(child));
  }

  // --- Splits ------------------------------------------------------------------

  /** Puts `cell` at the descent's place in its leaf, splitting it if it is full. */
  private insertCell(descent: Descent, cell: Uint8Array): void {
    const { path, leafPageNo, index } = descent;
    const leaf = this.edit(leafPageNo);
    if (leaf.fits(cell)) {
      leaf.insertCell(index, cell);
      return;
    }
    const content = withCell(contentOf(leaf), index, cell);
    this.split(path, leafPageNo, content, splitStyleAt(path, leaf, index));
  }

  /**
   * Writes `content`, too big for one page, as two, and adds the key between
   * them to the parent: the last step of `path`, the way down to `pageNo`.
   */
  private split(
    path: readonly Step[],
    pageNo: number,
    content: NodeContent,
    style: SplitStyle,
  ): void {
    const { left, separator, right } = cut(content, style);
    const parent = path.at(-1);
    if (parent === undefined) {
      this.splitRoot(pageNo, left, separator, right);
      return;
    }
    this.edit(pageNo).rebuild(left.cells, left.rightChild);
    const halves = { left: pageNo, separator, right: this.allocateNode(right) };
    this.addSeparator(path.slice(0, -1), parent, halves, style);
  }

  /**
   * Puts the separator between the split page and its new right half in the
   * parent: a new cell points left, and the reference that pointed at the
   * split page — now just after that cell — points right.
   */
  private addSeparator(
    path: readonly Step[],
    parent: Step,
    halves: Halves,
    style: SplitStyle,
  ): void {
    const editor = this.edit(parent.pageNo);
    const cell = this.interiorCell(halves.left, halves.separator);
    const rightIndex = parent.index + 1;
    if (editor.fits(cell)) {
      editor.insertCell(parent.index, cell);
      editor.setChild(rightIndex, halves.right);
      return;
    }
    const withSeparator = withCell(contentOf(editor), parent.index, cell);
    const content = withChild(withSeparator, rightIndex, halves.right);
    this.split(path, parent.pageNo, content, style);
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
    if (typeof separator === "number") {
      return tableInteriorCell(child, separator);
    }
    return indexInteriorCell(child, separator);
  }
}

function* cellsUpFrom(page: BTreePage, first: number): Generator<Position> {
  for (let index = first; index < page.cellCount; index++) {
    yield { page, index };
  }
}

function* cellsDownFrom(page: BTreePage, first: number): Generator<Position> {
  for (let index = first; index >= 0; index--) yield { page, index };
}

/**
 * A cell going past the last key of the tree splits off alone, so ascending
 * inserts leave full pages behind; any other splits in the middle.
 */
function splitStyleAt(
  path: readonly Step[],
  leaf: BTreePage,
  index: number,
): SplitStyle {
  const isLastInLeaf = index === leaf.cellCount;
  const isRightmostLeaf = path.every((step) => step.index === step.cellCount);
  return isLastInLeaf && isRightmostLeaf ? "right-edge" : "balanced";
}

function childrenOf(page: BTreePage): number[] {
  if (page.isLeaf) return [];
  return Array.from({ length: page.cellCount + 1 }, (_, index) =>
    page.child(index),
  );
}
