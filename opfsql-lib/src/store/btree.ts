import { encodedSize } from "./page-codec.js";
import type { SyncIPageStore } from "./types.js";

// ---------------------------------------------------------------------------
// B+tree core shared by table and index trees.
//
// Leaves hold entries sorted by key; internal nodes hold separator keys and
// child pages. A tree is known by its root page, which never moves: when the
// root splits, both halves move to new pages and the root becomes their
// parent. Nodes have no sibling links — cursors keep the path from the root.
//
// Nodes are sized in bytes. Each records the encoded size of its items
// (`bytes`), kept exact as items come and go, and splits when its items and
// fixed overhead would outgrow the page. Entries and keys must therefore hold
// no objects (see encodedSize). Two limits keep every split possible:
//   - an entry takes at most half a leaf, so an overflowing leaf divides
//     into two that fit;
//   - a key takes at most a quarter of an internal node, so an overflowing
//     internal node has at least three keys, and both halves keep one.
// A change at the right edge of the tree — past its last key — splits off
// just the new item and leaves full nodes behind, so ascending inserts fill
// pages completely.
//
// Nodes come from a page store shared with snapshots and other sessions:
// they are never mutated, only replaced.
// ---------------------------------------------------------------------------

export interface LeafNode<E> {
  kind: "leaf";
  /** Sorted by key, keys unique. */
  entries: E[];
  /** Encoded size of the entries. */
  bytes: number;
}

export interface InternalNode<K> {
  kind: "internal";
  /** children[i] holds the keys from keys[i - 1] up to, not including, keys[i]. */
  keys: K[];
  children: number[];
  /** Encoded size of the keys and children. */
  bytes: number;
}

export type Node<K, E> = LeafNode<E> | InternalNode<K>;

/** What sets one kind of tree apart: its entries, their keys and its errors. */
export interface BTreeLayout<K, E> {
  keyOf(entry: E): K;
  compare(a: K, b: K): number;
  /** The error for an entry or key of `size` bytes, over the `limit`. */
  tooLarge(size: number, limit: number): Error;
}

const MAX_U32 = 0xffff_ffff;

/** A CBOR array header takes 1 byte when the array is empty, and at most 5. */
const ARRAY_HEADER_GROWTH = 4;

/** Encoded size of a node besides its items, at most. */
const LEAF_OVERHEAD = encodedSize(leafNode([], MAX_U32)) + ARRAY_HEADER_GROWTH;
const INTERNAL_OVERHEAD =
  encodedSize(internalNode([], [], MAX_U32)) + 2 * ARRAY_HEADER_GROWTH;

interface Space {
  /** Room for items in a leaf and in an internal node. */
  leaf: number;
  internal: number;
  /** Largest entry and largest key. */
  maxEntry: number;
  maxKey: number;
}

function spaceFor(pageCapacity: number): Space {
  const leaf = pageCapacity - LEAF_OVERHEAD;
  const internal = pageCapacity - INTERNAL_OVERHEAD;
  return {
    leaf,
    internal,
    maxEntry: Math.floor(leaf / 2),
    maxKey: Math.floor(internal / 4),
  };
}

/** A node on the way down from the root, and the position taken in it. */
interface Step<K, E> {
  pageNo: number;
  node: Node<K, E>;
  /** Child followed in an internal node; entry position in a leaf. */
  index: number;
}

interface LeafStep<E> extends Step<never, E> {
  node: LeafNode<E>;
}

type Path<K, E> = Step<K, E>[];

/** An overflowing node cut in two, and the key that separates the halves. */
interface Split<K, E> {
  left: Node<K, E>;
  separator: K;
  right: Node<K, E>;
}

export class BTree<K, E> {
  private readonly space: Space;

  constructor(
    private readonly pages: SyncIPageStore,
    private readonly rootPageNo: number,
    private readonly layout: BTreeLayout<K, E>,
  ) {
    this.space = spaceFor(pages.pageCapacity);
  }

  /** Allocates the root page of a new, empty tree. */
  static create(pages: SyncIPageStore): number {
    const pageNo = pages.allocPage();
    pages.writePage(pageNo, leafNode([], 0));
    return pageNo;
  }

  // --- Reads ----------------------------------------------------------------

  get(key: K): E | null {
    const leaf = this.leaf(this.descend(key));
    return this.holds(leaf, key) ? leaf.node.entries[leaf.index] : null;
  }

  /** Entries in key order, from the first whose key is at least `from`. */
  *ascending(from?: K): Generator<E> {
    const path =
      from === undefined
        ? this.descendToEdge([], this.rootPageNo, "first")
        : this.descend(from);
    do {
      const { node, index } = this.leaf(path);
      for (let i = index; i < node.entries.length; i++) yield node.entries[i];
    } while (this.moveToLeaf(path, "next"));
  }

  /** Entries in reverse key order, from the last whose key is below `before`. */
  *descending(before?: K): Generator<E> {
    let path: Path<K, E>;
    if (before === undefined) {
      path = this.descendToEdge([], this.rootPageNo, "last");
    } else {
      path = this.descend(before);
      this.leaf(path).index--; // descend() stops at the first entry not below
    }
    do {
      const { node, index } = this.leaf(path);
      for (let i = index; i >= 0; i--) yield node.entries[i];
    } while (this.moveToLeaf(path, "previous"));
  }

  /** The entry with the largest key, or null if the tree is empty. */
  last(): E | null {
    for (const entry of this.descending()) return entry;
    return null;
  }

  // --- Changes --------------------------------------------------------------

  /** Adds `entry`. False, changing nothing, if its key is already present. */
  insert(entry: E): boolean {
    const size = this.sizeOf(entry);
    const key = this.layout.keyOf(entry);
    const path = this.descend(key);
    const leaf = this.leaf(path);
    if (this.holds(leaf, key)) return false;

    const { node, index } = leaf;
    const entries = insertAt(node.entries, index, entry);
    this.put(path, leafNode(entries, node.bytes + size), atRightEdge(path));
    return true;
  }

  /** Replaces the entry with `entry`'s key. False, changing nothing, if there is none. */
  replace(entry: E): boolean {
    const size = this.sizeOf(entry);
    const key = this.layout.keyOf(entry);
    const path = this.descend(key);
    const leaf = this.leaf(path);
    if (!this.holds(leaf, key)) return false;

    const { node, index } = leaf;
    const bytes = node.bytes - encodedSize(node.entries[index]) + size;
    this.put(
      path,
      leafNode(replaceAt(node.entries, index, entry), bytes),
      false,
    );
    return true;
  }

  /** Removes the entry with `key`. False if there is none. */
  delete(key: K): boolean {
    const path = this.descend(key);
    const leaf = this.leaf(path);
    if (!this.holds(leaf, key)) return false;

    const { pageNo, node, index } = leaf;
    const bytes = node.bytes - encodedSize(node.entries[index]);
    this.pages.writePage(
      pageNo,
      leafNode(removeAt(node.entries, index), bytes),
    );
    return true;
  }

  /** Fills an empty tree with `entries`, sorted by key and unique. */
  build(entries: readonly E[]): void {
    if (entries.length === 0) return;
    const sizes = entries.map((entry) => this.sizeOf(entry));

    // Pack each level into full nodes, bottom up, until one is left: the root.
    let level: Array<{ firstKey: K; node: Node<K, E> }> = runs(
      sizes,
      this.space.leaf,
    ).map(([from, to]) => ({
      firstKey: this.layout.keyOf(entries[from]),
      node: leafNode(entries.slice(from, to), sum(sizes, from, to)),
    }));
    while (level.length > 1) {
      const children = level.map(({ node }) => this.allocate(node));
      const keys = level.map(({ firstKey }) => firstKey);
      const childSizes = children.map(encodedSize);
      const keySizes = keys.map(encodedSize);
      // A child comes with the key that separates it from the one before;
      // counting that key for a node's first child too errs on the safe side.
      const itemSizes = childSizes.map((size, i) => size + keySizes[i]);
      level = runs(itemSizes, this.space.internal, 2).map(([from, to]) => ({
        firstKey: keys[from],
        node: internalNode(
          keys.slice(from + 1, to),
          children.slice(from, to),
          sum(keySizes, from + 1, to) + sum(childSizes, from, to),
        ),
      }));
    }
    this.pages.writePage(this.rootPageNo, level[0].node);
  }

  /** Frees every page of the tree, the root included. */
  drop(): void {
    this.free(this.rootPageNo, this.height());
  }

  // --- Writing nodes --------------------------------------------------------

  /**
   * Writes `node` in place of the last node of `path`, splitting it if it
   * outgrew its page and carrying the split up to the parent.
   */
  private put(path: Path<K, E>, node: Node<K, E>, rightEdge: boolean): void {
    const step = path.pop()!;
    if (node.bytes <= this.roomIn(node)) {
      this.pages.writePage(step.pageNo, node);
      return;
    }

    const { left, separator, right } =
      node.kind === "leaf"
        ? this.splitLeaf(node, rightEdge)
        : this.splitInternal(node, rightEdge);
    const parent = path.at(-1);
    if (!parent) {
      // The root keeps its page; both halves move to new ones.
      const children = [this.allocate(left), this.allocate(right)];
      const bytes =
        encodedSize(separator) +
        encodedSize(children[0]) +
        encodedSize(children[1]);
      this.pages.writePage(
        step.pageNo,
        internalNode([separator], children, bytes),
      );
      return;
    }

    this.pages.writePage(step.pageNo, left);
    const rightPageNo = this.allocate(right);
    const parentNode = parent.node as InternalNode<K>;
    const { index } = parent;
    const bytes =
      parentNode.bytes + encodedSize(separator) + encodedSize(rightPageNo);
    // On the right edge the separator lands at the end of the parent too.
    this.put(
      path,
      internalNode(
        insertAt(parentNode.keys, index, separator),
        insertAt(parentNode.children, index + 1, rightPageNo),
        bytes,
      ),
      rightEdge,
    );
  }

  private splitLeaf(node: LeafNode<E>, rightEdge: boolean): Split<K, E> {
    const sizes = node.entries.map(encodedSize);
    const at = rightEdge ? sizes.length - 1 : balancedCut(sizes);
    return {
      left: leafNode(node.entries.slice(0, at), sum(sizes, 0, at)),
      separator: this.layout.keyOf(node.entries[at]),
      right: leafNode(node.entries.slice(at), sum(sizes, at, sizes.length)),
    };
  }

  /** Splits around the key at `at`, which moves up to the parent. */
  private splitInternal(
    node: InternalNode<K>,
    rightEdge: boolean,
  ): Split<K, E> {
    const keySizes = node.keys.map(encodedSize);
    const childSizes = node.children.map(encodedSize);
    const at = rightEdge
      ? keySizes.length - 2
      : balancedPromotion(keySizes, childSizes);
    return {
      left: internalNode(
        node.keys.slice(0, at),
        node.children.slice(0, at + 1),
        sum(keySizes, 0, at) + sum(childSizes, 0, at + 1),
      ),
      separator: node.keys[at],
      right: internalNode(
        node.keys.slice(at + 1),
        node.children.slice(at + 1),
        sum(keySizes, at + 1, keySizes.length) +
          sum(childSizes, at + 1, childSizes.length),
      ),
    };
  }

  private allocate(node: Node<K, E>): number {
    const pageNo = this.pages.allocPage();
    this.pages.writePage(pageNo, node);
    return pageNo;
  }

  private roomIn(node: Node<K, E>): number {
    return node.kind === "leaf" ? this.space.leaf : this.space.internal;
  }

  /** Encoded size of `entry`, once it and its key are checked against the limits. */
  private sizeOf(entry: E): number {
    const keySize = encodedSize(this.layout.keyOf(entry));
    if (keySize > this.space.maxKey) {
      throw this.layout.tooLarge(keySize, this.space.maxKey);
    }
    const size = encodedSize(entry);
    if (size > this.space.maxEntry) {
      throw this.layout.tooLarge(size, this.space.maxEntry);
    }
    return size;
  }

  // --- Navigation -----------------------------------------------------------

  /** Path to where `key` is, or would be. */
  private descend(key: K): Path<K, E> {
    const path: Path<K, E> = [];
    let pageNo = this.rootPageNo;
    for (;;) {
      const node = this.read(pageNo);
      if (node.kind === "leaf") {
        path.push({ pageNo, node, index: this.entriesBelow(node, key) });
        return path;
      }
      const index = this.childFor(node, key);
      path.push({ pageNo, node, index });
      pageNo = node.children[index];
    }
  }

  /** Extends `path` from `pageNo` down to the first or last entry beneath it. */
  private descendToEdge(
    path: Path<K, E>,
    pageNo: number,
    edge: "first" | "last",
  ): Path<K, E> {
    for (;;) {
      const node = this.read(pageNo);
      if (node.kind === "leaf") {
        const index = edge === "first" ? 0 : node.entries.length - 1;
        path.push({ pageNo, node, index });
        return path;
      }
      const index = edge === "first" ? 0 : node.children.length - 1;
      path.push({ pageNo, node, index });
      pageNo = node.children[index];
    }
  }

  /** Moves `path` to the edge of the adjacent leaf. False if there is none. */
  private moveToLeaf(
    path: Path<K, E>,
    direction: "next" | "previous",
  ): boolean {
    path.pop();
    for (let parent = path.at(-1); parent; parent = path.at(-1)) {
      const { children } = parent.node as InternalNode<K>;
      const index = parent.index + (direction === "next" ? 1 : -1);
      if (index >= 0 && index < children.length) {
        parent.index = index;
        const edge = direction === "next" ? "first" : "last";
        this.descendToEdge(path, children[index], edge);
        return true;
      }
      path.pop();
    }
    return false;
  }

  private leaf(path: Path<K, E>): LeafStep<E> {
    return path.at(-1) as LeafStep<E>;
  }

  /** True if the leaf position of `leaf` holds `key`. */
  private holds({ node, index }: LeafStep<E>, key: K): boolean {
    return (
      index < node.entries.length &&
      this.layout.compare(this.layout.keyOf(node.entries[index]), key) === 0
    );
  }

  /** Number of entries in `leaf` with keys below `key`. */
  private entriesBelow(leaf: LeafNode<E>, key: K): number {
    let lo = 0;
    let hi = leaf.entries.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      const cmp = this.layout.compare(
        this.layout.keyOf(leaf.entries[mid]),
        key,
      );
      if (cmp < 0) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  /** The child of `node` whose range holds `key`. */
  private childFor(node: InternalNode<K>, key: K): number {
    let lo = 0;
    let hi = node.keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.layout.compare(key, node.keys[mid]) < 0) hi = mid;
      else lo = mid + 1;
    }
    return lo;
  }

  /** Number of levels; every leaf is at the same depth. */
  private height(): number {
    let levels = 1;
    for (
      let node = this.read(this.rootPageNo);
      node.kind === "internal";
      node = this.read(node.children[0])
    ) {
      levels++;
    }
    return levels;
  }

  private free(pageNo: number, levels: number): void {
    if (levels > 1) {
      const node = this.read(pageNo) as InternalNode<K>;
      for (const child of node.children) this.free(child, levels - 1);
    }
    this.pages.freePage(pageNo);
  }

  private read(pageNo: number): Node<K, E> {
    const node = this.pages.readPage<Node<K, E>>(pageNo);
    if (!node) throw new Error(`B-tree node at page ${pageNo} not found`);
    return node;
  }
}

// --- Nodes ------------------------------------------------------------------

function leafNode<E>(entries: E[], bytes: number): LeafNode<E> {
  return { kind: "leaf", entries, bytes };
}

function internalNode<K>(
  keys: K[],
  children: number[],
  bytes: number,
): InternalNode<K> {
  return { kind: "internal", keys, children, bytes };
}

/** True if `path` ends past the last entry of the tree. */
function atRightEdge<K, E>(path: Path<K, E>): boolean {
  return path.every(({ node, index }) =>
    node.kind === "leaf"
      ? index === node.entries.length
      : index === node.children.length - 1,
  );
}

// --- Sizes ------------------------------------------------------------------

function sum(sizes: readonly number[], from: number, to: number): number {
  let total = 0;
  for (let i = from; i < to; i++) total += sizes[i];
  return total;
}

/** Where to cut items so that the larger part is smallest; both keep one. */
function balancedCut(sizes: readonly number[]): number {
  const total = sum(sizes, 0, sizes.length);
  let best = 1;
  let bestLarger = Infinity;
  let left = 0;
  for (let at = 1; at < sizes.length; at++) {
    left += sizes[at - 1];
    const larger = Math.max(left, total - left);
    if (larger < bestLarger) {
      best = at;
      bestLarger = larger;
    }
  }
  return best;
}

/**
 * Which key of an internal node to move up so that the larger half is
 * smallest; both halves keep at least one key.
 */
function balancedPromotion(
  keySizes: readonly number[],
  childSizes: readonly number[],
): number {
  const total =
    sum(keySizes, 0, keySizes.length) + sum(childSizes, 0, childSizes.length);
  let best = 1;
  let bestLarger = Infinity;
  let left = childSizes[0];
  for (let at = 1; at < keySizes.length - 1; at++) {
    left += keySizes[at - 1] + childSizes[at];
    const larger = Math.max(left, total - left - keySizes[at]);
    if (larger < bestLarger) {
      best = at;
      bestLarger = larger;
    }
  }
  return best;
}

/**
 * Cuts items into consecutive runs, each as long as fits `room`; the last
 * run borrows from the one before to have at least `minLast` items.
 */
function runs(
  sizes: readonly number[],
  room: number,
  minLast = 1,
): Array<[from: number, to: number]> {
  const result: Array<[number, number]> = [];
  let from = 0;
  let used = 0;
  sizes.forEach((size, i) => {
    if (i > from && used + size > room) {
      result.push([from, i]);
      from = i;
      used = 0;
    }
    used += size;
  });
  result.push([from, sizes.length]);

  const last = result[result.length - 1];
  if (result.length > 1 && last[1] - last[0] < minLast) {
    result[result.length - 2][1]--;
    last[0]--;
  }
  return result;
}

// --- Arrays -----------------------------------------------------------------

function insertAt<T>(items: readonly T[], index: number, item: T): T[] {
  return [...items.slice(0, index), item, ...items.slice(index)];
}

function replaceAt<T>(items: readonly T[], index: number, item: T): T[] {
  return items.map((existing, i) => (i === index ? item : existing));
}

function removeAt<T>(items: readonly T[], index: number): T[] {
  return [...items.slice(0, index), ...items.slice(index + 1)];
}
