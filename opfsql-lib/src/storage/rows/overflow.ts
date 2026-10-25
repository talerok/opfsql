import { CorruptError } from "../../errors.js";
import type { PageReader, PageWriter } from "../btree/page-store.js";
import { readUint32, writeUint32 } from "../format/bytes.js";

// ---------------------------------------------------------------------------
// Overflow chains (docs/file-format.md, Overflow values): a value's bytes
// across pages, each holding the next page's number and as many bytes as fit.
// A chain is known by its first page and the bytes it holds, so its length
// in pages is known before reading it, and a corrupt chain cannot loop.
// ---------------------------------------------------------------------------

export const OVERFLOW_PAGE_TYPE = 0x05;
const NEXT_OFFSET = 4;
const HEADER_SIZE = 8;

export interface ChainReference {
  readonly firstPage: number;
  /** Bytes the chain holds. */
  readonly length: number;
}

export interface ChainPage {
  readonly pageNo: number;
  readonly page: Uint8Array;
}

/** Writes `bytes` to a new chain and returns its first page. */
export function writeChain(pages: PageWriter, bytes: Uint8Array): number {
  const capacity = capacityOf(pages.pageSize);
  const count = pagesFor(bytes.length, pages.pageSize);
  const chain = Array.from({ length: count }, () => pages.allocate());
  for (const [index, pageNo] of chain.entries()) {
    const part = bytes.subarray(index * capacity, (index + 1) * capacity);
    const next = chain[index + 1] ?? 0;
    writeOverflowPage(pages.write(pageNo), next, part);
  }
  return chain[0] ?? 0;
}

export function readChain(
  pages: PageReader,
  chain: ChainReference,
): Uint8Array {
  const bytes = new Uint8Array(chain.length);
  let offset = 0;
  for (const { page } of chainPages(pages, chain)) {
    const rest = chain.length - offset;
    const part = page.subarray(HEADER_SIZE, HEADER_SIZE + rest);
    bytes.set(part, offset);
    offset += part.length;
  }
  return bytes;
}

export function freeChain(pages: PageWriter, chain: ChainReference): void {
  // Walks the whole chain first: a corrupt one throws before any page is freed.
  const pageNumbers = [...chainPages(pages, chain)].map((page) => page.pageNo);
  for (const pageNo of pageNumbers) pages.free(pageNo);
}

/** The pages of a chain in order: exactly as many as its bytes take. */
export function* chainPages(
  pages: PageReader,
  chain: ChainReference,
): Generator<ChainPage> {
  const count = pagesFor(chain.length, pages.pageSize);
  let pageNo = chain.firstPage;
  for (let index = 0; index < count; index++) {
    const page = readOverflowPage(pages, pageNo, chain);
    yield { pageNo, page };
    pageNo = readUint32(page, NEXT_OFFSET);
  }
  if (pageNo !== 0) throw lengthMismatch(chain);
}

/** Pages a chain of `length` bytes takes; even an empty value takes one. */
function pagesFor(length: number, pageSize: number): number {
  return Math.max(1, Math.ceil(length / capacityOf(pageSize)));
}

/** Bytes of the value one page holds. */
function capacityOf(pageSize: number): number {
  return pageSize - HEADER_SIZE;
}

function writeOverflowPage(
  page: Uint8Array,
  next: number,
  part: Uint8Array,
): void {
  page[0] = OVERFLOW_PAGE_TYPE;
  writeUint32(page, NEXT_OFFSET, next);
  page.set(part, HEADER_SIZE);
}

function readOverflowPage(
  pages: PageReader,
  pageNo: number,
  chain: ChainReference,
): Uint8Array {
  if (pageNo === 0) throw lengthMismatch(chain);
  const page = pages.read(pageNo);
  if (page[0] !== OVERFLOW_PAGE_TYPE) {
    throw new CorruptError(`page ${String(pageNo)} is not an overflow page`);
  }
  return page;
}

function lengthMismatch(chain: ChainReference): CorruptError {
  const { firstPage, length } = chain;
  return new CorruptError(
    `overflow chain at page ${String(firstPage)} does not hold its ${String(length)} bytes`,
  );
}
