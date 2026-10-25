import { Encoder, decode } from "cbor-x";
import { PageOverflowError } from "./errors.js";

// Byte strings are never tagged. By default cbor-x tags a Uint8Array but not
// a Buffer, and only under Node, so the same row would encode to different
// sizes depending on where its bytes came from.
const encoder = new Encoder({
  structuredClone: false,
  useRecords: true,
  tagUint8Array: false,
});

// ---------------------------------------------------------------------------
// EncodedPage — a page value paired with its CBOR bytes.
//
// Dirty pages are encoded exactly once, when a transaction commits
// (Storage.commitPages). Every layer below reuses the same bytes — the WAL
// frames them as-is and checkpoint copies them to the main file without
// re-encoding. Each backend rejects the whole batch if any page exceeds its
// capacity, before anything is written.
// ---------------------------------------------------------------------------

export interface EncodedPage {
  readonly value: unknown;
  readonly bytes: Uint8Array;
}

export function encodePage(value: unknown): EncodedPage {
  return { value, bytes: encodeCbor(value) };
}

export function decodePage<T>(bytes: Uint8Array): T {
  return decodeCbor<T>(bytes);
}

/** The one CBOR configuration for everything stored: pages and row cells. */
export function encodeCbor(value: unknown): Uint8Array {
  return encoder.encode(value);
}

export function decodeCbor<T>(bytes: Uint8Array): T {
  return decode(bytes) as T;
}

/**
 * Encoded size of `value`. A CBOR array is its header followed by its items'
 * encodings, so an array's size is its header plus its items' sizes — as
 * long as the items hold no objects, whose record definitions one encoding
 * shares between them.
 */
export function encodedSize(value: unknown): number {
  return encoder.encode(value).length;
}

/** Throws PageOverflowError for the first page larger than `capacity`. */
export function assertPagesFit(
  pages: ReadonlyMap<number, EncodedPage>,
  capacity: number,
): void {
  for (const [pageNo, { bytes }] of pages) {
    if (bytes.length > capacity) {
      throw new PageOverflowError(pageNo, bytes.length, capacity);
    }
  }
}
