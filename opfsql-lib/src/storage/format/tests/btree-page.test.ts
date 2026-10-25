import { describe, expect, it } from "vitest";
import { mulberry32 } from "../../../../test/support/random.js";
import { CorruptError } from "../../../errors.js";
import { compareBytes } from "../../../values/index.js";
import type { BTreePageType } from "../btree-page.js";
import {
  BTreePage,
  BTreePageEditor,
  cellFootprint,
  headerSize,
  indexInteriorCell,
  indexLeafCell,
  maxCellSize,
  tableInteriorCell,
  tableLeafCell,
} from "../btree-page.js";

const PAGE_SIZE = 4096;

function page(type: BTreePageType, rightChild = 0): BTreePageEditor {
  return BTreePageEditor.initialize(
    new Uint8Array(PAGE_SIZE),
    type,
    rightChild,
  );
}

function record(size: number, fill: number): Uint8Array {
  return new Uint8Array(size).fill(fill);
}

interface ModelRow {
  readonly rowid: number;
  readonly record: Uint8Array;
}

/** Removes the row if the leaf has it, else inserts it if it fits; the model follows. */
function toggleRow(
  leaf: BTreePageEditor,
  model: ModelRow[],
  row: ModelRow,
): void {
  const index = leaf.lowerBound(row.rowid);
  if (leaf.holds(index, row.rowid)) {
    leaf.removeCell(index);
    model.splice(index, 1);
    return;
  }
  const cell = tableLeafCell(row.rowid, row.record);
  if (!leaf.fits(cell)) return;
  leaf.insertCell(index, cell);
  model.splice(index, 0, row);
}

describe("B-tree pages", () => {
  it("read back table leaf cells by position", () => {
    const leaf = page("table-leaf");
    leaf.insertCell(0, tableLeafCell(10, record(3, 1)));
    leaf.insertCell(0, tableLeafCell(-5, record(0, 0)));
    leaf.insertCell(2, tableLeafCell(2 ** 40, record(300, 7)));

    expect([0, 1, 2].map((index) => leaf.rowid(index))).toEqual([
      -5,
      10,
      2 ** 40,
    ]);
    expect(leaf.record(1)).toEqual(record(3, 1));
    expect(leaf.record(2)).toEqual(record(300, 7));
    expect(leaf.lowerBound(10)).toBe(1);
    expect(leaf.lowerBound(11)).toBe(2);
    expect(leaf.holds(1, 10)).toBe(true);
    expect(leaf.layoutProblems()).toEqual([]);
  });

  it("route keys to children in interior pages", () => {
    const interior = page("table-interior", 99);
    interior.insertCell(0, tableInteriorCell(7, 100));
    interior.insertCell(1, tableInteriorCell(8, 200));

    expect(
      [50, 100, 150, 200, 250].map((rowid) =>
        interior.child(interior.childIndexFor(rowid)),
      ),
    ).toEqual([7, 8, 8, 99, 99]);
    interior.setChild(2, 42);
    expect(interior.rightChild).toBe(42);
  });

  it("compare index keys as bytes", () => {
    const leaf = page("index-leaf");
    const keys = [[3], [1, 2], [1], [2, 0]].map((bytes) =>
      Uint8Array.from(bytes),
    );
    for (const key of keys.toSorted(compareBytes)) {
      leaf.insertCell(leaf.lowerBound(key), indexLeafCell(key));
    }
    expect(
      Array.from({ length: 4 }, (_, index) => [...leaf.key(index)]),
    ).toEqual([[1], [1, 2], [2, 0], [3]]);
    const interior = page("index-interior", 5);
    interior.insertCell(0, indexInteriorCell(4, Uint8Array.from([2])));
    expect(
      interior.child(interior.childIndexFor(Uint8Array.from([1, 9]))),
    ).toBe(4);
    expect(interior.child(interior.childIndexFor(Uint8Array.from([2])))).toBe(
      5,
    );
  });

  it.each([1, 2, 3])(
    "stay packed and zeroed through random inserts and removals (seed %i)",
    (seed) => {
      const random = mulberry32(seed);
      const leaf = page("table-leaf");
      const model: ModelRow[] = [];
      for (let step = 0; step < 2000; step++) {
        const rowid = Math.floor(random() * 500);
        const bytes = record(Math.floor(random() * 60), rowid % 256);
        toggleRow(leaf, model, { rowid, record: bytes });
        expect(leaf.cellCount).toBe(model.length);
      }
      expect(leaf.layoutProblems()).toEqual([]);
      for (const [index, entry] of model.entries()) {
        expect(leaf.rowid(index)).toBe(entry.rowid);
        expect(leaf.record(index)).toEqual(entry.record);
      }
      const used = model.reduce(
        (total, _, index) => total + cellFootprint(leaf.cell(index)),
        0,
      );
      expect(leaf.freeSpace).toBe(PAGE_SIZE - headerSize("table-leaf") - used);
    },
  );

  it("rebuild from a list of cells", () => {
    const leaf = page("index-leaf");
    leaf.insertCell(0, indexLeafCell(Uint8Array.from([9])));
    leaf.rebuild([
      indexLeafCell(Uint8Array.from([1])),
      indexLeafCell(Uint8Array.from([2])),
    ]);
    expect(leaf.cellCount).toBe(2);
    expect([...leaf.key(1)]).toEqual([2]);
    expect(leaf.layoutProblems()).toEqual([]);
  });

  it("allow cells up to a quarter of the page", () => {
    expect(maxCellSize("table-leaf", 4096)).toBe(1020);
    expect(maxCellSize("index-interior", 4096)).toBe(1019);
  });

  it("refuse bytes that are not a B-tree page, and cells that do not fit", () => {
    expect(() => new BTreePage(new Uint8Array(PAGE_SIZE))).toThrow(
      CorruptError,
    );
    const leaf = page("index-leaf");
    expect(() => {
      leaf.insertCell(0, indexLeafCell(new Uint8Array(PAGE_SIZE)));
    }).toThrow(CorruptError);
  });

  it("report a damaged layout", () => {
    const leaf = page("table-leaf");
    leaf.insertCell(0, tableLeafCell(1, record(10, 1)));
    leaf.bytes[leaf.contentStart - 1] = 1;
    expect(leaf.layoutProblems()).toEqual(["free space is not zero"]);
  });
});
