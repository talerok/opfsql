import { describe, expect, it } from "vitest";
import type { Table } from "../../../schema/index.js";
import type { ColumnBinding } from "../index.js";
import { logical, physical, ROWID_COLUMN } from "../index.js";

const users: Table = {
  name: "users",
  columns: [
    { name: "id", type: "INTEGER", isNotNull: false, defaultValue: null },
    { name: "name", type: "TEXT", isNotNull: false, defaultValue: null },
  ],
  rootPage: 3,
  rowidColumn: 0,
  isAutoincrement: false,
};

const get: logical.Get = {
  kind: "get",
  tableIndex: 1,
  table: users,
  columns: [1, ROWID_COLUMN],
};

const TRUE = { kind: "constant", value: true, type: "BOOLEAN" } as const;

function bindings(...pairs: [number, number][]): ColumnBinding[] {
  return pairs.map(([tableIndex, columnIndex]) => ({
    tableIndex,
    columnIndex,
  }));
}

describe("layoutOf", () => {
  it("gives a table's columns, the rowid included, under its table index", () => {
    expect(logical.layoutOf(get)).toEqual(bindings([1, 1], [1, ROWID_COLUMN]));
  });

  it("passes a child's layout through filters, orders and limits", () => {
    const limited: logical.Plan = {
      kind: "limit",
      limit: null,
      offset: null,
      child: { kind: "filter", conditions: [TRUE], child: get },
    };
    expect(logical.layoutOf(limited)).toEqual(logical.layoutOf(get));
  });

  it("binds an aggregate's groups and aggregates to their own indexes", () => {
    const aggregate: logical.Aggregate = {
      kind: "aggregate",
      groupIndex: 2,
      aggregateIndex: 3,
      child: get,
      groups: [
        {
          kind: "column",
          binding: { tableIndex: 1, columnIndex: 1 },
          depth: 0,
          name: "name",
          type: "TEXT",
        },
      ],
      aggregates: [],
    };
    expect(logical.layoutOf(aggregate)).toEqual(bindings([2, 0]));
  });

  it("gives a join both sides, left first, as a physical plan does", () => {
    const scan: physical.TableScan = {
      kind: "table-scan",
      tableIndex: 1,
      table: users,
      columns: [0],
      residual: [],
      direction: "ascending",
    };
    const join: physical.Plan = {
      kind: "nested-loop-join",
      joinType: "left",
      left: scan,
      right: { ...scan, tableIndex: 4, columns: [0, 1] },
      conditions: [],
    };
    expect(physical.layoutOf(join)).toEqual(bindings([1, 0], [4, 0], [4, 1]));
  });

  it("gives a lookup join the left side's columns, then its right scan's", () => {
    const scan: physical.TableScan = {
      kind: "table-scan",
      tableIndex: 1,
      table: users,
      columns: [1, 0],
      residual: [],
      direction: "ascending",
    };
    const leftId = {
      kind: "column",
      binding: { tableIndex: 1, columnIndex: 0 },
      depth: 0,
      name: "id",
      type: "INTEGER",
    } as const;
    const join: physical.Plan = {
      kind: "lookup-join",
      joinType: "inner",
      left: scan,
      right: {
        kind: "rowid-lookup",
        tableIndex: 4,
        table: users,
        columns: [ROWID_COLUMN, 1],
        residual: [],
        rowids: { kind: "list", values: [leftId] },
        direction: "ascending",
      },
      residual: [],
    };
    expect(physical.layoutOf(join)).toEqual(
      bindings([1, 1], [1, 0], [4, ROWID_COLUMN], [4, 1]),
    );
    expect(physical.typesOf(join)).toEqual([
      "TEXT",
      "INTEGER",
      "INTEGER",
      "TEXT",
    ]);
  });
});
