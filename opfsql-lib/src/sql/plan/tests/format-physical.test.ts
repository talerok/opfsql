import { describe, expect, it } from "vitest";
import { scalarFunction } from "../../../functions/index.js";
import type { Index, Table } from "../../../schema/index.js";
import {
  type ColumnReference,
  type Constant,
  formatPhysicalPlan,
  type Parameter,
  type physical,
} from "../index.js";

const users: Table = {
  name: "users",
  columns: [
    { name: "id", type: "INTEGER", isNotNull: true, defaultValue: null },
    { name: "name", type: "TEXT", isNotNull: false, defaultValue: null },
    { name: "age", type: "INTEGER", isNotNull: false, defaultValue: null },
  ],
  rootPage: 3,
  rowidColumn: 0,
  isAutoincrement: false,
};

const byNameAge: Index = {
  name: "users_name_age",
  table: "users",
  keys: [
    { kind: "column", column: 1 },
    { kind: "column", column: 2 },
  ],
  isUnique: false,
  rootPage: 4,
  origin: "statement",
};

const byLowerName: Index = {
  name: "users_lower_name",
  table: "users",
  keys: [{ kind: "expression", sql: "lower(name)", type: "TEXT" }],
  isUnique: false,
  rootPage: 5,
  origin: "statement",
};

function column(
  name: string,
  columnIndex: number,
  tableIndex = 1,
): ColumnReference {
  return {
    kind: "column",
    binding: { tableIndex, columnIndex },
    depth: 0,
    name,
    type: "INTEGER",
  };
}

function constant(value: number | string | null): Constant {
  const type = typeof value === "string" ? "TEXT" : "INTEGER";
  return { kind: "constant", value, type };
}

const parameter: Parameter = { kind: "parameter", index: 0, type: "ANY" };

function compare(
  left: physical.Expression,
  operator: "EQUAL" | "GREATER" | "LESS",
  right: physical.Expression,
): physical.Expression {
  return { kind: "comparison", operator, left, right, type: "BOOLEAN" };
}

function or(...operands: physical.Expression[]): physical.Expression {
  return { kind: "conjunction", operator: "OR", operands, type: "BOOLEAN" };
}

const access = { tableIndex: 1, table: users, columns: [0, 1, 2] } as const;

describe("formatPhysicalPlan", () => {
  it("shows an index scan's key range and its residual", () => {
    const scan: physical.IndexScan = {
      ...access,
      kind: "index-scan",
      index: byNameAge,
      keys: {
        equal: [constant("bob")],
        range: {
          lower: { value: constant(18), isInclusive: true },
          upper: { value: parameter, isInclusive: false },
        },
      },
      direction: "descending",
      residual: [compare(column("age", 2), "LESS", parameter)],
    };
    expect(formatPhysicalPlan(scan)).toBe(
      "index-scan users#1 using users_name_age (name = 'bob', age >= 18 AND age < $1) descending residual: age < $1",
    );
  });

  it("indents children under their node and labels join keys", () => {
    const left: physical.TableScan = {
      ...access,
      kind: "table-scan",
      direction: "ascending",
      residual: [],
    };
    const right: physical.RowidLookup = {
      ...access,
      tableIndex: 2,
      kind: "rowid-lookup",
      rowids: { kind: "list", values: [constant(1), constant(2)] },
      residual: [],
    };
    const join: physical.HashJoin = {
      kind: "hash-join",
      joinType: "left",
      left,
      right,
      leftKeys: [column("a.age", 2)],
      rightKeys: [column("b.age", 2, 2)],
      residual: [compare(column("a.id", 0), "GREATER", column("b.id", 0, 2))],
    };
    const sorted: physical.TopN = {
      kind: "top-n",
      child: join,
      keys: [
        {
          expression: column("a.name", 1),
          direction: "ascending",
          nulls: "last",
        },
      ],
      limit: constant(10),
      offset: null,
    };
    expect(formatPhysicalPlan(sorted)).toBe(
      [
        "top-n 10 by a.name ASC NULLS LAST",
        "  hash-join left on a.age = b.age residual: a.id > b.id",
        "    table-scan users#1",
        "    rowid-lookup users#2 (rowid IN (1, 2))",
      ].join("\n"),
    );
  });

  it("prints index unions, expression keys and empty values", () => {
    const lowerName: physical.Expression = {
      kind: "function",
      name: "lower",
      function: scalarFunction("lower"),
      arguments: [column("name", 1)],
      type: "TEXT",
    };
    const union: physical.IndexUnion = {
      ...access,
      kind: "index-union",
      branches: [
        {
          kind: "index",
          index: byLowerName,
          keys: { equal: [constant("bob")], range: null },
        },
        {
          kind: "rowids",
          rowids: {
            kind: "range",
            range: {
              lower: { value: constant(5), isInclusive: false },
              upper: null,
            },
          },
        },
      ],
      residual: [
        or(
          compare(lowerName, "EQUAL", constant("bob")),
          compare(column("id", 0), "GREATER", constant(5)),
        ),
      ],
    };
    const empty: physical.Values = {
      kind: "values",
      tableIndex: 7,
      rows: [],
      types: [],
    };
    expect(formatPhysicalPlan(union)).toBe(
      "index-union users#1 [using users_lower_name (lower(name) = 'bob'); rowid > 5] residual: lower(name) = 'bob' OR id > 5",
    );
    expect(formatPhysicalPlan(empty)).toBe("values#7 no rows");
  });

  it("prints subquery plans after the plan, numbered as their expressions name them", () => {
    const inner: physical.Values = {
      kind: "values",
      tableIndex: 3,
      rows: [[constant(1)]],
      types: ["INTEGER"],
    };
    const filter: physical.Filter = {
      kind: "filter",
      child: {
        ...access,
        kind: "table-scan",
        direction: "ascending",
        residual: [],
      },
      conditions: [
        {
          kind: "not",
          operand: {
            kind: "exists",
            plan: inner,
            isCorrelated: true,
            type: "BOOLEAN",
          },
          type: "BOOLEAN",
        },
        or(compare(column("age", 2), "EQUAL", constant(1)), {
          kind: "is-null",
          operand: column("age", 2),
          type: "BOOLEAN",
        }),
      ],
    };
    expect(formatPhysicalPlan(filter)).toBe(
      [
        "filter NOT EXISTS (correlated subquery 1) AND (age = 1 OR age IS NULL)",
        "  table-scan users#1",
        "subquery 1:",
        "  values#3 (1)",
      ].join("\n"),
    );
  });
});
