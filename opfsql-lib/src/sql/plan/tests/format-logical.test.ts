import { describe, expect, it } from "vitest";
import { scalarFunction } from "../../../functions/index.js";
import type { Table } from "../../../schema/index.js";
import {
  type ColumnReference,
  formatPlan,
  type logical,
  ROWID_COLUMN,
} from "../index.js";

const users: Table = {
  name: "users",
  columns: [
    { name: "id", type: "INTEGER", isNotNull: false, defaultValue: null },
    { name: "data", type: "JSON", isNotNull: false, defaultValue: null },
  ],
  rootPage: 3,
  rowidColumn: 0,
  isAutoincrement: false,
};

/** Column `columnIndex` of the users get, #0. */
function column(columnIndex: number, depth = 0): logical.Expression {
  return {
    kind: "column",
    binding: { tableIndex: 0, columnIndex },
    depth,
    name: columnIndex === ROWID_COLUMN ? "rowid" : `c${String(columnIndex)}`,
    type: "ANY",
  };
}

describe("formatPlan", () => {
  it("prints a node per line, its children indented, its subqueries after it", () => {
    const subquery: logical.Plan = {
      kind: "project",
      tableIndex: 3,
      child: { kind: "values", tableIndex: 2, rows: [[]], types: [] },
      expressions: [column(1, 1)],
    };
    const plan: logical.Plan = {
      kind: "limit",
      limit: null,
      offset: { kind: "constant", value: 5, type: "INTEGER" },
      child: {
        kind: "filter",
        conditions: [
          {
            kind: "comparison",
            operator: "LESS_EQUAL",
            left: column(ROWID_COLUMN),
            right: {
              kind: "scalar-subquery",
              plan: subquery,
              isCorrelated: true,
              type: "ANY",
            },
            type: "BOOLEAN",
          },
          {
            kind: "function",
            name: "json_valid",
            function: scalarFunction("json_valid"),
            arguments: [{ kind: "constant", value: { a: [1] }, type: "JSON" }],
            type: "BOOLEAN",
          },
        ],
        child: {
          kind: "get",
          tableIndex: 0,
          table: users,
          columns: [1, ROWID_COLUMN],
        },
      },
    };
    expect(formatPlan(plan)).toBe(
      [
        "limit all offset 5",
        "  filter rowid#0.rowid <= subquery(1) AND json_valid(JSON '{\"a\":[1]}')",
        "    subquery(1, correlated): project #3 (c1#0.1^1)",
        "      values #2 ()",
        "    get #0 users (data, rowid)",
      ].join("\n"),
    );
  });

  it("puts a negated operand that starts with a minus in parentheses", () => {
    const minusOne: logical.Expression = {
      kind: "constant",
      value: -1,
      type: "INTEGER",
    };
    const negate = (operand: logical.Expression): logical.Expression => ({
      kind: "negate",
      operand,
      type: "INTEGER",
    });
    const plan: logical.Plan = {
      kind: "values",
      tableIndex: 0,
      rows: [[negate(minusOne), negate(negate(column(1))), negate(column(1))]],
      types: ["INTEGER", "INTEGER", "INTEGER"],
    };
    expect(formatPlan(plan)).toBe("values #0 (-(-1), -(-c1#0.1), -c1#0.1)");
  });

  it("quotes a JSON path key that is not a plain name, as physical plans do", () => {
    const data: ColumnReference = {
      kind: "column",
      binding: { tableIndex: 0, columnIndex: 1 },
      depth: 0,
      name: "data",
      type: "JSON",
    };
    const plan: logical.Plan = {
      kind: "values",
      tableIndex: 1,
      rows: [
        [
          {
            kind: "json-access",
            column: data,
            path: [
              { kind: "key", key: "plain" },
              { kind: "key", key: "a b" },
              { kind: "index", index: 2 },
            ],
            type: "JSON",
          },
        ],
      ],
      types: ["JSON"],
    };
    expect(formatPlan(plan)).toBe('values #1 (data#0.1.plain."a b"[2])');
  });

  it("keeps a REAL constant's decimal point", () => {
    const plan: logical.Plan = {
      kind: "values",
      tableIndex: 0,
      rows: [
        [
          { kind: "constant", value: 2, type: "REAL" },
          { kind: "constant", value: 2, type: "INTEGER" },
          { kind: "constant", value: "it's", type: "TEXT" },
          { kind: "constant", value: Uint8Array.from([0xab]), type: "BLOB" },
        ],
      ],
      types: ["REAL", "INTEGER", "TEXT", "BLOB"],
    };
    expect(formatPlan(plan)).toBe("values #0 (2.0, 2, 'it''s', X'ab')");
  });
});
