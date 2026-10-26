import { describe, expect, it } from "vitest";
import { type physical, ROWID_COLUMN } from "../../plan/index.js";
import { sqlPhysicalPlan } from "./sql.js";

// Column pruning (docs/planning.md, Physical plans): a scan reads only the
// columns some node above it uses, and the rowid only when one does. The
// tables are builders.ts's: users(id INTEGER PRIMARY KEY, name, age, email,
// tag) and orders(id INTEGER PRIMARY KEY, user_id, total, status, paid).

/** Each scan of the plan, as `table#index: columns` by name. */
function scansOf(sql: string): string[] {
  const found: string[] = [];
  visit(sqlPhysicalPlan(sql), found);
  return found;
}

function visit(plan: physical.Plan, found: string[]): void {
  if ("columns" in plan) {
    const names = plan.columns.map((column) =>
      column === ROWID_COLUMN
        ? "rowid"
        : (plan.table.columns[column]?.name ?? "?"),
    );
    found.push(
      `${plan.table.name}#${String(plan.tableIndex)}: ${names.join(", ")}`,
    );
  }
  for (const child of childrenOf(plan)) visit(child, found);
}

function childrenOf(plan: physical.Plan): physical.Plan[] {
  if ("child" in plan) return [plan.child];
  if ("left" in plan) return [plan.left, plan.right];
  if (plan.kind === "cte") return [plan.definition, plan.body];
  return [];
}

describe("column pruning", () => {
  it("reads only the columns an aggregate groups by and computes from", () => {
    expect(
      scansOf("SELECT age, count(*), max(name) FROM users GROUP BY age"),
    ).toEqual(["users#0: name, age"]);
  });

  it("reads no column at all for count(*)", () => {
    expect(scansOf("SELECT count(*) FROM users")).toEqual(["users#0: "]);
  });

  it("keeps what a residual reads, though nothing above reads it", () => {
    expect(scansOf("SELECT name FROM users WHERE email || name = 'x'")).toEqual(
      ["users#0: name, email"],
    );
  });

  it("reads the columns the select list reads, in the table's order", () => {
    expect(scansOf("SELECT name FROM users")).toEqual(["users#0: name"]);
    expect(scansOf("SELECT name, id FROM users")).toEqual([
      "users#0: id, name",
    ]);
  });

  it("keeps a lookup join's keys on its left side, and nothing unread on its right", () => {
    expect(
      scansOf(
        "SELECT o.total FROM orders o JOIN users u ON u.id = o.user_id WHERE o.id = $1",
      ),
    ).toEqual(["orders#0: user_id, total", "users#1: "]);
  });

  it("keeps a hash join's keys and residual on both sides", () => {
    expect(
      scansOf(
        "SELECT u.name FROM users u JOIN orders o ON o.status = u.email AND o.total > u.age",
      ),
    ).toEqual(["users#0: name, age, email", "orders#1: total, status"]);
  });

  it("keeps every column of a row a correlated subquery reads", () => {
    expect(
      scansOf(
        "SELECT u.name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id + 0)",
      ),
    ).toEqual(["users#0: id, name, age, email, tag"]);
  });

  it("keeps every column of a set operation's sides and a DISTINCT's input", () => {
    expect(
      scansOf(
        "SELECT name, age FROM users UNION SELECT status, user_id FROM orders",
      ),
    ).toEqual(["users#0: name, age", "orders#2: user_id, status"]);
  });
});
