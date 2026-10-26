import { describe, expect, it } from "vitest";
import { formatPlan } from "../../plan/index.js";
import {
  bind,
  boundAs,
  lines,
  schemaOf,
  semanticError,
  shop,
  SHOP,
  type BoundOf,
} from "./helpers.js";

function insert(sql: string): BoundOf<"insert"> {
  return boundAs(sql, "insert");
}

function update(sql: string): BoundOf<"update"> {
  return boundAs(sql, "update");
}

function remove(sql: string): BoundOf<"delete"> {
  return boundAs(sql, "delete");
}

/** Whether an INSERT's source reads its target table. */
function isTargetRead(sql: string): boolean {
  return insert(sql).readsTarget;
}

describe("INSERT", () => {
  it("fills every column from VALUES unless it lists some", () => {
    const bound = insert(
      "INSERT INTO users VALUES (1, 'a', 30, 'x@y', NULL), (2, 'b', ?, NULL, '{}')",
    );
    expect(bound.columns).toEqual([0, 1, 2, 3, 4]);
    expect(bound.parameterCount).toBe(1);
    expect(formatPlan(bound.source)).toBe(
      "values #0 (1, 'a', 30, 'x@y', NULL), (2, 'b', $1, NULL, '{}')",
    );
    expect(bound.source).toMatchObject({
      types: ["INTEGER", "TEXT", "ANY", "TEXT", "TEXT"],
    });
  });

  it("fills the listed columns from a query, in its column order", () => {
    const bound = insert(
      "INSERT INTO orders (total, user_id) SELECT age, id FROM users WHERE age > ?2",
    );
    expect(bound.columns).toEqual([2, 1]);
    expect(bound.parameterCount).toBe(2);
    expect(formatPlan(bound.source)).toBe(
      lines(
        "project #1 (age#0.2, id#0.0)",
        "  filter age#0.2 > $2",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("says whether its source reads the target table, anywhere in it", () => {
    expect(isTargetRead("INSERT INTO orders (id) VALUES (1), (2)")).toBe(false);
    expect(
      isTargetRead("INSERT INTO orders (id) SELECT order_id FROM items"),
    ).toBe(false);
    expect(isTargetRead("INSERT INTO orders SELECT * FROM orders")).toBe(true);
    expect(
      isTargetRead(
        "INSERT INTO orders (id) SELECT order_id FROM items WHERE order_id NOT IN (SELECT id FROM orders)",
      ),
    ).toBe(true);
    expect(
      isTargetRead(
        "INSERT INTO orders (id) WITH known AS (SELECT id FROM Orders) SELECT id + 1 FROM known",
      ),
    ).toBe(true);
    expect(
      isTargetRead(
        "INSERT INTO orders (id) VALUES ((SELECT max(id) + 1 FROM orders))",
      ),
    ).toBe(true);
    expect(
      isTargetRead(
        "INSERT INTO orders (id) VALUES (1) ON CONFLICT DO UPDATE SET total = (SELECT max(total) FROM orders)",
      ),
    ).toBe(false);
  });

  it("counts opfsql_sequence as written when the target has AUTOINCREMENT", () => {
    const schema = schemaOf(
      ...SHOP,
      "CREATE TABLE logs (id INTEGER PRIMARY KEY AUTOINCREMENT, note TEXT)",
    );
    const isWrittenTableRead = (sql: string): boolean =>
      boundAs(sql, "insert", schema).readsTarget;
    expect(
      isWrittenTableRead(
        "INSERT INTO logs (note) SELECT name FROM opfsql_sequence",
      ),
    ).toBe(true);
    expect(
      isWrittenTableRead(
        "INSERT INTO logs (note) VALUES ((SELECT max(name) FROM opfsql_sequence))",
      ),
    ).toBe(true);
    expect(
      isWrittenTableRead(
        "INSERT INTO orders (status) SELECT name FROM opfsql_sequence",
      ),
    ).toBe(false);
  });

  it("needs each row as long as the column list, each column once", () => {
    expect(semanticError("INSERT INTO users (name) VALUES ('a', 'b')")).toBe(
      "2 values for 1 columns",
    );
    expect(semanticError("INSERT INTO users VALUES (1, 'a')")).toBe(
      "2 values for 5 columns",
    );
    expect(
      semanticError("INSERT INTO users (id, name) SELECT id FROM orders"),
    ).toBe("1 values for 2 columns");
    expect(
      semanticError("INSERT INTO users (name, NAME) VALUES ('a', 'b')"),
    ).toBe("column NAME is listed twice in INSERT");
    expect(semanticError("INSERT INTO users (nope) VALUES (1)")).toBe(
      "table users has no column named nope",
    );
    expect(semanticError("INSERT INTO nowhere VALUES (1)")).toBe(
      "no such table: nowhere",
    );
  });

  it("binds VALUES of any number of rows", () => {
    const rows = Array.from(
      { length: 200_000 },
      (_, row) => `(${String(row)})`,
    );
    const bound = insert(`INSERT INTO orders (id) VALUES ${rows.join(", ")}`);
    expect(bound.source).toMatchObject({ kind: "values", types: ["INTEGER"] });
  });

  it("refuses columns and aggregates in VALUES", () => {
    expect(semanticError("INSERT INTO users (name) VALUES (name)")).toBe(
      "no such column: name",
    );
    expect(semanticError("INSERT INTO users (age) VALUES (count(*))")).toBe(
      "aggregate functions are not allowed in VALUES",
    );
  });
});

describe("ON CONFLICT", () => {
  it("names the rowid, a constraint or a unique index by its columns", () => {
    const rowid = insert(
      "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING",
    );
    expect(rowid.onConflict).toEqual({
      target: "rowid",
      action: { kind: "nothing" },
    });
    const unique = insert(
      "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (email) DO NOTHING",
    );
    expect(unique.onConflict?.target).toBe(
      shop.index("opfsql_autoindex_users_1"),
    );
    const composite = insert(
      "INSERT INTO items VALUES (1, 'a', 2) ON CONFLICT (sku, order_id) DO NOTHING",
    );
    expect(composite.onConflict?.target).toBe(
      shop.index("opfsql_autoindex_items_1"),
    );
    const none = insert(
      "INSERT INTO users (name) VALUES ('a') ON CONFLICT DO NOTHING",
    );
    expect(none.onConflict?.target).toBeNull();
  });

  it("refuses columns no unique constraint has", () => {
    const message =
      "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint";
    expect(
      semanticError(
        "INSERT INTO users (name) VALUES ('a') ON CONFLICT (name) DO NOTHING",
      ),
    ).toBe(message);
    expect(
      semanticError(
        "INSERT INTO items VALUES (1, 'a', 2) ON CONFLICT (sku) DO NOTHING",
      ),
    ).toBe(message);
    expect(
      semanticError(
        "INSERT INTO orders VALUES (1, 1, 1, 'a') ON CONFLICT (user_id) DO NOTHING",
      ),
    ).toBe(message);
  });

  it("refuses a column the target lists twice, the rowid too", () => {
    expect(
      semanticError(
        "INSERT INTO users (id) VALUES (1) ON CONFLICT (id, ID) DO NOTHING",
      ),
    ).toBe("column ID is listed twice in ON CONFLICT");
    expect(
      semanticError(
        "INSERT INTO users (id) VALUES (1) ON CONFLICT (email, email) DO NOTHING",
      ),
    ).toBe("column email is listed twice in ON CONFLICT");
  });

  it("binds DO UPDATE over the row in the way and the refused row, `excluded`", () => {
    const bound = insert(
      "INSERT INTO items VALUES (1, 'a', 2) ON CONFLICT (order_id, sku) DO UPDATE SET qty = qty + excluded.qty WHERE items.qty < 10",
    );
    expect(bound.onConflict?.action).toMatchObject({
      kind: "update",
      targetIndex: 1,
      excludedIndex: 2,
      assignments: [
        {
          column: 2,
          value: {
            kind: "arithmetic",
            left: { binding: { tableIndex: 1, columnIndex: 2 } },
            right: { binding: { tableIndex: 2, columnIndex: 2 } },
          },
        },
      ],
      condition: {
        kind: "comparison",
        left: { binding: { tableIndex: 1, columnIndex: 2 } },
      },
    });
    expect(
      semanticError(
        "INSERT INTO users (name) VALUES ('a') ON CONFLICT DO UPDATE SET age = excluded",
      ),
    ).toBe("no such column: excluded");
    expect(
      semanticError(
        "INSERT INTO users (name) VALUES ('a') ON CONFLICT DO UPDATE SET age = 1 WHERE age",
      ),
    ).toBe("ON CONFLICT … WHERE must be BOOLEAN, not INTEGER");
  });
});

describe("UPDATE", () => {
  it("reads every column, then the rowid, under its WHERE", () => {
    const bound = update(
      "UPDATE users SET age = age + 1, name = upper(name) WHERE id = $1",
    );
    expect(bound.tableIndex).toBe(0);
    expect(bound.parameterCount).toBe(1);
    expect(formatPlan(bound.source)).toBe(
      lines(
        "filter id#0.0 = $1",
        "  get #0 users (id, name, age, email, data, rowid)",
      ),
    );
    expect(bound.assignments.map((assignment) => assignment.column)).toEqual([
      2, 1,
    ]);
  });

  it("assigns columns that exist, each once", () => {
    expect(semanticError("UPDATE users SET age = 1, AGE = 2")).toBe(
      "column AGE is assigned more than once",
    );
    expect(semanticError("UPDATE users SET nope = 1")).toBe(
      "table users has no column named nope",
    );
    expect(semanticError("UPDATE users SET age = count(*)")).toBe(
      "aggregate functions are not allowed in UPDATE",
    );
  });

  it("correlates a subquery that reads the row it updates", () => {
    const bound = update(
      "UPDATE users SET age = (SELECT max(qty) FROM items WHERE items.order_id = users.id)",
    );
    expect(bound.assignments[0]?.value).toMatchObject({
      kind: "scalar-subquery",
      isCorrelated: true,
    });
  });
});

describe("DELETE", () => {
  it("reads the rows its WHERE selects, with their rowids", () => {
    const bound = remove("DELETE FROM orders WHERE total > 5 AND status = 'x'");
    expect(formatPlan(bound.source)).toBe(
      lines(
        "filter total#0.2 > 5 AND status#0.3 = 'x'",
        "  get #0 orders (id, user_id, total, status, rowid)",
      ),
    );
    expect(formatPlan(remove("DELETE FROM orders").source)).toBe(
      "get #0 orders (id, user_id, total, status, rowid)",
    );
    expect(semanticError("DELETE FROM orders WHERE total")).toBe(
      "WHERE must be BOOLEAN, not REAL",
    );
  });
});

describe("parameterCount", () => {
  it("is the highest parameter number used", () => {
    const counts = [
      "SELECT 1",
      "SELECT ?",
      "SELECT ?, ?",
      "SELECT $3",
      "SELECT (SELECT $2 FROM users WHERE id = ?)",
      "WITH a AS (SELECT $4 AS x) SELECT x FROM a",
    ].map((sql) => {
      const bound = bind(sql);
      return bound.kind === "query" ? bound.parameterCount : -1;
    });
    expect(counts).toEqual([0, 1, 2, 3, 3, 4]);
  });
});
