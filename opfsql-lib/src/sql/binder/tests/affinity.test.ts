import { describe, expect, it } from "vitest";
import { bind, only, schemaOf } from "./helpers.js";
import { bindStatement } from "../index.js";

/**
 * Each parameter of a bound statement, by number, with the affinity the
 * binder gave it: `$1 numeric`, `$2 text`, `$3 none`.
 */
function parameters(sql: string): string[] {
  const found: string[] = [];
  collect(bindStatement(only(sql), withAny), found);
  return found.toSorted((a, b) => a.localeCompare(b, "en", { numeric: true }));
}

function collect(node: unknown, found: string[]): void {
  if (typeof node !== "object" || node === null) return;
  if (isParameter(node)) {
    found.push(`$${String(node.index + 1)} ${node.affinity ?? "none"}`);
    return;
  }
  for (const value of Object.values(node)) collect(value, found);
}

function isParameter(
  node: object,
): node is { index: number; affinity: string | null } {
  return "kind" in node && node.kind === "parameter";
}

const withAny = schemaOf(
  "CREATE TABLE t (i INTEGER, r REAL, s TEXT, b BOOLEAN, j JSON, x BLOB, a ANY)",
);

describe("a parameter's affinity", () => {
  it("comes from the type of the operand it is compared with", () => {
    expect(
      parameters(
        "SELECT 1 FROM t WHERE i = $1 AND $2 < r AND s <> $3 AND b = $4 AND j = $5 AND x = $6 AND a = $7 AND $8 = $9",
      ),
    ).toEqual([
      "$1 numeric",
      "$2 numeric",
      "$3 text",
      "$4 none",
      "$5 none",
      "$6 none",
      "$7 none",
      "$8 none",
      "$9 none",
    ]);
  });

  it("is shared by the operands it is compared with at once, or none when they disagree", () => {
    expect(
      parameters(
        "SELECT 1 FROM t WHERE $1 IN (1, 2.5) AND $2 IN (s, 'x') AND $3 IN (1, 'x') AND $4 IN (i, a)",
      ),
    ).toEqual(["$1 numeric", "$2 text", "$3 none", "$4 none"]);
    expect(
      parameters(
        "SELECT 1 FROM t WHERE $1 BETWEEN i AND r AND $2 BETWEEN s AND 'z' AND $3 BETWEEN 1 AND s",
      ),
    ).toEqual(["$1 numeric", "$2 text", "$3 none"]);
    expect(
      parameters(
        "SELECT CASE $1 WHEN 1 THEN 'a' WHEN r THEN 'b' END, CASE $2 WHEN 1 THEN 'a' WHEN 'x' THEN 'b' END FROM t",
      ),
    ).toEqual(["$1 numeric", "$2 none"]);
  });

  it("of an IN item, a BETWEEN bound or a WHEN value is that of the operand", () => {
    expect(
      parameters(
        "SELECT CASE s WHEN $1 THEN 1 END FROM t WHERE i IN ($2, 3) AND r BETWEEN $3 AND $4 AND a IN ($5)",
      ),
    ).toEqual(["$1 text", "$2 numeric", "$3 numeric", "$4 numeric", "$5 none"]);
  });

  it("of a needle compared with a subquery is that of its column", () => {
    expect(
      parameters(
        "SELECT 1 FROM t WHERE $1 IN (SELECT i FROM t) AND $2 = ANY (SELECT s FROM t) AND $3 > ALL (SELECT j FROM t) AND NOT $4 IN (SELECT r FROM t)",
      ),
    ).toEqual(["$1 numeric", "$2 text", "$3 none", "$4 numeric"]);
  });

  it("is none for a parameter inside an operand: an expression or a subquery", () => {
    expect(
      parameters(
        "SELECT 1 FROM t WHERE i IN (SELECT $1) AND $2 + 0 = i AND s = ANY (SELECT $3) AND (SELECT $4) = r",
      ),
    ).toEqual(["$1 none", "$2 none", "$3 none", "$4 none"]);
  });

  it("is none anywhere else", () => {
    expect(
      parameters(
        "SELECT $1 + 1, upper($2), CAST($3 AS INTEGER) FROM t WHERE $4 LIKE 'a%' AND -$5 = 1 LIMIT $6",
      ),
    ).toEqual([
      "$1 none",
      "$2 none",
      "$3 none",
      "$4 none",
      "$5 none",
      "$6 none",
    ]);
    expect(parameters("UPDATE t SET i = $1 WHERE i = $2")).toEqual([
      "$1 none",
      "$2 numeric",
    ]);
    expect(parameters("INSERT INTO t (i, s) VALUES ($1, $2)")).toEqual([
      "$1 none",
      "$2 none",
    ]);
  });

  it("leaves an ANY column compared with a constant as it is", () => {
    const bound = bind("SELECT 1 FROM t WHERE a = 1", withAny);
    expect(bound).toMatchObject({
      plan: {
        child: {
          kind: "filter",
          conditions: [
            {
              kind: "comparison",
              left: { kind: "column", type: "ANY" },
              right: { kind: "constant", value: 1, type: "INTEGER" },
            },
          ],
        },
      },
    });
    expect(parameters("SELECT 1 FROM t WHERE a = 1 AND a IN (1, 2)")).toEqual(
      [],
    );
  });
});
