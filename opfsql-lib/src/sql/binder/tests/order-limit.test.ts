import { describe, expect, it } from "vitest";
import { DatatypeMismatchError } from "../../../errors.js";
import { lines, plan, query, semanticError, thrown } from "./helpers.js";

describe("ORDER BY", () => {
  it("reads an integer literal as a 1-based output position", () => {
    expect(plan("SELECT name, age FROM users ORDER BY 2, 1 DESC")).toBe(
      lines(
        "order age#1.1 ASC NULLS FIRST, name#1.0 DESC NULLS LAST",
        "  project #1 (name#0.1, age#0.2)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
    expect(semanticError("SELECT name FROM users ORDER BY 2")).toBe(
      "ORDER BY position 2 is not between 1 and 1",
    );
    expect(semanticError("SELECT name FROM users ORDER BY 0")).toBe(
      "ORDER BY position 0 is not between 1 and 1",
    );
  });

  it("reads a bare name that is an output's alias as that output", () => {
    expect(plan("SELECT name AS n FROM users ORDER BY n")).toBe(
      lines(
        "order name#1.0 ASC NULLS FIRST",
        "  project #1 (name#0.1)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
    expect(
      plan("SELECT name AS age, age AS name FROM users ORDER BY age"),
    ).toBe(
      lines(
        "order name#1.0 ASC NULLS FIRST",
        "  project #1 (name#0.1, age#0.2)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
    expect(
      semanticError("SELECT name AS x, age AS x FROM users ORDER BY x"),
    ).toBe("ORDER BY x is ambiguous");
    expect(plan("SELECT name AS x, name AS x FROM users ORDER BY x")).toBe(
      lines(
        "order name#1.0 ASC NULLS FIRST",
        "  project #1 (name#0.1, name#0.1)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("sorts by an output an expression equals", () => {
    expect(plan("SELECT age + 1 FROM users ORDER BY age + 1 DESC")).toBe(
      lines(
        "order column1#1.0 DESC NULLS LAST",
        "  project #1 (age#0.2 + 1)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("computes any other expression as a column the result does not show", () => {
    expect(
      plan("SELECT name FROM users ORDER BY age DESC NULLS FIRST, id"),
    ).toBe(
      lines(
        "project #2 (name#1.0)",
        "  order age#1.1 DESC NULLS FIRST, id#1.2 ASC NULLS FIRST",
        "    project #1 (name#0.1, age#0.2, id#0.0)",
        "      get #0 users (id, name, age, email, data)",
      ),
    );
    expect(query("SELECT name FROM users ORDER BY age").columns).toEqual([
      { name: "name", type: "TEXT" },
    ]);
  });

  it("puts NULLs first ascending and last descending, unless told", () => {
    expect(
      plan(
        "SELECT id FROM users ORDER BY id, id DESC, id NULLS LAST, id DESC NULLS FIRST",
      ),
    ).toBe(
      lines(
        "order id#1.0 ASC NULLS FIRST, id#1.0 DESC NULLS LAST, id#1.0 ASC NULLS LAST, id#1.0 DESC NULLS FIRST",
        "  project #1 (id#0.0)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("names only output columns after DISTINCT", () => {
    expect(plan("SELECT DISTINCT name FROM users ORDER BY name DESC")).toBe(
      lines(
        "order name#1.0 DESC NULLS LAST",
        "  distinct",
        "    project #1 (name#0.1)",
        "      get #0 users (id, name, age, email, data)",
      ),
    );
    expect(semanticError("SELECT DISTINCT name FROM users ORDER BY age")).toBe(
      "for SELECT DISTINCT, ORDER BY expressions must appear in the select list",
    );
  });

  it("names only output columns after a compound query", () => {
    expect(
      plan(
        "SELECT id FROM users UNION SELECT user_id FROM orders ORDER BY 1 DESC",
      ),
    ).toBe(
      lines(
        "order id#4.0 DESC NULLS LAST",
        "  union #4",
        "    project #1 (id#0.0)",
        "      get #0 users (id, name, age, email, data)",
        "    project #3 (user_id#2.1)",
        "      get #2 orders (id, user_id, total, status)",
      ),
    );
    expect(
      plan(
        "SELECT id AS k FROM users UNION SELECT user_id FROM orders ORDER BY k",
      ),
    ).toBe(
      lines(
        "order k#4.0 ASC NULLS FIRST",
        "  union #4",
        "    project #1 (id#0.0)",
        "      get #0 users (id, name, age, email, data)",
        "    project #3 (user_id#2.1)",
        "      get #2 orders (id, user_id, total, status)",
      ),
    );
    expect(
      semanticError(
        "SELECT id FROM users UNION SELECT user_id FROM orders ORDER BY id + 1",
      ),
    ).toBe(
      "an ORDER BY key of a compound query must be one of its result columns",
    );
  });
});

describe("LIMIT and OFFSET", () => {
  it("take integer constants", () => {
    expect(plan("SELECT id FROM users ORDER BY id LIMIT 1")).toBe(
      lines(
        "limit 1",
        "  order id#1.0 ASC NULLS FIRST",
        "    project #1 (id#0.0)",
        "      get #0 users (id, name, age, email, data)",
      ),
    );
    expect(plan("SELECT id FROM users LIMIT 5, 10")).toBe(
      lines(
        "limit 10 offset 5",
        "  project #1 (id#0.0)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
    expect(plan("SELECT id FROM users LIMIT 0")).toBe(
      lines(
        "limit 0",
        "  project #1 (id#0.0)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("convert a constant as an INTEGER column does", () => {
    expect(plan("SELECT id FROM users LIMIT '5' OFFSET 2.0")).toBe(
      lines(
        "limit 5 offset 2",
        "  project #1 (id#0.0)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("keep a negative count, which the run reads as no limit or none skipped", () => {
    expect(plan("SELECT id FROM users LIMIT -1 OFFSET -5")).toBe(
      lines(
        "limit -1 offset -5",
        "  project #1 (id#0.0)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
    expect(plan("SELECT id FROM users OFFSET 0")).toBe(
      lines(
        "limit all offset 0",
        "  project #1 (id#0.0)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("leave a parameter's value to the run", () => {
    const bound = query("SELECT id FROM users LIMIT ? OFFSET $3");
    expect(bound.parameterCount).toBe(3);
    expect(bound.plan).toMatchObject({
      kind: "limit",
      limit: { kind: "parameter", index: 0, type: "INTEGER" },
      offset: { kind: "parameter", index: 2, type: "INTEGER" },
    });
  });

  it("refuse a constant that is no integer when the statement is prepared", () => {
    const mismatch = (sql: string): string => {
      const error = thrown(sql);
      expect(error).toBeInstanceOf(DatatypeMismatchError);
      return error.message;
    };
    expect(mismatch("SELECT 1 LIMIT 1.5")).toBe(
      "LIMIT: cannot store 1.5 as INTEGER",
    );
    expect(mismatch("SELECT 1 LIMIT 'five'")).toBe(
      'LIMIT: cannot store "five" as INTEGER',
    );
    expect(mismatch("SELECT 1 LIMIT NULL")).toBe(
      "LIMIT must be an integer, not NULL",
    );
    expect(mismatch("SELECT 1 LIMIT 1 OFFSET 0.5")).toBe(
      "OFFSET: cannot store 0.5 as INTEGER",
    );
  });

  it("refuse what is neither a constant nor a parameter", () => {
    expect(semanticError("SELECT 1 LIMIT 1 + 1")).toBe(
      "LIMIT must be a constant or a parameter",
    );
    expect(semanticError("SELECT 1 LIMIT 1 OFFSET -$1")).toBe(
      "OFFSET must be a constant or a parameter",
    );
  });
});
