import { beforeEach, describe, expect, it } from "vitest";
import { BindError } from "../core/errors.js";
import type {
  BoundColumnRefExpression,
  LogicalCTERef,
  LogicalMaterializedCTE,
  LogicalProjection,
  LogicalRecursiveCTE,
} from "../types.js";
import { LogicalOperatorType } from "../types.js";
import { createTestContext } from "./test_helpers.js";

let catalog: ReturnType<typeof createTestContext>["catalog"];
let bind: ReturnType<typeof createTestContext>["bind"];

beforeEach(() => {
  const ctx = createTestContext();
  catalog = ctx.catalog;
  bind = ctx.bind;
});

describe("CTE — additional", () => {
  it("CTE ref produces LogicalCTERef node", () => {
    const plan = bind(
      "WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active",
    );
    const cte = plan as LogicalMaterializedCTE;
    const mainProj = cte.children[1] as LogicalProjection;
    const cteRef = mainProj.children[0] as LogicalCTERef;
    expect(cteRef.type).toBe(LogicalOperatorType.LOGICAL_CTE_REF);
    expect(cteRef.cteName).toBe("active");
  });

  it("CTE columns are accessible from main query", () => {
    const plan = bind(
      "WITH active AS (SELECT id, name FROM users WHERE active = true) SELECT name FROM active",
    );
    const cte = plan as LogicalMaterializedCTE;
    const mainProj = cte.children[1] as LogicalProjection;
    expect(mainProj.expressions).toHaveLength(1);
    const col = mainProj.expressions[0] as BoundColumnRefExpression;
    expect(col.columnName).toBe("name");
  });
});

describe("CTE — more coverage", () => {
  it("multiple CTEs", () => {
    const plan = bind(
      "WITH a AS (SELECT id FROM users), b AS (SELECT id FROM orders) SELECT * FROM a",
    );
    // Should have nested LogicalMaterializedCTE nodes
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
    const outer = plan as LogicalMaterializedCTE;
    // The inner plan should also be a materialized CTE
    expect(outer.children[1].type).toBe(
      LogicalOperatorType.LOGICAL_MATERIALIZED_CTE,
    );
  });

  it("CTE referencing another CTE", () => {
    const plan = bind(
      "WITH a AS (SELECT id, name FROM users), b AS (SELECT id FROM a) SELECT * FROM b",
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
  });
});

describe("CTE column aliases", () => {
  it("WITH cte(a, b) AS (...) applies column aliases", () => {
    const plan = bind(
      "WITH cte(a, b) AS (SELECT id, name FROM users) SELECT a, b FROM cte",
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
    const cte = plan as LogicalMaterializedCTE;
    const proj = cte.children[1] as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
    const col0 = proj.expressions[0] as BoundColumnRefExpression;
    const col1 = proj.expressions[1] as BoundColumnRefExpression;
    expect(col0.columnName).toBe("a");
    expect(col1.columnName).toBe("b");
  });

  it("CTE with wrong number of aliases throws BindError", () => {
    expect(() =>
      bind("WITH cte(a) AS (SELECT id, name FROM users) SELECT * FROM cte"),
    ).toThrow(BindError);
    expect(() =>
      bind("WITH cte(a) AS (SELECT id, name FROM users) SELECT * FROM cte"),
    ).toThrow("column aliases");
  });

  it("CTE without aliases still uses original column names", () => {
    const plan = bind(
      "WITH cte AS (SELECT id, name FROM users) SELECT id, name FROM cte",
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
    const cte = plan as LogicalMaterializedCTE;
    const proj = cte.children[1] as LogicalProjection;
    const col0 = proj.expressions[0] as BoundColumnRefExpression;
    expect(col0.columnName).toBe("id");
  });

  it("CTE aliases override original names — original names no longer resolve", () => {
    expect(() =>
      bind("WITH cte(a, b) AS (SELECT id, name FROM users) SELECT id FROM cte"),
    ).toThrow(BindError);
  });
});

describe("Recursive CTE", () => {
  it("produces LogicalRecursiveCTE wrapped in LogicalMaterializedCTE", () => {
    const plan = bind(
      "WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 10) SELECT * FROM nums",
    );
    // Outermost is MaterializedCTE wrapping the recursive CTE definition + main query
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
    const matCte = plan as LogicalMaterializedCTE;
    const recCte = matCte.children[0] as LogicalRecursiveCTE;
    expect(recCte.type).toBe(LogicalOperatorType.LOGICAL_RECURSIVE_CTE);
    expect(recCte.cteName).toBe("nums");
    expect(recCte.isUnionAll).toBe(true);
    expect(recCte.types).toHaveLength(1);
    // children[0] = anchor, children[1] = recursive term
    expect(recCte.children).toHaveLength(2);
  });

  it("binds UNION (not UNION ALL) recursive CTE", () => {
    const plan = bind(
      "WITH RECURSIVE nums AS (SELECT 1 AS n UNION SELECT n + 1 FROM nums WHERE n < 5) SELECT * FROM nums",
    );
    const matCte = plan as LogicalMaterializedCTE;
    const recCte = matCte.children[0] as LogicalRecursiveCTE;
    expect(recCte.isUnionAll).toBe(false);
  });

  it("non-recursive CTE under WITH RECURSIVE is bound normally", () => {
    const plan = bind(
      "WITH RECURSIVE helper AS (SELECT 1 AS x) SELECT * FROM helper",
    );
    const matCte = plan as LogicalMaterializedCTE;
    // helper is not self-referencing, so it should be a regular plan, not LogicalRecursiveCTE
    expect(matCte.children[0].type).not.toBe(
      LogicalOperatorType.LOGICAL_RECURSIVE_CTE,
    );
  });

  it("non-self-referencing CTE under WITH RECURSIVE binds without error", () => {
    expect(() =>
      bind("WITH RECURSIVE r AS (SELECT 1 AS n FROM users) SELECT * FROM r"),
    ).not.toThrow(); // No UNION + no self-reference → bound as normal CTE
  });

  it("errors on column count mismatch between anchor and recursive term", () => {
    expect(() =>
      bind(
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1, 2 FROM r) SELECT * FROM r",
      ),
    ).toThrow(/column/i);
  });

  it("errors on type mismatch between anchor and recursive term", () => {
    expect(() =>
      bind(
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 'text' FROM r WHERE n < 5) SELECT * FROM r",
      ),
    ).toThrow(/incompatible/i);
  });

  it("allows compatible numeric types between anchor and recursive term", () => {
    // INTEGER anchor, REAL recursive — should be compatible
    expect(() =>
      bind(
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 0.5 FROM r WHERE n < 5) SELECT * FROM r",
      ),
    ).not.toThrow();
  });

  it("errors on aggregate in recursive term", () => {
    expect(() =>
      bind(
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT SUM(n) FROM r) SELECT * FROM r",
      ),
    ).toThrow(/aggregate/i);
  });

  it("errors on GROUP BY in recursive term", () => {
    expect(() =>
      bind(
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n FROM r GROUP BY n) SELECT * FROM r",
      ),
    ).toThrow(/GROUP BY/i);
  });

  it("errors on DISTINCT in recursive term", () => {
    expect(() =>
      bind(
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT DISTINCT n + 1 FROM r WHERE n < 5) SELECT * FROM r",
      ),
    ).toThrow(/DISTINCT/i);
  });

  it("errors on HAVING in recursive term", () => {
    expect(() =>
      bind(
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r HAVING n < 5) SELECT * FROM r",
      ),
    ).toThrow(/HAVING/i);
  });
});

describe("Isolated scope CTE parent chain", () => {
  it("UNION right side inside correlated subquery can see outer CTE", () => {
    const plan = bind(
      "WITH cte AS (SELECT id, name FROM users) SELECT * FROM users WHERE EXISTS (SELECT id FROM cte UNION ALL SELECT id FROM cte)",
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
  });

  it("nested CTE accessible from UNION right side", () => {
    const plan = bind(
      "WITH a AS (SELECT id, name FROM users), b AS (SELECT id, name FROM a) SELECT id, name FROM b UNION ALL SELECT id, name FROM a",
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
  });
});
