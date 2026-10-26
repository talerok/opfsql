import { describe, expect, it } from "vitest";
import { lines } from "./builders.js";
import { expectSameRows, expectSameSequence } from "./same-rows.js";
import { boundPlan, sqlPlanOf } from "./sql.js";

// LIMIT and OFFSET below projections (limit.ts): the select list is computed
// for the rows returned, not for the rows OFFSET skips.

const ORDER_COUNT = "(SELECT count(*) FROM orders o WHERE o.user_id = u.id)";

const COUNT_SUBQUERY = [
  "subquery 1:",
  "  project#4 count(*)",
  "    hash-aggregate#2#3 compute count(*)",
  "      index-scan orders#1 using orders_user (user_id = id)",
];

describe("LIMIT and OFFSET below projections", () => {
  it("computes a correlated select list only for the rows a LIMIT returns", () => {
    const sql = `SELECT u.name, ${ORDER_COUNT} FROM users u LIMIT 2 OFFSET 1000`;
    expect(sqlPlanOf(sql)).toBe(
      lines(
        "project#5 name, (correlated subquery 1)",
        "  limit 2 offset 1000",
        "    table-scan users#0",
        ...COUNT_SUBQUERY,
      ),
    );
    expectSameRows(
      boundPlan(`SELECT u.name, ${ORDER_COUNT} FROM users u LIMIT 2 OFFSET 5`),
    );
  });

  it("takes an ORDER BY below the projects with its limit when its keys are computed there", () => {
    const sql = `SELECT u.name, ${ORDER_COUNT} FROM users u ORDER BY u.email LIMIT 2 OFFSET 1000`;
    expect(sqlPlanOf(sql)).toBe(
      lines(
        "project#6 name, column2",
        "  project#5 name, (correlated subquery 1), email",
        "    limit 2 offset 1000",
        "      index-scan users#0 using users_email",
        ...COUNT_SUBQUERY,
      ),
    );
    const shown = `SELECT u.name, ${ORDER_COUNT} FROM users u WHERE u.age > 5 ORDER BY u.email LIMIT 3 OFFSET 2`;
    expectSameSequence(boundPlan(shown));
  });

  it("keeps a sort by a correlated subquery above the projects that compute it", () => {
    const sql = `SELECT u.name, ${ORDER_COUNT} FROM users u ORDER BY 2 DESC LIMIT 2`;
    expect(sqlPlanOf(sql)).toBe(
      lines(
        "top-n 2 by column2 DESC",
        "  project#5 name, (correlated subquery 1)",
        "    table-scan users#0",
        ...COUNT_SUBQUERY,
      ),
    );
  });

  it("keeps a sort by a volatile value above the project that computes it", () => {
    expect(
      sqlPlanOf("SELECT name, random() FROM users ORDER BY 2 LIMIT 2"),
    ).toBe(
      lines(
        "top-n 2 by column2 ASC",
        "  project#1 name, random()",
        "    table-scan users#0",
      ),
    );
  });

  it("stays above a DISTINCT, which needs every row", () => {
    expect(sqlPlanOf("SELECT DISTINCT name FROM users LIMIT 2")).toBe(
      lines(
        "limit 2",
        "  hash-distinct",
        "    project#1 name",
        "      table-scan users#0",
      ),
    );
  });
});
