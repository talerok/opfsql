import { type Check, partitions, unordered } from "./check.js";
import { distinctRows, multisetDifference, type Sides } from "./compare.js";
import { print } from "./expression.js";
import type { QueryCase } from "./query.js";

// ---------------------------------------------------------------------------
// TLP, ternary logic partitioning: Q's rows are, as a multiset, those of
// Q WHERE p, Q WHERE NOT p and Q WHERE p IS NULL together, since p is
// exactly one of true, false and NULL on every row. Likewise for DISTINCT,
// for aggregates — combined from the partitions' in SQL — for groups, and
// for HAVING on groups.
// ---------------------------------------------------------------------------

const PARTS: Sides = ["the query", "its partitions"];

export function tlpChecks(queryCase: QueryCase): Check[] {
  return [
    tlpRows(queryCase, false),
    tlpRows(queryCase, true),
    tlpAggregates(queryCase),
    tlpGroups(queryCase),
    tlpHaving(queryCase),
  ];
}

export function selectList(queryCase: QueryCase): string {
  return queryCase.outputs.map((output) => print(output)).join(", ");
}

function tlpRows(queryCase: QueryCase, isDistinct: boolean): Check {
  const keyword = isDistinct ? "SELECT DISTINCT" : "SELECT";
  const query = `${keyword} ${selectList(queryCase)} FROM ${queryCase.from.sql}`;
  const parts = partitions(print(queryCase.condition)).map(
    (where) => `${query}${where}`,
  );
  return {
    oracle: isDistinct ? "TLP DISTINCT" : "TLP",
    forms: [query, ...parts].map((sql) => unordered(sql)),
    verdict: ([whole = [], ...split]) => {
      const together = split.flat();
      const expected = isDistinct ? distinctRows(together) : together;
      return multisetDifference(whole, expected, PARTS);
    },
  };
}

/** The aggregates, and how the partitions' aggregates combine into them. */
function aggregates(queryCase: QueryCase): { each: string; combined: string } {
  const measured = print(queryCase.measured);
  const each = [
    "count(*) AS n",
    `count(${measured}) AS m`,
    `sum(${print(queryCase.summed)}) AS s`,
    `min(${measured}) AS lo`,
    `max(${measured}) AS hi`,
  ];
  return {
    each: each.join(", "),
    combined: "sum(n), sum(m), sum(s), min(lo), max(hi)",
  };
}

/** The partitions' queries, one after another as a subquery to combine. */
function unionOfPartitions(
  query: string,
  condition: string,
  tail = "",
): string {
  const parts = partitions(condition).map((where) => `${query}${where}${tail}`);
  return `(${parts.join(" UNION ALL ")})`;
}

function tlpAggregates(queryCase: QueryCase): Check {
  const { each, combined } = aggregates(queryCase);
  const query = `SELECT ${each} FROM ${queryCase.from.sql}`;
  const union = unionOfPartitions(query, print(queryCase.condition));
  return {
    oracle: "TLP aggregates",
    forms: [unordered(query), unordered(`SELECT ${combined} FROM ${union}`)],
    verdict: ([whole = [], split = []]) =>
      multisetDifference(whole, split, PARTS),
  };
}

function tlpGroups(queryCase: QueryCase): Check {
  const { each, combined } = aggregates(queryCase);
  const key = print(queryCase.groupKey);
  const query = `SELECT ${key} AS g, ${each} FROM ${queryCase.from.sql}`;
  const grouping = ` GROUP BY ${key}`;
  const union = unionOfPartitions(query, print(queryCase.condition), grouping);
  return {
    oracle: "TLP GROUP BY",
    forms: [
      unordered(`${query}${grouping}`),
      unordered(`SELECT g, ${combined} FROM ${union} GROUP BY g`),
    ],
    verdict: ([whole = [], split = []]) =>
      multisetDifference(whole, split, PARTS),
  };
}

function tlpHaving(queryCase: QueryCase): Check {
  const key = print(queryCase.groupKey);
  const measured = print(queryCase.measured);
  const query = `SELECT ${key}, count(*), min(${measured}) FROM ${queryCase.from.sql} GROUP BY ${key}`;
  const condition = print(queryCase.groupCondition);
  const having = [
    ` HAVING ${condition}`,
    ` HAVING NOT ${condition}`,
    ` HAVING ${condition} IS NULL`,
  ];
  return {
    oracle: "TLP HAVING",
    forms: [query, ...having.map((clause) => `${query}${clause}`)].map((sql) =>
      unordered(sql),
    ),
    verdict: ([whole = [], ...split]) =>
      multisetDifference(whole, split.flat(), PARTS),
  };
}
