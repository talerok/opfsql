import {
  type Check,
  ordered as orderedForm,
  partitions,
  unordered,
} from "./check.js";
import {
  distinctRows,
  multisetDifference,
  rowKey,
  sequenceDifference,
} from "./compare.js";
import { print } from "./expression.js";
import type { QueryCase } from "./query.js";
import { selectList, tlpChecks } from "./tlp.js";

// ---------------------------------------------------------------------------
// The oracles. Each check is a few forms of one question whose results must
// agree:
// - TLP (tlp.ts): a query's rows are those of its three partitions on p;
// - NoREC: count(*) WHERE p is the number of rows for which p, computed in
//   the select list where no access path can use it, is true;
// - LIMIT and OFFSET take a slice of the rows ORDER BY puts in order;
// - UNION, INTERSECT and EXCEPT of a query and its partitions are what
//   their sets say;
// - parameters: p with some constants as parameters finds the same rows.
// Every form also runs on the copy of the database with the random indexes,
// and must return the same there: index invariance.
// ---------------------------------------------------------------------------

export function checksOf(queryCase: QueryCase): Check[] {
  return [
    ...tlpChecks(queryCase),
    noRec(queryCase),
    limited(queryCase),
    setOperations(queryCase),
    parameters(queryCase),
  ];
}

function noRec(queryCase: QueryCase): Check {
  const condition = print(queryCase.condition);
  const from = queryCase.from.sql;
  const counted = `SELECT count(*) FROM ${from} WHERE ${condition}`;
  const computed = `SELECT sum(CASE WHEN ${condition} THEN 1 ELSE 0 END) FROM ${from}`;
  return {
    oracle: "NoREC",
    forms: [unordered(counted), unordered(computed)],
    verdict: ([count = [], sum = []]) => {
      // The sum over no rows is NULL, where the count is 0.
      const total = sum[0]?.[0] ?? 0;
      return multisetDifference(count, [[total]]);
    },
  };
}

/** ORDER BY every column, so that ties are equal rows, with and without LIMIT … OFFSET. */
function limited(queryCase: QueryCase): Check {
  const keys = queryCase.outputs.map((_, index) => {
    const direction = index % 2 === 0 ? "" : " DESC";
    return `${String(index + 1)}${direction}`;
  });
  const { limit, offset } = queryCase;
  const query = `SELECT ${selectList(queryCase)} FROM ${queryCase.from.sql} WHERE ${print(queryCase.condition)} ORDER BY ${keys.join(", ")}`;
  return {
    oracle: "LIMIT … OFFSET",
    forms: [
      orderedForm(query),
      orderedForm(`${query} LIMIT ${String(limit)} OFFSET ${String(offset)}`),
    ],
    verdict: ([all = [], slice = []]) =>
      sequenceDifference(all.slice(offset, offset + limit), slice),
  };
}

function setOperations(queryCase: QueryCase): Check {
  const query = `SELECT ${selectList(queryCase)} FROM ${queryCase.from.sql}`;
  const [matching = "", ...others] = partitions(print(queryCase.condition)).map(
    (where) => `${query}${where}`,
  );
  return {
    oracle: "UNION, INTERSECT, EXCEPT",
    forms: [
      query,
      matching,
      [matching, ...others].join(" UNION "),
      `${query} INTERSECT ${matching}`,
      `${query} EXCEPT ${matching}`,
    ].map((sql) => unordered(sql)),
    verdict: ([
      whole = [],
      part = [],
      union = [],
      intersect = [],
      except = [],
    ]) => {
      const inPart = new Set(part.map((row) => rowKey(row)));
      const outside = distinctRows(whole).filter(
        (row) => !inPart.has(rowKey(row)),
      );
      return (
        labelled("UNION", multisetDifference(distinctRows(whole), union)) ??
        labelled(
          "INTERSECT",
          multisetDifference(distinctRows(part), intersect),
        ) ??
        labelled("EXCEPT", multisetDifference(outside, except))
      );
    },
  };
}

function parameters(queryCase: QueryCase): Check {
  const query = `SELECT ${selectList(queryCase)} FROM ${queryCase.from.sql} WHERE `;
  const { expression, values } = queryCase.parameterized;
  return {
    oracle: "parameters",
    forms: [
      unordered(`${query}${print(queryCase.condition)}`),
      unordered(`${query}${print(expression)}`, values),
    ],
    verdict: ([constants = [], bound = []]) =>
      multisetDifference(constants, bound),
  };
}

function labelled(label: string, difference: string | null): string | null {
  return difference === null ? null : `${label}: ${difference}`;
}
