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
import { constant, print } from "./expression.js";
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

/**
 * ORDER BY every column, so that ties are equal rows — after a key, when
 * the case has one — with and without LIMIT … OFFSET.
 */
function limited(queryCase: QueryCase): Check {
  const positions = queryCase.outputs.map((_, index) => {
    const direction = index % 2 === 0 ? "" : " DESC NULLS FIRST";
    return `${String(index + 1)}${direction}`;
  });
  const keys =
    queryCase.orderKey === null
      ? positions
      : [queryCase.orderKey, ...positions];
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

/**
 * The condition with some constants as parameters, prepared once and run
 * with their values, then with others; each against the condition with
 * those values written in.
 */
function parameters(queryCase: QueryCase): Check {
  const query = `SELECT ${selectList(queryCase)} FROM ${queryCase.from.sql} WHERE `;
  const { expression, values, others } = queryCase.parameterized;
  const prepared = `${query}${print(expression)}`;
  const written = `${query}${withLiterals(print(expression), others)}`;
  const otherValues = others.map((sql) => constant("untyped", sql).value);
  return {
    oracle: "parameters",
    forms: [
      unordered(`${query}${print(queryCase.condition)}`),
      unordered(prepared, values),
      unordered(written),
      unordered(prepared, otherValues),
    ],
    verdict: ([
      constants = [],
      bound = [],
      otherConstants = [],
      otherBound = [],
    ]) =>
      labelled("first values", multisetDifference(constants, bound)) ??
      labelled("next values", multisetDifference(otherConstants, otherBound)),
  };
}

/** `sql` with `$n` replaced by the nth of `literals`. */
function withLiterals(sql: string, literals: readonly string[]): string {
  return sql.replaceAll(/\$(\d+)/gu, (parameter, number: string) => {
    return literals[Number(number) - 1] ?? parameter;
  });
}

function labelled(label: string, difference: string | null): string | null {
  return difference === null ? null : `${label}: ${difference}`;
}
