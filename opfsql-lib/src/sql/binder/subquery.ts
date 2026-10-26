import { SemanticError } from "../../errors.js";
import type { SubqueryExpression } from "../parser/index.js";
import type { logical, OutputColumn } from "../plan/index.js";
import type { Clause } from "./clause.js";
import { comparisonOperator, not } from "./operators.js";
import type { BoundQuery } from "./scope.js";
import { checkComparable } from "./type-rules.js";

// ---------------------------------------------------------------------------
// Subqueries in expressions: a level of their own. A scalar subquery and
// one compared with IN, ANY or ALL give one column; `x IN (SELECT …)` is
// `x = ANY (…)`, and NOT IN and NOT EXISTS are NOT of them. A subquery is
// correlated when a name in it — in a subquery nested in it too — reads a
// column of an enclosing query.
// ---------------------------------------------------------------------------

type Expression = logical.Expression;

export function bindSubquery(
  expression: SubqueryExpression,
  clause: Clause,
): Expression {
  const scope = clause.scope.subquery();
  const query = scope.context.bindQuery(expression.subquery, scope);
  const { plan } = query;
  const isCorrelated = scope.isCorrelated;
  switch (expression.subqueryType) {
    case "SCALAR": {
      const { type } = onlyColumn(query, "a scalar subquery");
      return { kind: "scalar-subquery", plan, isCorrelated, type };
    }
    case "EXISTS": {
      return { kind: "exists", plan, isCorrelated, type: "BOOLEAN" };
    }
    case "NOT_EXISTS": {
      return not({ kind: "exists", plan, isCorrelated, type: "BOOLEAN" });
    }
    case "ANY":
    case "ALL": {
      const needle = clause.bind(expression.child);
      const column = onlyColumn(
        query,
        `a subquery compared with ${expression.subqueryType}`,
      );
      checkComparable(needle.type, column.type);
      return {
        kind: "quantified-comparison",
        needle,
        operator: comparisonOperator(expression.comparisonType),
        quantifier: expression.subqueryType,
        columnType: column.type,
        plan,
        isCorrelated,
        type: "BOOLEAN",
      };
    }
  }
}

function onlyColumn(query: BoundQuery, what: string): OutputColumn {
  const [column, other] = query.columns;
  if (column === undefined || other !== undefined) {
    throw new SemanticError(
      `${what} must return one column, not ${String(query.columns.length)}`,
    );
  }
  return column;
}
