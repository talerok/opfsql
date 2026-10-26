import type { ParsedExpression } from "../parser/index.js";
import type { AggregateCall, ColumnReference, logical } from "../plan/index.js";
import type { Scope } from "./scope.js";
import { isSameAggregate } from "./same-expression.js";
import { checkCondition } from "./type-rules.js";

// ---------------------------------------------------------------------------
// A clause binds its expressions: in one query's scope, with a rule for
// aggregate calls. Above a query's aggregate (the select list, HAVING,
// ORDER BY) calls are collected into the aggregate node and replaced by its
// columns; everywhere else they are refused.
// ---------------------------------------------------------------------------

export type AggregateRule =
  | { readonly kind: "collect"; readonly collector: AggregateCollector }
  /** `where` completes "aggregate functions are not allowed in …". */
  | { readonly kind: "refuse"; readonly where: string };

export interface Clause {
  readonly scope: Scope;
  readonly aggregates: AggregateRule;
  /** Binds an expression of the clause (expression.ts). */
  bind(expression: ParsedExpression): logical.Expression;
  /** The clause, with another rule for aggregates: an aggregate's arguments. */
  withAggregates(rule: AggregateRule): Clause;
}

/** A condition: bound, and BOOLEAN or of a type a row decides. */
export function bindCondition(
  expression: ParsedExpression,
  clause: Clause,
  where: string,
): logical.Expression {
  const bound = clause.bind(expression);
  checkCondition(bound.type, where);
  return bound;
}

/** The conjuncts of a condition: `a AND b` is two. */
export function conjunctsOf(condition: logical.Expression): readonly logical.Expression[] {
  const isAnd = condition.kind === "conjunction" && condition.operator === "AND";
  return isAnd ? condition.operands : [condition];
}

/** The rule for clauses that refuse aggregates. */
export function refuse(where: string): AggregateRule {
  return { kind: "refuse", where };
}

/** The aggregate calls of one query, each once, bound to `aggregateIndex`. */
export class AggregateCollector {
  private readonly calls: AggregateCall<logical.Plan>[] = [];

  constructor(readonly aggregateIndex: number) {}

  get aggregates(): readonly AggregateCall<logical.Plan>[] {
    return this.calls;
  }

  /** The column of the aggregate node that holds `call`'s result. */
  add(call: AggregateCall<logical.Plan>, name: string): ColumnReference {
    let position = this.calls.findIndex((other) =>
      isSameAggregate(other, call),
    );
    if (position === -1) {
      position = this.calls.length;
      this.calls.push(call);
    }
    return {
      kind: "column",
      binding: { tableIndex: this.aggregateIndex, columnIndex: position },
      depth: 0,
      name,
      type: call.type,
    };
  }
}
