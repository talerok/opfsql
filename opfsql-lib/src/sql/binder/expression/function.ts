import { SemanticError } from "../../../errors.js";
import {
  lookupFunction,
  resolveCall,
  type ScalarFunction,
  scalarFunction,
  staticType,
} from "../../../functions/index.js";
import type {
  FunctionExpression,
  ParsedExpression,
} from "../../parser/types.js";
import type { AggregateContext } from "../core/context.js";
import type {
  BoundAggregateExpression,
  BoundExpression,
  BoundFunctionExpression,
} from "../types.js";
import { BoundExpressionClass } from "../types.js";
import { sameAggregate } from "./same-expression.js";

/** How a function call binds what it contains. */
export interface FunctionBinding {
  /** Binds an argument in the call's scope. */
  readonly bind: (child: ParsedExpression) => BoundExpression;
  /** Binds the call as an aggregate. */
  readonly bindAggregate: () => BoundAggregateExpression;
  /** The aggregates of the query, when binding above its aggregation. */
  readonly aggregateContext?: AggregateContext;
}

/**
 * A function call, resolved through the function registry: an unknown
 * name, a wrong number of arguments or an argument type the function does
 * not take is a SemanticError here, at bind time.
 */
export function bindFunction(
  expression: FunctionExpression,
  binding: FunctionBinding,
): BoundExpression {
  const name = expression.functionName;
  const definition = lookupFunction(name);
  if (definition === undefined) {
    throw new SemanticError(`no such function: ${name}`);
  }
  if (definition.kind === "aggregate") {
    const bound = binding.bindAggregate();
    const match = binding.aggregateContext?.aggregates.find((aggregate) =>
      sameAggregate(aggregate, bound),
    );
    return match ?? bound;
  }
  if (expression.distinct || expression.isStar) {
    throw new SemanticError(`${name}() is not an aggregate function`);
  }
  const children = expression.children.map((child) => binding.bind(child));
  return bindCall(definition, children, name);
}

/** A call of a scalar function on bound arguments, type-checked. */
export function bindCall(
  definition: ScalarFunction,
  children: BoundExpression[],
  name: string = definition.name,
): BoundFunctionExpression {
  const returnType = resolveCall(
    definition,
    children.map((child) => staticType(child.returnType)),
  );
  return {
    expressionClass: BoundExpressionClass.BOUND_FUNCTION,
    functionName: name.toUpperCase(),
    function: definition,
    children,
    returnType,
  };
}

/** `text LIKE pattern [ESCAPE escape]`: SQLite's like(pattern, text, escape). */
export function bindLike(
  text: BoundExpression,
  pattern: BoundExpression,
  escape?: BoundExpression,
): BoundFunctionExpression {
  const children = [pattern, text, ...(escape === undefined ? [] : [escape])];
  return bindCall(scalarFunction("like"), children);
}
