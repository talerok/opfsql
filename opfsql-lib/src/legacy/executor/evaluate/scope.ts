import type { BoundExpression } from "../../binder/types.js";
import type { Value } from "../../../values/index.js";
import type { Resolver } from "../resolve.js";
import type { Tuple } from "../types.js";
import type { ExecutionContext } from "./context.js";

/** An expression compiled for one row layout: the value for a row. */
export type CompiledExpression = (row: Tuple) => Value;

/** What compiling an expression needs: where columns are, and the context. */
export interface Scope {
  readonly resolver: Resolver;
  readonly context: ExecutionContext;
  /** Compiles a subexpression in the same scope. */
  readonly compile: (expression: BoundExpression) => CompiledExpression;
}
