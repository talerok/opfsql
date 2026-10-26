// The expression compiler (docs/planning.md, Execution): the expressions of
// a physical plan, compiled once per plan node and run into closures that
// read a row. The executor gives it layouts, the run's parameters and a
// compiler for subquery plans; the optimizer folds constants with it.

export { compileAggregate, type CompiledAggregate } from "./aggregate.js";
export {
  type CompiledPredicate,
  compileExpression,
  compilePredicate,
} from "./compile.js";
export { evaluateConstant } from "./constant.js";
export {
  type CompiledExpression,
  type CompiledSubquery,
  createRun,
  type Layout,
  type OuterRows,
  type QueryContext,
  type QueryLevel,
  type Run,
  type Scope,
  type SubqueryCompiler,
  type Tuple,
} from "./scope.js";
