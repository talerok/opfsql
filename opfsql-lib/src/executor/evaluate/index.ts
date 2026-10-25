export { compileExpression, compilePredicate } from "./compile.js";
export { evaluateConstant } from "./constant.js";
export { fixedValue, parameterValue, required } from "./parameter.js";
export {
  type ExecutionContext,
  type OuterRow,
  ROW_ONLY_CONTEXT,
} from "./context.js";
export { comparisonConversion, type Conversion } from "./affinity.js";
export { compare, compareTyped } from "./logic.js";
export type { CompiledExpression } from "./scope.js";
