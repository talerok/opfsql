// The contract between binder, optimizer and executor (docs/planning.md).

export type { JsonPathStep } from "../../functions/index.js";
export type { ArithmeticOperator, StaticType } from "../../values/index.js";

export {
  type AggregateCall,
  type Arithmetic,
  type Between,
  type Case,
  type CaseBranch,
  type Cast,
  type ColumnBinding,
  type ColumnReference,
  type Comparison,
  type ComparisonOperator,
  type Concatenate,
  type Conjunction,
  type Constant,
  type Exists,
  type Expression,
  type FunctionCall,
  type InList,
  isSameColumn,
  type IsNull,
  type JsonAccess,
  type SubqueryExpression,
  type Negate,
  type Not,
  type OrderDirection,
  type OrderKey,
  type Parameter,
  positionIn,
  type QuantifiedComparison,
  ROWID_COLUMN,
  type ScalarSubquery,
} from "./expression.js";
export { formatPlan } from "./format-logical.js";
export { columnType, positions } from "./logical.js";
export * as logical from "./logical.js";
export {
  type ColumnMatch,
  isSameColumnAnyLevel,
  isSameColumnAtLevel,
  isSameExpression,
} from "./same-expression.js";
export {
  childrenOf,
  isSubqueryExpression,
  nodesOf,
  type Rebuild,
  rebuildChildren,
} from "./walk.js";
export * as physical from "./physical.js";
export type * from "./statement.js";
export { formatPhysicalPlan } from "./format-physical.js";
