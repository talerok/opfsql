import {
  DatatypeMismatchError,
  NotSupportedError,
  SemanticError,
} from "../../errors.js";
import type { Column } from "../../schema/index.js";
import {
  ExpressionClass,
  childExpressions,
  type ColumnConstraint,
  type ColumnDefinition,
  type ParsedExpression,
} from "../parser/index.js";
import { assignValue, type SqlType, type Value } from "../../values/index.js";
import { declaredType } from "./type-rules.js";

// ---------------------------------------------------------------------------
// A column as CREATE TABLE and ALTER TABLE ADD COLUMN declare it: its type
// (an unknown name is an error, none is ANY), its constraints as written,
// and its DEFAULT — a constant, converted to the column's type now, so a
// default that does not fit fails when the column is declared.
// ---------------------------------------------------------------------------

export interface DeclaredColumn {
  readonly column: Column;
  /** PRIMARY KEY written with the column, how many times. */
  readonly primaryKeys: number;
  readonly isUnique: boolean;
  readonly isAutoincrement: boolean;
}

export function declareColumn(definition: ColumnDefinition): DeclaredColumn {
  const { constraints, name } = definition;
  const type = declaredType(definition.type);
  const has = (kind: ColumnConstraint["type"]): boolean =>
    constraints.some((constraint) => constraint.type === kind);
  const column: Column = {
    name,
    type,
    isNotNull: has("NOT NULL"),
    defaultValue: defaultOf(definition, type),
  };
  return {
    column,
    primaryKeys: constraints.filter((item) => item.type === "PRIMARY KEY")
      .length,
    isUnique: has("UNIQUE"),
    isAutoincrement: has("AUTOINCREMENT"),
  };
}

/** The last DEFAULT, as in SQLite, converted to `type`; NULL without one. */
function defaultOf(definition: ColumnDefinition, type: SqlType): Value {
  const written = definition.constraints.findLast(
    (constraint) => constraint.type === "DEFAULT",
  );
  if (written?.type !== "DEFAULT") return null;
  const value = constantOf(definition.name, written.value);
  try {
    return assignValue(value, type);
  } catch (error) {
    if (!(error instanceof DatatypeMismatchError)) throw error;
    throw new DatatypeMismatchError(
      `the DEFAULT of column ${definition.name}: ${error.message}`,
      { cause: error },
    );
  }
}

/** A DEFAULT's value: a literal; any other constant is not supported yet. */
function constantOf(column: string, expression: ParsedExpression): Value {
  if (!isConstant(expression)) {
    throw new SemanticError(`the DEFAULT of column ${column} is not constant`);
  }
  if (expression.expressionClass !== ExpressionClass.CONSTANT) {
    throw new NotSupportedError("a DEFAULT other than a literal");
  }
  return expression.value;
}

function isConstant(expression: ParsedExpression): boolean {
  switch (expression.expressionClass) {
    case ExpressionClass.COLUMN_REF:
    case ExpressionClass.PARAMETER:
    case ExpressionClass.SUBQUERY:
    case ExpressionClass.STAR: {
      return false;
    }
    case ExpressionClass.CONSTANT:
    case ExpressionClass.COMPARISON:
    case ExpressionClass.CONJUNCTION:
    case ExpressionClass.OPERATOR:
    case ExpressionClass.BETWEEN:
    case ExpressionClass.FUNCTION:
    case ExpressionClass.CASE:
    case ExpressionClass.CAST: {
      return childExpressions(expression).every((child) => isConstant(child));
    }
  }
}

/** A PRIMARY KEY, UNIQUE or index key over a BLOB or JSON column is not supported. */
export function checkKeyType(type: SqlType, what: string): void {
  if (type === "BLOB" || type === "JSON") {
    throw new NotSupportedError(`${what} on a ${type} column`);
  }
}
