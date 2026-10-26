import type { StaticType } from "../../functions/index.js";
import {
  canonicalJson,
  realLiteral,
  sqlLiteral,
  type JsonComposite,
  type Value,
} from "../../values/index.js";
import {
  ROWID_COLUMN,
  type ArithmeticOperator,
  type ColumnReference,
  type ComparisonOperator,
  type JsonPathStep,
} from "./expression.js";
import type { Expression } from "./logical.js";

// ---------------------------------------------------------------------------
// The terms a printed plan is made of (format-logical.ts): columns,
// constants, operators, and which expressions print without parentheses.
// ---------------------------------------------------------------------------

/** Names, literals and calls: what needs no parentheses as an operand. */
export function isAtomic(expression: Expression): boolean {
  switch (expression.kind) {
    case "column":
    case "constant":
    case "parameter":
    case "function":
    case "case":
    case "cast":
    case "json-access":
    case "scalar-subquery": {
      return true;
    }
    case "comparison":
    case "conjunction":
    case "not":
    case "is-null":
    case "arithmetic":
    case "negate":
    case "concatenate":
    case "in-list":
    case "between":
    case "exists":
    case "quantified-comparison": {
      return false;
    }
  }
}

export function comparisonText(operator: ComparisonOperator): string {
  switch (operator) {
    case "EQUAL": {
      return "=";
    }
    case "NOT_EQUAL": {
      return "<>";
    }
    case "LESS": {
      return "<";
    }
    case "GREATER": {
      return ">";
    }
    case "LESS_EQUAL": {
      return "<=";
    }
    case "GREATER_EQUAL": {
      return ">=";
    }
  }
}

export function arithmeticText(operator: ArithmeticOperator): string {
  switch (operator) {
    case "ADD": {
      return "+";
    }
    case "SUBTRACT": {
      return "-";
    }
    case "MULTIPLY": {
      return "*";
    }
    case "DIVIDE": {
      return "/";
    }
    case "MOD": {
      return "%";
    }
  }
}

export function formatColumn(reference: ColumnReference): string {
  const { tableIndex, columnIndex } = reference.binding;
  const column = columnIndex === ROWID_COLUMN ? "rowid" : String(columnIndex);
  const depth = reference.depth === 0 ? "" : `^${String(reference.depth)}`;
  return `${reference.name}#${String(tableIndex)}.${column}${depth}`;
}

export function formatStep(step: JsonPathStep): string {
  return step.kind === "key" ? `.${step.key}` : `[${String(step.index)}]`;
}

/** A constant as SQL writes it; a REAL keeps its decimal point, JSON is marked. */
export function formatValue(value: Value, type: StaticType): string {
  if (typeof value === "number" && type === "REAL") return realLiteral(value);
  if (isJsonComposite(value)) return `JSON '${canonicalJson(value)}'`;
  return sqlLiteral(value);
}

function isJsonComposite(value: Value): value is JsonComposite {
  return (
    typeof value === "object" &&
    value !== null &&
    !(value instanceof Uint8Array)
  );
}
