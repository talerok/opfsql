import type { ColumnInfo, IndexInfo, Schema, TableInfo } from "./api/index.js";
import type {
  ColumnDef as StoredColumn,
  ICatalog,
  IndexDef as StoredIndex,
  IndexExpression,
} from "../store/types.js";

/** The public description of a schema: no pages, no internal indexes. */
export function describeSchema(catalog: ICatalog): Schema {
  return {
    tables: catalog.getAllTables().map((table): TableInfo => ({
      name: table.name,
      columns: table.columns.map((column) => describeColumn(column)),
      indexes: catalog
        .getTableIndexes(table.name)
        .filter((index) => !isInternal(index))
        .map((index) => describeIndex(index)),
    })),
  };
}

function describeColumn(column: StoredColumn): ColumnInfo {
  return {
    name: column.name,
    type: column.type,
    nullable: column.nullable,
    primaryKey: column.primaryKey,
    unique: column.unique,
    autoIncrement: column.autoIncrement,
    defaultValue: column.defaultValue,
  };
}

/** Indexes opfsql makes for primary keys; the columns already say so. */
function isInternal(index: StoredIndex): boolean {
  return index.rowid === true || index.name.startsWith("__pk_");
}

function describeIndex(index: StoredIndex): IndexInfo {
  return {
    name: index.name,
    unique: index.unique,
    expressions: index.expressions.map((expression) =>
      formatExpression(expression),
    ),
  };
}

type OperatorExpression = Extract<IndexExpression, { type: "operator" }>;
type ComparisonExpression = Extract<IndexExpression, { type: "comparison" }>;
type CaseExpression = Extract<IndexExpression, { type: "case" }>;
type JsonAccessExpression = Extract<IndexExpression, { type: "json_access" }>;

/** An index key part as SQL text. */
export function formatExpression(expression: IndexExpression): string {
  switch (expression.type) {
    case "column": {
      return expression.name;
    }
    case "json_access": {
      return formatJsonAccess(expression);
    }
    case "function": {
      const parts = expression.args.map((part) => formatExpression(part));
      return `${expression.name}(${parts.join(", ")})`;
    }
    case "cast": {
      return `CAST(${formatExpression(expression.child)} AS ${expression.castType})`;
    }
    case "operator": {
      return formatOperator(expression);
    }
    case "comparison": {
      const symbol = comparisonSymbol(expression.comparisonType);
      return `${operand(expression.left)} ${symbol} ${operand(expression.right)}`;
    }
    case "case": {
      return formatCase(expression);
    }
    case "constant": {
      return formatConstant(expression.value);
    }
  }
}

function formatJsonAccess(expression: JsonAccessExpression): string {
  let text = expression.column;
  for (const step of expression.path) {
    text += step.type === "field" ? `.${step.name}` : `[${String(step.value)}]`;
  }
  return text;
}

function formatOperator(expression: OperatorExpression): string {
  const [subject = "", ...rest] = expression.args.map((part) => operand(part));
  switch (expression.operatorType) {
    case "NOT": {
      return `NOT ${subject}`;
    }
    case "NEGATE": {
      return `-${subject}`;
    }
    case "IS_NULL": {
      return `${subject} IS NULL`;
    }
    case "IS_NOT_NULL": {
      return `${subject} IS NOT NULL`;
    }
    case "IN": {
      return `${subject} IN (${rest.join(", ")})`;
    }
    case "NOT_IN": {
      return `${subject} NOT IN (${rest.join(", ")})`;
    }
    case "ADD":
    case "SUBTRACT":
    case "MULTIPLY":
    case "DIVIDE":
    case "MOD":
    case "CONCAT": {
      const symbol = arithmeticSymbol(expression.operatorType);
      return [subject, ...rest].join(` ${symbol} `);
    }
  }
}

function arithmeticSymbol(
  operator: Extract<
    OperatorExpression["operatorType"],
    "ADD" | "SUBTRACT" | "MULTIPLY" | "DIVIDE" | "MOD" | "CONCAT"
  >,
): string {
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
    case "CONCAT": {
      return "||";
    }
  }
}

function comparisonSymbol(
  comparison: ComparisonExpression["comparisonType"],
): string {
  switch (comparison) {
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

function formatCase(expression: CaseExpression): string {
  const checks = expression.checks.map(
    (check) =>
      `WHEN ${formatExpression(check.when)} THEN ${formatExpression(check.then)}`,
  );
  const otherwise =
    expression.elseExpr === null
      ? ""
      : ` ELSE ${formatExpression(expression.elseExpr)}`;
  return `CASE ${checks.join(" ")}${otherwise} END`;
}

/** A sub-expression, in parentheses where precedence could misread it. */
function operand(expression: IndexExpression): string {
  const text = formatExpression(expression);
  return expression.type === "operator" || expression.type === "comparison"
    ? `(${text})`
    : text;
}

function formatConstant(value: string | number | boolean | null): string {
  if (value === null) return "NULL";
  if (typeof value === "string") return `'${value.replaceAll("'", "''")}'`;
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  return String(value);
}
