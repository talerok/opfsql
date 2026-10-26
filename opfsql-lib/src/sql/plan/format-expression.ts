import type { StaticType } from "../../functions/index.js";
import {
  canonicalJson,
  type JsonComposite,
  realLiteral,
  sqlLiteral,
  type Value,
} from "../../values/index.js";
import type {
  AggregateCall,
  ArithmeticOperator,
  Case,
  ColumnReference,
  ComparisonOperator,
  Expression,
  JsonPathStep,
} from "./expression.js";

// ---------------------------------------------------------------------------
// Expressions as SQL-like text: the one expression printer of both plan
// printers (format-logical.ts, format-physical.ts). An operand that has
// operators of its own is parenthesized, so a printed condition never
// depends on precedence; constants print as literals that read back as the
// same value. What differs between plans is how a column and a subquery
// read, which each plan printer gives as its style.
// ---------------------------------------------------------------------------

/** What a plan printer decides: how a column and a subquery reference read. */
export interface ExpressionStyle<Plan> {
  readonly column: (reference: ColumnReference) => string;
  /** The reference to a subquery's plan, which prints elsewhere; atomic. */
  readonly subquery: (plan: Plan, isCorrelated: boolean) => string;
}

export function formatExpression<Plan>(
  expression: Expression<Plan>,
  style: ExpressionStyle<Plan>,
): string {
  return new ExpressionPrinter(style).print(expression);
}

/** Conditions as their conjunction, each one parenthesized if it has operators. */
export function formatConditions<Plan>(
  conditions: readonly Expression<Plan>[],
  style: ExpressionStyle<Plan>,
): string {
  const printer = new ExpressionPrinter(style);
  return conditions
    .map((condition) => printer.operand(condition))
    .join(" AND ");
}

/** `name(DISTINCT a, b)`, or `count(*)`. */
export function formatAggregateCall<Plan>(
  call: AggregateCall<Plan>,
  style: ExpressionStyle<Plan>,
): string {
  if (call.arguments.length === 0) return `${call.name}(*)`;
  const printer = new ExpressionPrinter(style);
  const distinct = call.isDistinct ? "DISTINCT " : "";
  return `${call.name}(${distinct}${printer.list(call.arguments)})`;
}

/**
 * A constant as SQL writes it: a REAL keeps its decimal point, so it reads
 * back as REAL; a JSON object or array, which has no literal, is marked.
 */
function formatValue(value: Value, type: StaticType): string {
  if (typeof value === "number" && type === "REAL") return realLiteral(value);
  if (isJsonComposite(value)) return `JSON ${sqlLiteral(canonicalJson(value))}`;
  return sqlLiteral(value);
}

function isJsonComposite(value: Value): value is JsonComposite {
  const isObject = typeof value === "object" && value !== null;
  return isObject && !(value instanceof Uint8Array);
}

/** Names, literals and calls: what needs no parentheses as an operand. */
function isAtomic<Plan>(expression: Expression<Plan>): boolean {
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

type Operation<Plan> = Extract<
  Expression<Plan>,
  {
    readonly kind:
      | "comparison"
      | "conjunction"
      | "arithmetic"
      | "concatenate"
      | "not"
      | "is-null"
      | "negate"
      | "in-list"
      | "between";
  }
>;

type Subquery<Plan> = Extract<
  Expression<Plan>,
  { readonly kind: "scalar-subquery" | "exists" | "quantified-comparison" }
>;

class ExpressionPrinter<Plan> {
  constructor(private readonly style: ExpressionStyle<Plan>) {}

  print(expression: Expression<Plan>): string {
    switch (expression.kind) {
      case "column": {
        return this.style.column(expression);
      }
      case "constant": {
        return formatValue(expression.value, expression.type);
      }
      case "parameter": {
        return `$${String(expression.index + 1)}`;
      }
      case "json-access": {
        return (
          this.style.column(expression.column) + formatPath(expression.path)
        );
      }
      case "function": {
        return `${expression.name}(${this.list(expression.arguments)})`;
      }
      case "case": {
        return this.caseOf(expression);
      }
      case "cast": {
        return `CAST(${this.print(expression.operand)} AS ${expression.type})`;
      }
      case "scalar-subquery":
      case "exists":
      case "quantified-comparison": {
        return this.subquery(expression);
      }
      case "comparison":
      case "conjunction":
      case "arithmetic":
      case "concatenate":
      case "not":
      case "is-null":
      case "negate":
      case "in-list":
      case "between": {
        return this.operation(expression);
      }
    }
  }

  /** `expression` as an operand: parenthesized when it has operators of its own. */
  operand(expression: Expression<Plan>): string {
    const text = this.print(expression);
    return isAtomic(expression) ? text : `(${text})`;
  }

  list(expressions: readonly Expression<Plan>[]): string {
    return expressions.map((expression) => this.print(expression)).join(", ");
  }

  private operation(expression: Operation<Plan>): string {
    switch (expression.kind) {
      case "comparison": {
        return this.binary(
          expression.left,
          comparisonSymbol(expression.operator),
          expression.right,
        );
      }
      case "arithmetic": {
        return this.binary(
          expression.left,
          arithmeticSymbol(expression.operator),
          expression.right,
        );
      }
      case "concatenate": {
        return this.binary(expression.left, "||", expression.right);
      }
      case "conjunction": {
        const operands = expression.operands.map((operand) =>
          this.operand(operand),
        );
        return operands.join(` ${expression.operator} `);
      }
      case "not":
      case "is-null":
      case "negate":
      case "in-list":
      case "between": {
        return this.unary(expression);
      }
    }
  }

  private binary(
    left: Expression<Plan>,
    symbol: string,
    right: Expression<Plan>,
  ): string {
    return `${this.operand(left)} ${symbol} ${this.operand(right)}`;
  }

  private unary(
    expression: Extract<
      Operation<Plan>,
      { readonly kind: "not" | "is-null" | "negate" | "in-list" | "between" }
    >,
  ): string {
    switch (expression.kind) {
      case "not": {
        return `NOT ${this.operand(expression.operand)}`;
      }
      case "is-null": {
        return `${this.operand(expression.operand)} IS NULL`;
      }
      case "negate": {
        // `--` starts a comment in SQL: a negative operand gets parentheses.
        const operand = this.operand(expression.operand);
        return operand.startsWith("-") ? `-(${operand})` : `-${operand}`;
      }
      case "in-list": {
        return `${this.operand(expression.needle)} IN (${this.list(expression.list)})`;
      }
      case "between": {
        const { input, lower, upper } = expression;
        return `${this.operand(input)} BETWEEN ${this.operand(lower)} AND ${this.operand(upper)}`;
      }
    }
  }

  private caseOf(expression: Case<Plan>): string {
    const parts = ["CASE"];
    if (expression.operand !== null) parts.push(this.print(expression.operand));
    for (const { when, result } of expression.branches) {
      parts.push(`WHEN ${this.print(when)} THEN ${this.print(result)}`);
    }
    if (expression.otherwise !== null) {
      parts.push(`ELSE ${this.print(expression.otherwise)}`);
    }
    parts.push("END");
    return parts.join(" ");
  }

  private subquery(expression: Subquery<Plan>): string {
    const reference = this.style.subquery(
      expression.plan,
      expression.isCorrelated,
    );
    switch (expression.kind) {
      case "scalar-subquery": {
        return reference;
      }
      case "exists": {
        return `EXISTS ${reference}`;
      }
      case "quantified-comparison": {
        const needle = this.operand(expression.needle);
        const symbol = comparisonSymbol(expression.operator);
        return `${needle} ${symbol} ${expression.quantifier} ${reference}`;
      }
    }
  }
}

function comparisonSymbol(operator: ComparisonOperator): string {
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
    case "LESS_EQUAL": {
      return "<=";
    }
    case "GREATER": {
      return ">";
    }
    case "GREATER_EQUAL": {
      return ">=";
    }
  }
}

function arithmeticSymbol(operator: ArithmeticOperator): string {
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

function formatPath(path: readonly JsonPathStep[]): string {
  return path.map((step) => formatStep(step)).join("");
}

const PLAIN_NAME = /^[A-Za-z_]\w*$/u;

/** `.key`, `."a key"` when the key is not a plain name, or `[0]`. */
function formatStep(step: JsonPathStep): string {
  if (step.kind === "index") return `[${String(step.index)}]`;
  if (PLAIN_NAME.test(step.key)) return `.${step.key}`;
  return `."${step.key.replaceAll('"', '""')}"`;
}
