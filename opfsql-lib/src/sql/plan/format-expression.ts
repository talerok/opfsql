import type { JsonPathStep } from "../../functions/index.js";
import {
  canonicalJson,
  type JsonComposite,
  realLiteral,
  sqlLiteral,
  type Value,
  type StaticType,
  type ArithmeticOperator,
} from "../../values/index.js";
import { above, power } from "../precedence.js";
import type {
  AggregateCall,
  Case,
  ColumnReference,
  ComparisonOperator,
  Expression,
  SubqueryExpression,
} from "./expression.js";
import { comparisonPower, powerOf } from "./printed-power.js";

// ---------------------------------------------------------------------------
// Expressions as SQL text: the one expression printer of both plan printers
// (format-logical.ts, format-physical.ts). An operand is parenthesized only
// where the parser's precedence (sql/precedence.ts) needs it, so with
// columns printed as SQL names the text parses back to the same tree; a NOT
// over IN, IS NULL or BETWEEN reads as SQL writes it, `x NOT IN …`, and
// constants as literals that read back as the same value. What differs
// between plans is how a column and a subquery read: each plan printer
// gives its style.
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

/** Conditions as their conjunction: `a AND b`, an OR among several parenthesized. */
export function formatConditions<Plan>(
  conditions: readonly Expression<Plan>[],
  style: ExpressionStyle<Plan>,
): string {
  const printer = new ExpressionPrinter(style);
  const [only] = conditions;
  if (only !== undefined && conditions.length === 1) return printer.print(only);
  const level = above(power.and);
  return conditions
    .map((condition) => printer.operand(condition, level))
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
        const path = formatPath(expression.path);
        return this.style.column(expression.column) + path;
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

  /** `expression`, in parentheses unless it binds at least as tightly as `minimum`. */
  operand(expression: Expression<Plan>, minimum: number): string {
    const text = this.print(expression);
    return powerOf(expression) >= minimum ? text : `(${text})`;
  }

  list(expressions: readonly Expression<Plan>[]): string {
    return expressions.map((expression) => this.print(expression)).join(", ");
  }

  private operation(expression: Operation<Plan>): string {
    switch (expression.kind) {
      case "comparison": {
        const { left, operator, right } = expression;
        const level = comparisonPower(operator);
        return this.binary(left, comparisonSymbol(operator), right, level);
      }
      case "arithmetic": {
        const { left, operator, right } = expression;
        const level = powerOf(expression);
        return this.binary(left, arithmeticSymbol(operator), right, level);
      }
      case "concatenate": {
        const { left, right } = expression;
        return this.binary(left, "||", right, power.concatenation);
      }
      case "conjunction": {
        const level = powerOf(expression);
        const operands = expression.operands.map((operand) =>
          this.operand(operand, above(level)),
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

  /** A left-associative operator: its right side binds tighter than it. */
  private binary(
    left: Expression<Plan>,
    symbol: string,
    right: Expression<Plan>,
    level: number,
  ): string {
    const leftText = this.operand(left, level);
    return `${leftText} ${symbol} ${this.operand(right, above(level))}`;
  }

  private unary(
    expression: Extract<
      Operation<Plan>,
      { readonly kind: "not" | "is-null" | "negate" | "in-list" | "between" }
    >,
  ): string {
    switch (expression.kind) {
      case "not": {
        return this.negation(expression.operand);
      }
      case "is-null": {
        return `${this.operand(expression.operand, power.equality)} IS NULL`;
      }
      case "negate": {
        return `-${this.negated(expression.operand)}`;
      }
      case "in-list": {
        return this.inList(expression, "IN");
      }
      case "between": {
        return this.between(expression, "BETWEEN");
      }
    }
  }

  /** NOT IN, IS NOT NULL and NOT BETWEEN as SQL writes them; any other NOT before its operand. */
  private negation(operand: Expression<Plan>): string {
    if (operand.kind === "in-list") return this.inList(operand, "NOT IN");
    if (operand.kind === "between") return this.between(operand, "NOT BETWEEN");
    if (operand.kind === "is-null") {
      return `${this.operand(operand.operand, power.equality)} IS NOT NULL`;
    }
    return `NOT ${this.operand(operand, power.not)}`;
  }

  /**
   * The operand of a prefix `-`. A number goes in parentheses, since `-5`
   * reads back as a literal of its own, and so does a second `-`, since
   * `--` starts a comment.
   */
  private negated(operand: Expression<Plan>): string {
    const text = this.print(operand);
    const isNumber =
      operand.kind === "constant" && typeof operand.value === "number";
    const isBare =
      !isNumber && operand.kind !== "negate" && powerOf(operand) >= power.unary;
    return isBare ? text : `(${text})`;
  }

  private inList(
    expression: Extract<Expression<Plan>, { readonly kind: "in-list" }>,
    keyword: "IN" | "NOT IN",
  ): string {
    const needle = this.operand(expression.needle, power.equality);
    return `${needle} ${keyword} (${this.list(expression.list)})`;
  }

  private between(
    expression: Extract<Expression<Plan>, { readonly kind: "between" }>,
    keyword: "BETWEEN" | "NOT BETWEEN",
  ): string {
    const bound = above(power.equality);
    const input = this.operand(expression.input, power.equality);
    const lower = this.operand(expression.lower, bound);
    const upper = this.operand(expression.upper, bound);
    return `${input} ${keyword} ${lower} AND ${upper}`;
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

  private subquery(expression: SubqueryExpression<Plan>): string {
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
        const level = comparisonPower(expression.operator);
        const needle = this.operand(expression.needle, level);
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
