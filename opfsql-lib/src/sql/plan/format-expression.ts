import { bytesToHex, canonicalJson, type Value } from "../../values/index.js";
import type {
  AggregateCall,
  ArithmeticOperator,
  Case,
  ComparisonOperator,
  Expression,
  JsonPathStep,
  OrderKey,
} from "./expression.js";

// ---------------------------------------------------------------------------
// Expressions as SQL-like text, for plans printed by EXPLAIN and tests.
// Columns print as written; a subquery prints as the label its caller gives
// it, since its plan prints elsewhere. Parentheses appear only where the
// operator precedence below needs them.
// ---------------------------------------------------------------------------

/** What a subquery prints as, inside the expression that holds it. */
export type SubqueryLabel<Plan> = (plan: Plan, isCorrelated: boolean) => string;

/** Binding strength, loosest first: a child looser than its parent is parenthesized. */
const PRECEDENCE = {
  or: 1,
  and: 2,
  not: 3,
  comparison: 4,
  additive: 5,
  multiplicative: 6,
  concatenate: 7,
  unary: 8,
  atom: 9,
} as const;

/** The symbol SQL writes for a comparison. */
export function comparisonSymbol(operator: ComparisonOperator): string {
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

export function formatExpression<Plan>(
  expression: Expression<Plan>,
  label: SubqueryLabel<Plan>,
): string {
  return new ExpressionPrinter(label).print(expression);
}

/** Conditions as their conjunction: `a AND b`, an OR among several parenthesized. */
export function formatConditions<Plan>(
  conditions: readonly Expression<Plan>[],
  label: SubqueryLabel<Plan>,
): string {
  const printer = new ExpressionPrinter(label);
  const [only] = conditions;
  if (only !== undefined && conditions.length === 1) return printer.print(only);
  return conditions
    .map((condition) => printer.operand(condition, PRECEDENCE.and))
    .join(" AND ");
}

/** `name(DISTINCT a, b)`, or `count(*)`. */
export function formatAggregateCall<Plan>(
  call: AggregateCall<Plan>,
  label: SubqueryLabel<Plan>,
): string {
  const printer = new ExpressionPrinter(label);
  const parts = call.arguments.map((argument) => printer.print(argument));
  const list = parts.length === 0 ? "*" : parts.join(", ");
  return `${call.name}(${call.isDistinct ? "DISTINCT " : ""}${list})`;
}

/** `expression ASC`, with NULLS FIRST / LAST only where it is not the default. */
export function formatOrderKey<Plan>(
  key: OrderKey<Plan>,
  label: SubqueryLabel<Plan>,
): string {
  const isAscending = key.direction === "ascending";
  const direction = isAscending ? "ASC" : "DESC";
  const defaultNulls = isAscending ? "first" : "last";
  const nulls =
    key.nulls === defaultNulls ? "" : ` NULLS ${key.nulls.toUpperCase()}`;
  return `${formatExpression(key.expression, label)} ${direction}${nulls}`;
}

/** A value as a SQL literal; JSON as `JSON '…'`, so it never reads as text. */
export function formatValue(value: Value): string {
  if (value === null) return "NULL";
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (typeof value === "number") return String(value);
  if (typeof value === "string") return quote(value);
  if (value instanceof Uint8Array) return `X'${bytesToHex(value)}'`;
  return `JSON ${quote(canonicalJson(value))}`;
}

function quote(text: string): string {
  return `'${text.replaceAll("'", "''")}'`;
}

class ExpressionPrinter<Plan> {
  constructor(private readonly label: SubqueryLabel<Plan>) {}

  print(expression: Expression<Plan>): string {
    switch (expression.kind) {
      case "column": {
        return expression.name;
      }
      case "constant": {
        return formatValue(expression.value);
      }
      case "parameter": {
        return `$${String(expression.index + 1)}`;
      }
      case "json-access": {
        return expression.column.name + formatPath(expression.path);
      }
      case "comparison":
      case "conjunction":
      case "not":
      case "is-null":
      case "in-list":
      case "between": {
        return this.condition(expression);
      }
      case "arithmetic":
      case "negate":
      case "concatenate": {
        return this.operation(expression);
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
    }
  }

  /** `expression` as an operand of an operator of strength `parent`. */
  operand(expression: Expression<Plan>, parent: number): string {
    const text = this.print(expression);
    return precedenceOf(expression) < parent ? `(${text})` : text;
  }

  private condition(
    expression: Extract<
      Expression<Plan>,
      {
        readonly kind:
          | "comparison"
          | "conjunction"
          | "not"
          | "is-null"
          | "in-list"
          | "between";
      }
    >,
  ): string {
    const tight = PRECEDENCE.comparison + 1;
    switch (expression.kind) {
      case "comparison": {
        const symbol = comparisonSymbol(expression.operator);
        const left = this.operand(expression.left, tight);
        return `${left} ${symbol} ${this.operand(expression.right, tight)}`;
      }
      case "conjunction": {
        const strength =
          expression.operator === "AND" ? PRECEDENCE.and : PRECEDENCE.or;
        const operands = expression.operands.map((operand) =>
          this.operand(operand, strength + 1),
        );
        return operands.join(` ${expression.operator} `);
      }
      case "not": {
        return this.negation(expression.operand);
      }
      case "is-null": {
        return `${this.operand(expression.operand, tight)} IS NULL`;
      }
      case "in-list": {
        return `${this.operand(expression.needle, tight)} IN (${this.list(expression.list)})`;
      }
      case "between": {
        return this.between(expression, "BETWEEN");
      }
    }
  }

  /** NOT IN, IS NOT NULL, NOT BETWEEN and NOT EXISTS read as SQL writes them. */
  private negation(operand: Expression<Plan>): string {
    const tight = PRECEDENCE.comparison + 1;
    if (operand.kind === "in-list") {
      const needle = this.operand(operand.needle, tight);
      return `${needle} NOT IN (${this.list(operand.list)})`;
    }
    if (operand.kind === "is-null") {
      return `${this.operand(operand.operand, tight)} IS NOT NULL`;
    }
    if (operand.kind === "between") return this.between(operand, "NOT BETWEEN");
    if (operand.kind === "exists") return `NOT ${this.subquery(operand)}`;
    return `NOT ${this.operand(operand, PRECEDENCE.not)}`;
  }

  private between(
    expression: Extract<Expression<Plan>, { readonly kind: "between" }>,
    keyword: string,
  ): string {
    const tight = PRECEDENCE.comparison + 1;
    const input = this.operand(expression.input, tight);
    const lower = this.operand(expression.lower, tight);
    const upper = this.operand(expression.upper, tight);
    return `${input} ${keyword} ${lower} AND ${upper}`;
  }

  private operation(
    expression: Extract<
      Expression<Plan>,
      { readonly kind: "arithmetic" | "negate" | "concatenate" }
    >,
  ): string {
    if (expression.kind === "negate") {
      return `-${this.operand(expression.operand, PRECEDENCE.unary)}`;
    }
    const strength = precedenceOf(expression);
    const symbol =
      expression.kind === "concatenate"
        ? "||"
        : arithmeticSymbol(expression.operator);
    // Left-associative: an equal operator on the right needs parentheses.
    const left = this.operand(expression.left, strength);
    const right = this.operand(expression.right, strength + 1);
    return `${left} ${symbol} ${right}`;
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

  private subquery(
    expression: Extract<
      Expression<Plan>,
      { readonly kind: "scalar-subquery" | "exists" | "quantified-comparison" }
    >,
  ): string {
    const name = this.label(expression.plan, expression.isCorrelated);
    switch (expression.kind) {
      case "scalar-subquery": {
        return `(${name})`;
      }
      case "exists": {
        return `EXISTS (${name})`;
      }
      case "quantified-comparison": {
        const needle = this.operand(
          expression.needle,
          PRECEDENCE.comparison + 1,
        );
        const symbol = comparisonSymbol(expression.operator);
        return `${needle} ${symbol} ${expression.quantifier} (${name})`;
      }
    }
  }

  private list(expressions: readonly Expression<Plan>[]): string {
    return expressions.map((expression) => this.print(expression)).join(", ");
  }
}

function precedenceOf<Plan>(expression: Expression<Plan>): number {
  switch (expression.kind) {
    case "conjunction": {
      return expression.operator === "AND" ? PRECEDENCE.and : PRECEDENCE.or;
    }
    case "not": {
      return PRECEDENCE.not;
    }
    case "comparison":
    case "is-null":
    case "in-list":
    case "between":
    case "quantified-comparison": {
      return PRECEDENCE.comparison;
    }
    case "arithmetic": {
      return additiveOrNot(expression.operator);
    }
    case "concatenate": {
      return PRECEDENCE.concatenate;
    }
    case "negate": {
      return PRECEDENCE.unary;
    }
    case "column":
    case "constant":
    case "parameter":
    case "json-access":
    case "function":
    case "case":
    case "cast":
    case "scalar-subquery":
    case "exists": {
      return PRECEDENCE.atom;
    }
  }
}

function additiveOrNot(operator: ArithmeticOperator): number {
  const isAdditive = operator === "ADD" || operator === "SUBTRACT";
  return isAdditive ? PRECEDENCE.additive : PRECEDENCE.multiplicative;
}

function formatPath(path: readonly JsonPathStep[]): string {
  return path
    .map((step) =>
      step.kind === "key" ? `.${step.key}` : `[${String(step.index)}]`,
    )
    .join("");
}
