import { describe, expect, it } from "vitest";
import { InternalError } from "../../../errors.js";
import { kindOf, valueKey } from "../../../values/index.js";
import {
  childrenOf,
  type Expression,
  formatPhysicalPlan,
  type physical,
} from "../../plan/index.js";
import { naive } from "./naive.js";
import { booleanExpression, number } from "./random-expressions.js";
import { Random } from "./random.js";
import { boundPlan } from "./sql.js";

// The expression printer prints by precedence, and what it prints parses
// back to the tree it printed: random expressions, printed with columns as
// SQL names, parsed and bound again, have the shape they had. Types are the
// binder's to decide and are not compared, except a constant's, which its
// literal must keep (2.0 stays REAL).

const SEED = 20_261_027;
const CASES = 400;

/** The expression printed as the physical plan printer prints it: columns by name. */
function printed(expression: Expression<physical.Plan>): string {
  const plan: physical.Values = {
    kind: "values",
    tableIndex: 9,
    rows: [[expression]],
    types: [expression.type],
  };
  const line = formatPhysicalPlan(plan);
  const prefix = "values#9 (";
  if (!line.startsWith(prefix) || !line.endsWith(")")) {
    throw new InternalError(`not a one-value row: ${line}`);
  }
  return line.slice(prefix.length, -1);
}

/** The expression `text` binds to, as a select item over users. */
function reparsed(text: string): Expression<unknown> {
  const plan = boundPlan(`SELECT ${text} FROM users u`);
  if (plan.kind !== "project") throw new InternalError(`a project: ${text}`);
  const [expression] = plan.expressions;
  if (expression === undefined) throw new InternalError(`no item: ${text}`);
  return expression;
}

/** A tree's shape: kinds, operators, columns by position, constants by value and type. */
function shapeOf<Plan>(expression: Expression<Plan>): string {
  const parts = [
    ownOf(expression),
    ...childrenOf(expression).map((child) => shapeOf(child)),
  ];
  return `(${parts.join(" ")})`;
}

function ownOf<Plan>(expression: Expression<Plan>): string {
  switch (expression.kind) {
    case "column": {
      return `column ${String(expression.binding.columnIndex)}`;
    }
    case "constant": {
      const { value, type } = expression;
      return `constant ${kindOf(value)} ${valueKey(value)} ${type}`;
    }
    case "parameter": {
      return `parameter ${String(expression.index)}`;
    }
    case "comparison":
    case "arithmetic":
    case "conjunction": {
      return `${expression.kind} ${expression.operator}`;
    }
    case "function": {
      return `function ${expression.function.name}`;
    }
    case "cast": {
      return `cast ${expression.type}`;
    }
    case "quantified-comparison": {
      return `${expression.kind} ${expression.operator} ${expression.quantifier}`;
    }
    case "json-access":
    case "not":
    case "is-null":
    case "negate":
    case "concatenate":
    case "in-list":
    case "between":
    case "case":
    case "scalar-subquery":
    case "exists": {
      return expression.kind;
    }
  }
}

describe("printed expressions parse back to the trees they came from", () => {
  it("for random conditions and numbers, every operator nested in every order", () => {
    const random = new Random(SEED);
    for (let count = 0; count < CASES; count += 1) {
      const expression = random.chance(0.7)
        ? booleanExpression(random, 4)
        : number(random, 4);
      const text = printed(naive(expression));
      expect(shapeOf(reparsed(text)), text).toBe(shapeOf(expression));
    }
  });
});
