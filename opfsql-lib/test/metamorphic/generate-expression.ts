import { BOOLEANS, INTEGERS, PATTERNS, TEXTS } from "./data.js";
import {
  areComparable,
  constant,
  type Expression,
  type ExpressionType,
  type Leaf,
  leaf,
  node,
  print,
} from "./expression.js";
import type { Random } from "./random.js";
import type { TableSpec } from "./schema.js";
import { tableLeaves } from "./scope.js";

// ---------------------------------------------------------------------------
// Well-typed random expressions. Every expression binds — conditions are
// BOOLEAN, comparisons compare comparable types — and none is meant to fail
// at run time: no arithmetic, function or condition takes an untyped value,
// integers stay far from overflow, and a scalar subquery is an aggregate,
// so it has one row.
// ---------------------------------------------------------------------------

/** What an expression may read. */
export interface Scope {
  /** Columns, JSON paths, or a group's key and aggregates. */
  readonly leaves: readonly Leaf[];
  /** Tables a subquery may read: none where subqueries are not allowed. */
  readonly tables: readonly TableSpec[];
}

/** Names for subqueries' tables, unique within a statement. */
export class AliasSource {
  private count = 0;

  next(): string {
    const alias = `s${String(this.count)}`;
    this.count++;
    return alias;
  }
}

type Choice = readonly [weight: number, make: (depth: number) => Expression];

const COMPARISONS = ["=", "<>", "<", "<=", ">", ">="];
const REAL_LITERALS = ["-2.5", "-0.25", "0.5", "1.5", "2.75", "13.5"];
const NULL_CHANCE = 0.03;
const LEAF_CHANCE = 0.45;
/** How often a leaf is a column rather than a constant, when there is one. */
const COLUMN_CHANCE = 0.85;

export class ExpressionGenerator {
  constructor(
    private readonly random: Random,
    private readonly scope: Scope,
    private readonly aliases: AliasSource,
  ) {}

  /** A BOOLEAN expression. */
  condition(depth: number): Expression {
    const hasBooleans = this.scope.leaves.some(
      (each) => each.type === "boolean",
    );
    if (depth <= 0) {
      const isColumn = hasBooleans && this.random.chance(0.3);
      return isColumn ? this.leafOf("boolean") : this.comparison(0);
    }
    return this.choose(depth - 1, [
      [6, (inner) => this.comparison(inner)],
      [3, (inner) => this.junction(inner)],
      [1, (inner) => node("boolean", "NOT ", this.condition(inner))],
      [2, (inner) => this.isNull(inner)],
      [1, (inner) => this.between(inner)],
      [2, (inner) => this.inList(inner)],
      [this.subqueryWeight(), (inner) => this.inSubquery(inner)],
      [this.subqueryWeight(), (inner) => this.exists(inner)],
      [1, (inner) => this.like(inner)],
      [hasBooleans ? 1 : 0, () => this.leafOf("boolean")],
    ]);
  }

  /** An expression of `type`. */
  value(type: ExpressionType, depth: number): Expression {
    if (depth <= 0 || this.random.chance(LEAF_CHANCE)) return this.leafOf(type);
    switch (type) {
      case "integer": {
        return this.integer(depth - 1);
      }
      case "real": {
        return this.real(depth - 1);
      }
      case "text": {
        return this.text(depth - 1);
      }
      case "boolean": {
        return this.condition(depth);
      }
      case "untyped": {
        return this.untyped(depth - 1);
      }
    }
  }

  /** An expression of any type there are values of. */
  anyValue(depth: number): Expression {
    // Mostly a type some column has, so that the expression reads a row.
    const columnTypes = [
      ...new Set(this.scope.leaves.map((each) => each.type)),
    ];
    const isColumnType = columnTypes.length > 0 && this.random.chance(0.8);
    const types = isColumnType ? columnTypes : this.types();
    return this.value(this.random.pick(types), depth);
  }

  /** A value `=` accepts beside one of `type`. */
  comparableTo(type: ExpressionType, depth: number): Expression {
    const types = this.types().filter((other) => areComparable(type, other));
    return this.value(this.random.pick(types), depth);
  }

  private types(): ExpressionType[] {
    const types: ExpressionType[] = ["integer", "real", "text", "boolean"];
    const hasUntyped = this.scope.leaves.some(
      (each) => each.type === "untyped",
    );
    return hasUntyped ? [...types, "untyped"] : types;
  }

  /** A column of `type`, or a constant; now and then NULL. */
  private leafOf(type: ExpressionType): Expression {
    if (this.random.chance(NULL_CHANCE)) return constant(type, "NULL");
    const columns = this.scope.leaves.filter((each) => each.type === type);
    const isColumn =
      columns.length > 0 &&
      (type === "untyped" || this.random.chance(COLUMN_CHANCE));
    if (isColumn) return this.random.pick(columns);
    return constant(type, this.literalSql(type));
  }

  private literalSql(type: ExpressionType): string {
    switch (type) {
      case "integer": {
        return this.random.pick(INTEGERS);
      }
      case "real": {
        return this.random.pick(REAL_LITERALS);
      }
      case "text": {
        return this.random.pick(TEXTS);
      }
      case "boolean": {
        return this.random.pick(BOOLEANS);
      }
      case "untyped": {
        // Only asked for when the scope has an untyped value; a constant is never untyped.
        return "NULL";
      }
    }
  }

  // --- Conditions ------------------------------------------------------------

  private comparison(depth: number): Expression {
    const left = this.anyValue(depth);
    const operator = this.random.pick(COMPARISONS);
    return node(
      "boolean",
      left,
      ` ${operator} `,
      this.comparableTo(left.type, depth),
    );
  }

  private junction(depth: number): Expression {
    const operator = this.random.chance(0.5) ? " AND " : " OR ";
    return node(
      "boolean",
      this.condition(depth),
      operator,
      this.condition(depth),
    );
  }

  private isNull(depth: number): Expression {
    const test = this.random.chance(0.5) ? " IS NULL" : " IS NOT NULL";
    return node("boolean", this.anyValue(depth), test);
  }

  private between(depth: number): Expression {
    const operand = this.anyValue(depth);
    const low = this.comparableTo(operand.type, depth);
    const high = this.comparableTo(operand.type, depth);
    const keyword = this.random.chance(0.2) ? " NOT BETWEEN " : " BETWEEN ";
    return node("boolean", operand, keyword, low, " AND ", high);
  }

  private inList(depth: number): Expression {
    const operand = this.anyValue(depth);
    const count = this.random.between(1, 4);
    const items = Array.from({ length: count }, () =>
      this.comparableTo(operand.type, 0),
    );
    const separated = items.flatMap((item, index) =>
      index === 0 ? [item] : [", ", item],
    );
    const keyword = this.random.chance(0.2) ? " NOT IN (" : " IN (";
    return node("boolean", operand, keyword, ...separated, ")");
  }

  private like(depth: number): Expression {
    const keyword = this.random.chance(0.2) ? " NOT LIKE " : " LIKE ";
    const pattern = constant("text", this.random.pick(PATTERNS));
    return node("boolean", this.value("text", depth), keyword, pattern);
  }

  // --- Values ------------------------------------------------------------------

  private integer(depth: number): Expression {
    return this.choose(depth, [
      [3, (inner) => this.arithmetic("integer", inner)],
      [
        1,
        (inner) => node("integer", "abs(", this.value("integer", inner), ")"),
      ],
      [
        1,
        (inner) => node("integer", "length(", this.value("text", inner), ")"),
      ],
      [
        1,
        (inner) =>
          node(
            "integer",
            "instr(",
            this.value("text", inner),
            ", ",
            this.leafOf("text"),
            ")",
          ),
      ],
      [
        1,
        (inner) =>
          node("integer", "CAST(", this.condition(inner), " AS INTEGER)"),
      ],
      [2, (inner) => this.choice("integer", inner)],
      [this.subqueryWeight(), (inner) => this.countSubquery(inner)],
    ]);
  }

  private real(depth: number): Expression {
    return this.choose(depth, [
      [3, (inner) => this.arithmetic("real", inner)],
      [1, (inner) => node("real", "round(", this.value("real", inner), ", 1)")],
      [
        1,
        (inner) =>
          node("real", "CAST(", this.value("integer", inner), " AS REAL)"),
      ],
      [2, (inner) => this.choice("real", inner)],
    ]);
  }

  private text(depth: number): Expression {
    const call = (name: string) => (inner: number) =>
      node("text", `${name}(`, this.value("text", inner), ")");
    return this.choose(depth, [
      [1, call("lower")],
      [1, call("upper")],
      [1, call("trim")],
      [
        1,
        (inner) =>
          node(
            "text",
            "substr(",
            this.value("text", inner),
            `, ${this.random.pick(["0", "1", "2", "-1"])}, 2)`,
          ),
      ],
      [
        1,
        (inner) =>
          node(
            "text",
            this.value("text", inner),
            " || ",
            this.value("text", inner),
          ),
      ],
      [
        1,
        (inner) =>
          node("text", "CAST(", this.value("integer", inner), " AS TEXT)"),
      ],
      [1, (inner) => node("text", "typeof(", this.anyValue(inner), ")")],
      [2, (inner) => this.choice("text", inner)],
      [this.subqueryWeight(), (inner) => this.extremeSubquery("text", inner)],
    ]);
  }

  private untyped(depth: number): Expression {
    return this.choose(depth, [
      [3, () => this.leafOf("untyped")],
      [1, (inner) => this.choice("untyped", inner)],
      [
        this.subqueryWeight(),
        (inner) => this.extremeSubquery("untyped", inner),
      ],
    ]);
  }

  /**
   * `+`, `-`, `*`, `/` or `%` of two numbers. A product takes a constant
   * factor, so that nested products stay far from overflow.
   */
  private arithmetic(type: "integer" | "real", depth: number): Expression {
    const operator = this.random.pick(["+", "-", "*", "/", "%"]);
    const left = this.value(type, depth);
    const otherType =
      type === "real" && this.random.chance(0.5) ? "integer" : type;
    const right =
      operator === "*" ? this.leafOf(otherType) : this.value(otherType, depth);
    return node(type, left, ` ${operator} `, right);
  }

  /** CASE, coalesce or nullif: one of several values of `type`. */
  private choice(type: ExpressionType, depth: number): Expression {
    switch (this.random.below(3)) {
      case 0: {
        const otherwise = this.value(type, depth);
        return node(
          type,
          "CASE WHEN ",
          this.condition(depth),
          " THEN ",
          this.value(type, depth),
          " ELSE ",
          otherwise,
          " END",
        );
      }
      case 1: {
        return node(
          type,
          "coalesce(",
          this.value(type, depth),
          ", ",
          this.value(type, depth),
          ")",
        );
      }
      default: {
        return node(
          type,
          "nullif(",
          this.value(type, depth),
          ", ",
          this.value(type, depth),
          ")",
        );
      }
    }
  }

  // --- Subqueries -------------------------------------------------------------

  private subqueryWeight(): number {
    return this.scope.tables.length > 0 ? 1 : 0;
  }

  /** `x IN (SELECT …)`, of a column comparable with `x`, or a comparison when there is none. */
  private inSubquery(depth: number): Expression {
    const operand = this.anyValue(depth);
    const table = this.random.pick(this.scope.tables);
    const alias = this.aliases.next();
    const columns = tableLeaves(table, alias).filter((column) =>
      areComparable(operand.type, column.type),
    );
    if (columns.length === 0) return this.comparison(depth);
    const where = this.subqueryWhere(table, alias, depth);
    const select = `SELECT ${this.random.pick(columns).sql} FROM ${table.name} AS ${alias}${where}`;
    const keyword = this.random.chance(0.3) ? " NOT IN (" : " IN (";
    return node("boolean", operand, keyword, leaf("untyped", select), ")");
  }

  private exists(depth: number): Expression {
    const table = this.random.pick(this.scope.tables);
    const alias = this.aliases.next();
    const where = this.subqueryWhere(table, alias, depth);
    const keyword = this.random.chance(0.3) ? "NOT EXISTS (" : "EXISTS (";
    return node(
      "boolean",
      keyword,
      leaf("untyped", `SELECT 1 FROM ${table.name} AS ${alias}${where}`),
      ")",
    );
  }

  private countSubquery(depth: number): Expression {
    const table = this.random.pick(this.scope.tables);
    const alias = this.aliases.next();
    const where = this.subqueryWhere(table, alias, depth);
    return leaf(
      "integer",
      `(SELECT count(*) FROM ${table.name} AS ${alias}${where})`,
    );
  }

  /** `(SELECT max(col) …)` of a column of `type`, or a value of `type` when there is none. */
  private extremeSubquery(type: ExpressionType, depth: number): Expression {
    const table = this.random.pick(this.scope.tables);
    const alias = this.aliases.next();
    const columns = tableLeaves(table, alias).filter(
      (column) => column.type === type,
    );
    if (columns.length === 0) return this.leafOf(type);
    const extreme = this.random.pick(["min", "max"]);
    const where = this.subqueryWhere(table, alias, depth);
    const select = `SELECT ${extreme}(${this.random.pick(columns).sql}) FROM ${table.name} AS ${alias}${where}`;
    return leaf(type, `(${select})`);
  }

  /**
   * A WHERE clause over the subquery's table and, half the time, the
   * enclosing query's values too: a correlated subquery.
   */
  private subqueryWhere(
    table: TableSpec,
    alias: string,
    depth: number,
  ): string {
    if (this.random.chance(0.25)) return "";
    const isCorrelated = this.random.chance(0.5);
    const outer = isCorrelated ? this.scope.leaves : [];
    const inner = new ExpressionGenerator(
      this.random,
      {
        leaves: [...tableLeaves(table, alias), ...outer],
        tables: this.scope.tables,
      },
      this.aliases,
    );
    return ` WHERE ${print(inner.condition(Math.min(depth, 1)))}`;
  }

  private choose(depth: number, choices: readonly Choice[]): Expression {
    const total = choices.reduce((sum, [weight]) => sum + weight, 0);
    let pick = this.random.below(total);
    for (const [weight, make] of choices) {
      if (pick < weight) return make(depth);
      pick -= weight;
    }
    throw new Error("no choice was picked");
  }
}
