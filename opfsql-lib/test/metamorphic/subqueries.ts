import {
  areComparable,
  type Expression,
  type ExpressionType,
  type Leaf,
  leaf,
  node,
  print,
} from "./expression.js";
import type { Random } from "./random.js";
import type { TableSpec } from "./schema.js";
import {
  type AliasSource,
  keyLeaf,
  type Scope,
  seekKeys,
  tableLeaves,
} from "./scope.js";

// ---------------------------------------------------------------------------
// Subqueries in expressions: for IN, EXISTS and as scalars. A scalar one is
// an aggregate, so it has exactly one row. Half of them are correlated: their
// WHERE reads the enclosing query's values, often equating one with a key
// of their own table, which a seek can use for each outer row.
// ---------------------------------------------------------------------------

/** What makes conditions over a scope: the expression generator. */
export type ConditionMaker = (scope: Scope) => {
  condition(depth: number): Expression;
};

export class SubqueryGenerator {
  constructor(
    private readonly random: Random,
    private readonly scope: Scope,
    private readonly aliases: AliasSource,
    private readonly conditionsOver: ConditionMaker,
  ) {}

  /** `SELECT col …` of a column comparable with `type`, or null when no table has one. */
  column(type: ExpressionType, depth: number): Leaf | null {
    const { table, alias } = this.target();
    const columns = tableLeaves(table, alias).filter((each) =>
      areComparable(type, each.type),
    );
    if (columns.length === 0) return null;
    const where = this.where(table, alias, depth);
    const select = `SELECT ${this.random.pick(columns).sql} FROM ${table.name} AS ${alias}${where}`;
    return leaf("untyped", select);
  }

  /** `SELECT 1 …`, for EXISTS. */
  any(depth: number): Leaf {
    const { table, alias } = this.target();
    const where = this.where(table, alias, depth);
    return leaf("untyped", `SELECT 1 FROM ${table.name} AS ${alias}${where}`);
  }

  /** `(SELECT count(*) …)`. */
  count(depth: number): Leaf {
    const { table, alias } = this.target();
    const where = this.where(table, alias, depth);
    return leaf(
      "integer",
      `(SELECT count(*) FROM ${table.name} AS ${alias}${where})`,
    );
  }

  /** `(SELECT min(col) …)` or max, of a column of `type`; null when there is none. */
  extreme(type: ExpressionType, depth: number): Leaf | null {
    const { table, alias } = this.target();
    const columns = tableLeaves(table, alias).filter(
      (each) => each.type === type,
    );
    if (columns.length === 0) return null;
    const extreme = this.random.pick(["min", "max"]);
    const where = this.where(table, alias, depth);
    const select = `SELECT ${extreme}(${this.random.pick(columns).sql}) FROM ${table.name} AS ${alias}${where}`;
    return leaf(type, `(${select})`);
  }

  private target(): { table: TableSpec; alias: string } {
    const tables = this.scope.database?.tables ?? [];
    return { table: this.random.pick(tables), alias: this.aliases.next() };
  }

  /** A WHERE clause over the subquery's table and, half the time, the enclosing query's values. */
  private where(table: TableSpec, alias: string, depth: number): string {
    if (this.random.chance(0.25)) return "";
    const isCorrelated = this.random.chance(0.5);
    const outer = isCorrelated ? this.scope.leaves : [];
    const leaves = [...tableLeaves(table, alias), ...outer];
    const generator = this.conditionsOver({ ...this.scope, leaves });
    const condition = generator.condition(Math.min(depth, 1));
    const seek = isCorrelated ? this.correlatedSeek(table, alias) : null;
    const where =
      seek === null ? condition : node("boolean", seek, " AND ", condition);
    return ` WHERE ${print(where)}`;
  }

  /** `key = outer value`, half the time, when the table has a key and the enclosing query a comparable value. */
  private correlatedSeek(table: TableSpec, alias: string): Expression | null {
    const { database } = this.scope;
    if (database === null || !this.random.chance(0.5)) return null;
    const keys = seekKeys(database, table, true);
    if (keys.length === 0) return null;
    const key = keyLeaf(this.random.pick(keys), alias);
    const partners = this.scope.leaves.filter((each) =>
      areComparable(each.type, key.type),
    );
    if (partners.length === 0) return null;
    return node("boolean", key, " = ", this.random.pick(partners));
  }
}
