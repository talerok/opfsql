import type { Index, Table } from "../../schema/index.js";
import {
  type ExpressionStyle,
  formatAggregateCall,
  formatConditions,
  formatExpression,
} from "./format-expression.js";
import type { OrderDirection } from "./expression.js";
import type {
  Bound,
  Expression,
  IndexBranch,
  KeyRange,
  OrderKey,
  Plan,
  Range,
  RowidSet,
} from "./physical.js";

// ---------------------------------------------------------------------------
// A physical plan as text, for EXPLAIN and tests: one line per node, its
// children indented under it. A scan shows its access path, key range and
// residual; a join its keys. `#n` is a table index. A column prints by its
// name as written; a subquery inside an expression as `(subquery n)`, its
// plan following the main plan under that heading.
// ---------------------------------------------------------------------------

const INDENT = "  ";

export function formatPhysicalPlan(plan: Plan): string {
  const subqueries: Plan[] = [];
  const style: ExpressionStyle<Plan> = {
    column: (reference) => reference.name,
    subquery: (subplan, isCorrelated) => {
      subqueries.push(subplan);
      const name = `subquery ${String(subqueries.length)}`;
      return isCorrelated ? `(correlated ${name})` : `(${name})`;
    },
  };
  const describer = new NodeDescriber(style);
  const lines = [...linesOf(plan, 0, describer)];
  // Printing a subquery can number further subqueries nested in it; an
  // array's iterator sees what is pushed while it runs.
  for (const [position, subquery] of subqueries.entries()) {
    const heading = `subquery ${String(position + 1)}:`;
    lines.push(heading, ...linesOf(subquery, 1, describer));
  }
  return lines.join("\n");
}

function* linesOf(
  plan: Plan,
  depth: number,
  describer: NodeDescriber,
): Generator<string> {
  yield INDENT.repeat(depth) + describer.describe(plan);
  for (const child of childPlans(plan)) {
    yield* linesOf(child, depth + 1, describer);
  }
}

/** A node's children, in the order they print. */
function childPlans(plan: Plan): readonly Plan[] {
  switch (plan.kind) {
    case "table-scan":
    case "rowid-lookup":
    case "index-scan":
    case "index-union":
    case "index-extreme":
    case "cte-scan":
    case "values": {
      return [];
    }
    case "filter":
    case "project":
    case "hash-aggregate":
    case "sort":
    case "top-n":
    case "limit":
    case "hash-distinct": {
      return [plan.child];
    }
    case "hash-join":
    case "nested-loop-join":
    case "set-operation": {
      return [plan.left, plan.right];
    }
    case "cte": {
      return [plan.definition, plan.body];
    }
    case "recursive-cte": {
      return [plan.anchor, plan.step];
    }
  }
}

/** The one line a node prints as, without its children. */
class NodeDescriber {
  constructor(private readonly style: ExpressionStyle<Plan>) {}

  describe(plan: Plan): string {
    switch (plan.kind) {
      case "table-scan":
      case "rowid-lookup":
      case "index-scan":
      case "index-union":
      case "index-extreme": {
        return this.access(plan);
      }
      case "hash-join":
      case "nested-loop-join": {
        return this.join(plan);
      }
      case "filter": {
        return `filter ${this.conditions(plan.conditions)}`;
      }
      case "project": {
        return `project#${String(plan.tableIndex)} ${this.list(plan.expressions)}`;
      }
      case "hash-aggregate": {
        return this.aggregate(plan);
      }
      case "sort": {
        return `sort ${this.keys(plan.keys)}`;
      }
      case "top-n": {
        const rows = this.limit(plan.limit, plan.offset);
        return `top-n ${rows} by ${this.keys(plan.keys)}`;
      }
      case "limit": {
        return `limit ${this.limit(plan.limit, plan.offset)}`;
      }
      case "hash-distinct": {
        return "hash-distinct";
      }
      case "set-operation": {
        return `set-operation#${String(plan.tableIndex)} ${plan.operator}`;
      }
      case "values": {
        return `values#${String(plan.tableIndex)} ${this.rows(plan.rows)}`;
      }
      case "cte":
      case "cte-scan":
      case "recursive-cte": {
        return this.cte(plan);
      }
    }
  }

  private access(
    plan: Extract<
      Plan,
      {
        readonly kind:
          | "table-scan"
          | "rowid-lookup"
          | "index-scan"
          | "index-union"
          | "index-extreme";
      }
    >,
  ): string {
    switch (plan.kind) {
      case "table-scan": {
        const head = `table-scan ${tableName(plan)}${backward(plan)}`;
        return this.scan(head, plan.residual);
      }
      case "rowid-lookup": {
        const rowids = this.rowids(plan.rowids);
        const head = `rowid-lookup ${tableName(plan)} (${rowids})${backward(plan)}`;
        return this.scan(head, plan.residual);
      }
      case "index-scan": {
        const using = this.indexRange(plan.table, plan.index, plan.keys);
        const head = `index-scan ${tableName(plan)} ${using}${backward(plan)}`;
        return this.scan(head, plan.residual);
      }
      case "index-union": {
        const branches = plan.branches.map((branch) =>
          this.branch(plan.table, branch),
        );
        const head = `index-union ${tableName(plan)} [${branches.join("; ")}]`;
        return this.scan(head, plan.residual);
      }
      case "index-extreme": {
        const part = keyPartName(plan.table, plan.index, plan.equal.length);
        const keys = { equal: plan.equal, range: null };
        const using = this.indexRange(plan.table, plan.index, keys);
        return `index-extreme#${String(plan.aggregateIndex)} ${plan.extreme}(${part}) of ${plan.table.name} ${using}`;
      }
    }
  }

  private scan(head: string, residual: readonly Expression[]): string {
    if (residual.length === 0) return head;
    return `${head} residual: ${this.conditions(residual)}`;
  }

  private branch(table: Table, branch: IndexBranch): string {
    if (branch.kind === "rowids") return this.rowids(branch.rowids);
    return this.indexRange(table, branch.index, branch.keys);
  }

  /** `using name (a = 1, b > 2)`, or `using name` for the whole index. */
  private indexRange(table: Table, index: Index, keys: KeyRange): string {
    const parts = keys.equal.map(
      (value, part) =>
        `${keyPartName(table, index, part)} = ${this.expression(value)}`,
    );
    if (keys.range !== null) {
      const name = keyPartName(table, index, keys.equal.length);
      parts.push(this.range(name, keys.range));
    }
    const using = `using ${index.name}`;
    return parts.length === 0 ? using : `${using} (${parts.join(", ")})`;
  }

  private rowids(rowids: RowidSet): string {
    if (rowids.kind === "range") return this.range("rowid", rowids.range);
    const [only] = rowids.values;
    if (only !== undefined && rowids.values.length === 1) {
      return `rowid = ${this.expression(only)}`;
    }
    return `rowid IN (${this.list(rowids.values)})`;
  }

  /** `name > 1 AND name <= 5`, or one side when the other is open. */
  private range(name: string, range: Range): string {
    const sides: string[] = [];
    if (range.lower !== null) sides.push(this.bound(name, range.lower, ">"));
    if (range.upper !== null) sides.push(this.bound(name, range.upper, "<"));
    return sides.join(" AND ");
  }

  private bound(name: string, bound: Bound, symbol: ">" | "<"): string {
    const operator = bound.isInclusive ? `${symbol}=` : symbol;
    return `${name} ${operator} ${this.expression(bound.value)}`;
  }

  private join(
    plan: Extract<Plan, { readonly kind: "hash-join" | "nested-loop-join" }>,
  ): string {
    if (plan.kind === "nested-loop-join") {
      const head = `nested-loop-join ${plan.joinType}`;
      if (plan.conditions.length === 0) return head;
      return `${head} on ${this.conditions(plan.conditions)}`;
    }
    const keys = plan.leftKeys.map((left, position) => {
      const right = plan.rightKeys[position];
      const rightText = right === undefined ? "?" : this.expression(right);
      return `${this.expression(left)} = ${rightText}`;
    });
    const head = `hash-join ${plan.joinType} on ${keys.join(", ")}`;
    if (plan.residual.length === 0) return head;
    return `${head} residual: ${this.conditions(plan.residual)}`;
  }

  private aggregate(
    plan: Extract<Plan, { readonly kind: "hash-aggregate" }>,
  ): string {
    const indexes = `#${String(plan.groupIndex)}#${String(plan.aggregateIndex)}`;
    const parts = [`hash-aggregate${indexes}`];
    if (plan.groups.length > 0) {
      parts.push(`group by ${this.list(plan.groups)}`);
    }
    if (plan.aggregates.length > 0) {
      const calls = plan.aggregates.map((call) =>
        formatAggregateCall(call, this.style),
      );
      parts.push(`compute ${calls.join(", ")}`);
    }
    return parts.join(" ");
  }

  private cte(
    plan: Extract<
      Plan,
      { readonly kind: "cte" | "cte-scan" | "recursive-cte" }
    >,
  ): string {
    switch (plan.kind) {
      case "cte": {
        return `cte#${String(plan.cteIndex)} ${plan.name}`;
      }
      case "cte-scan": {
        return `cte-scan#${String(plan.tableIndex)} of cte#${String(plan.cteIndex)}`;
      }
      case "recursive-cte": {
        const union = plan.isUnionAll ? "UNION ALL" : "UNION";
        return `recursive-cte#${String(plan.tableIndex)} ${plan.name} working set cte#${String(plan.cteIndex)} ${union}`;
      }
    }
  }

  private limit(limit: Expression | null, offset: Expression | null): string {
    const count = limit === null ? "all" : this.expression(limit);
    if (offset === null) return count;
    return `${count} offset ${this.expression(offset)}`;
  }

  private rows(rows: readonly (readonly Expression[])[]): string {
    if (rows.length === 0) return "no rows";
    return rows.map((row) => `(${this.list(row)})`).join(", ");
  }

  private keys(keys: readonly OrderKey[]): string {
    return keys.map((key) => this.orderKey(key)).join(", ");
  }

  /** `expression ASC`, with NULLS FIRST / LAST only where it is not the default. */
  private orderKey(key: OrderKey): string {
    const isAscending = key.direction === "ascending";
    const direction = isAscending ? "ASC" : "DESC";
    const defaultNulls = isAscending ? "first" : "last";
    const nulls =
      key.nulls === defaultNulls ? "" : ` NULLS ${key.nulls.toUpperCase()}`;
    return `${this.expression(key.expression)} ${direction}${nulls}`;
  }

  private conditions(conditions: readonly Expression[]): string {
    return formatConditions(conditions, this.style);
  }

  private list(expressions: readonly Expression[]): string {
    return expressions
      .map((expression) => this.expression(expression))
      .join(", ");
  }

  private expression(expression: Expression): string {
    return formatExpression(expression, this.style);
  }
}

/** " descending" for a read from the largest key, else nothing. */
function backward(plan: { readonly direction: OrderDirection }): string {
  return plan.direction === "descending" ? " descending" : "";
}

function tableName(plan: {
  readonly table: Table;
  readonly tableIndex: number;
}): string {
  return `${plan.table.name}#${String(plan.tableIndex)}`;
}

/** A key part as it reads: its column's name, or its expression's text. */
function keyPartName(table: Table, index: Index, part: number): string {
  const key = index.keys[part];
  if (key === undefined) return "?";
  if (key.kind === "expression") return key.sql;
  return table.columns[key.column]?.name ?? "?";
}
