import type { TableSchema } from "../../store/types.js";
import type { BoundColumnRefExpression, LogicalOperator } from "../types.js";
import { BoundExpressionClass } from "../types.js";
import { BindError } from "./errors.js";
import { findColumnIndex } from "./utils/find-column.js";

export interface BindingEntry {
  tableIndex: number;
  tableName: string;
  alias: string;
  schema: TableSchema;
}

interface CTEEntry {
  plan: LogicalOperator;
  index: number;
  aliases: string[];
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeColumnRef(entry: BindingEntry, colIdx: number): BoundColumnRefExpression {
  const col = entry.schema.columns[colIdx];
  return {
    expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
    binding: { tableIndex: entry.tableIndex, columnIndex: colIdx },
    tableName: entry.tableName,
    columnName: col.name,
    returnType: col.type,
  };
}

// ---------------------------------------------------------------------------
// BindScope
// ---------------------------------------------------------------------------

export class BindScope {
  private bindings: BindingEntry[] = [];
  private ctes = new Map<string, CTEEntry>();
  private tableIndexCounter: () => number;

  constructor(tableIndexCounter: () => number, private parent?: BindScope) {
    this.tableIndexCounter = tableIndexCounter;
  }

  addTable(tableName: string, alias: string, schema: TableSchema): BindingEntry {
    const entry: BindingEntry = {
      tableIndex: this.tableIndexCounter(),
      tableName,
      alias,
      schema,
    };
    this.bindings.push(entry);
    return entry;
  }

  resolveColumn(columnName: string, tableAlias?: string): BoundColumnRefExpression {
    return tableAlias
      ? this.resolveQualified(columnName, tableAlias)
      : this.resolveUnqualified(columnName);
  }

  resolveColumnIn(columnName: string, entries: BindingEntry[]): BoundColumnRefExpression {
    for (const entry of entries) {
      const colIdx = findColumnIndex(entry.schema, columnName);
      if (colIdx !== -1) return makeColumnRef(entry, colIdx);
    }
    throw new BindError(`Column "${columnName}" not found in any table`);
  }

  findByAlias(alias: string): BindingEntry | undefined {
    const lower = alias.toLowerCase();
    return this.bindings.find((b) => b.alias.toLowerCase() === lower)
      ?? this.parent?.findByAlias(alias);
  }

  getAllBindings(): BindingEntry[] {
    return this.bindings;
  }

  addCTE(name: string, plan: LogicalOperator, index: number, aliases: string[] = []): void {
    this.ctes.set(name.toLowerCase(), { plan, index, aliases });
  }

  getCTE(name: string): CTEEntry | undefined {
    return this.ctes.get(name.toLowerCase()) ?? this.parent?.getCTE(name);
  }

  createChildScope(): BindScope {
    return new BindScope(this.tableIndexCounter, this);
  }

  createIsolatedScope(): BindScope {
    const child = new BindScope(this.tableIndexCounter);
    this.collectCTEs(child.ctes);
    return child;
  }

  // ---------------------------------------------------------------------------
  // Private
  // ---------------------------------------------------------------------------

  private resolveQualified(columnName: string, tableAlias: string): BoundColumnRefExpression {
    const entry = this.findByAlias(tableAlias);
    if (!entry) throw new BindError(`Unknown table alias "${tableAlias}"`);

    const colIdx = findColumnIndex(entry.schema, columnName);
    if (colIdx === -1) {
      throw new BindError(`Column "${columnName}" not found in table "${entry.tableName}"`);
    }
    return makeColumnRef(entry, colIdx);
  }

  private resolveUnqualified(columnName: string): BoundColumnRefExpression {
    const matches = this.findMatchingColumns(columnName);

    if (matches.length === 0) {
      if (this.parent) return this.parent.resolveUnqualified(columnName);
      throw new BindError(`Column "${columnName}" not found in any table`);
    }
    if (matches.length > 1) {
      const tables = matches.map((m) => `"${m.entry.tableName}"`).join(" and ");
      throw new BindError(`Column "${columnName}" is ambiguous — exists in ${tables}`);
    }

    return makeColumnRef(matches[0].entry, matches[0].colIdx);
  }

  private findMatchingColumns(columnName: string): Array<{ entry: BindingEntry; colIdx: number }> {
    const matches: Array<{ entry: BindingEntry; colIdx: number }> = [];
    for (const entry of this.bindings) {
      const colIdx = findColumnIndex(entry.schema, columnName);
      if (colIdx !== -1) matches.push({ entry, colIdx });
    }
    return matches;
  }

  private collectCTEs(target: Map<string, CTEEntry>): void {
    let current: BindScope | undefined = this as BindScope;
    while (current) {
      for (const [name, entry] of current.ctes) {
        if (!target.has(name)) target.set(name, entry);
      }
      current = current.parent;
    }
  }
}
