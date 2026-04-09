import type { TableSchema } from '../../store/types.js';
import type {
  BoundColumnRefExpression,
  LogicalOperator,
} from '../types.js';
import { BoundExpressionClass } from '../types.js';
import { BindError } from './errors.js';

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

export class BindScope {
  private bindings: BindingEntry[] = [];
  private ctes = new Map<string, CTEEntry>();
  private tableIndexCounter: () => number;

  constructor(tableIndexCounter: () => number, private parent?: BindScope) {
    this.tableIndexCounter = tableIndexCounter;
  }

  addTable(
    tableName: string,
    alias: string,
    schema: TableSchema,
  ): BindingEntry {
    const entry: BindingEntry = {
      tableIndex: this.tableIndexCounter(),
      tableName,
      alias,
      schema,
    };
    this.bindings.push(entry);
    return entry;
  }

  resolveColumn(
    columnName: string,
    tableAlias?: string,
  ): BoundColumnRefExpression {
    const lowerCol = columnName.toLowerCase();

    if (tableAlias) {
      const lowerAlias = tableAlias.toLowerCase();
      const entry = this.bindings.find(
        (b) => b.alias.toLowerCase() === lowerAlias,
      );
      if (!entry) {
        if (this.parent) {
          return this.parent.resolveColumn(columnName, tableAlias);
        }
        throw new BindError(`Unknown table alias "${tableAlias}"`);
      }
      const colIdx = entry.schema.columns.findIndex(
        (c) => c.name.toLowerCase() === lowerCol,
      );
      if (colIdx === -1) {
        throw new BindError(
          `Column "${columnName}" not found in table "${entry.tableName}"`,
        );
      }
      return {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        binding: { tableIndex: entry.tableIndex, columnIndex: colIdx },
        tableName: entry.tableName,
        columnName: entry.schema.columns[colIdx].name,
        returnType: entry.schema.columns[colIdx].type,
      };
    }

    const matches: Array<{ entry: BindingEntry; colIdx: number }> = [];

    for (const entry of this.bindings) {
      const colIdx = entry.schema.columns.findIndex(
        (c) => c.name.toLowerCase() === lowerCol,
      );
      if (colIdx !== -1) {
        matches.push({ entry, colIdx });
      }
    }

    if (matches.length === 0) {
      if (this.parent) {
        return this.parent.resolveColumn(columnName, tableAlias);
      }
      throw new BindError(
        `Column "${columnName}" not found in any table`,
      );
    }

    if (matches.length > 1) {
      const tables = matches.map((m) => `"${m.entry.tableName}"`).join(' and ');
      throw new BindError(
        `Column "${columnName}" is ambiguous — exists in ${tables}`,
      );
    }

    const { entry, colIdx } = matches[0];
    return {
      expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
      binding: { tableIndex: entry.tableIndex, columnIndex: colIdx },
      tableName: entry.tableName,
      columnName: entry.schema.columns[colIdx].name,
      returnType: entry.schema.columns[colIdx].type,
    };
  }

  resolveColumnIn(
    columnName: string,
    entries: BindingEntry[],
  ): BoundColumnRefExpression {
    const lowerCol = columnName.toLowerCase();
    for (const entry of entries) {
      const colIdx = entry.schema.columns.findIndex(
        (c) => c.name.toLowerCase() === lowerCol,
      );
      if (colIdx !== -1) {
        return {
          expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
          binding: { tableIndex: entry.tableIndex, columnIndex: colIdx },
          tableName: entry.tableName,
          columnName: entry.schema.columns[colIdx].name,
          returnType: entry.schema.columns[colIdx].type,
        };
      }
    }
    throw new BindError(
      `Column "${columnName}" not found in any table`,
    );
  }

  findByAlias(alias: string): BindingEntry | undefined {
    const lower = alias.toLowerCase();
    return this.bindings.find((b) => b.alias.toLowerCase() === lower);
  }

  getAllBindings(): BindingEntry[] {
    return this.bindings;
  }

  addCTE(name: string, plan: LogicalOperator, index: number, aliases: string[] = []): void {
    this.ctes.set(name.toLowerCase(), { plan, index, aliases });
  }

  getCTE(name: string): CTEEntry | undefined {
    const entry = this.ctes.get(name.toLowerCase());
    if (entry) return entry;
    return this.parent?.getCTE(name);
  }

  createChildScope(): BindScope {
    return new BindScope(this.tableIndexCounter, this);
  }

  createIsolatedScope(): BindScope {
    const child = new BindScope(this.tableIndexCounter);
    let current: BindScope | undefined = this as BindScope;
    while (current) {
      for (const [name, entry] of current.ctes) {
        if (!child.ctes.has(name)) {
          child.ctes.set(name, entry);
        }
      }
      current = current.parent;
    }
    return child;
  }
}
