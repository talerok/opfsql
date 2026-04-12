import type {
  AlterTableStatement,
  CreateIndexStatement,
  CreateTableStatement,
  DeleteStatement,
  DropStatement,
  InsertStatement,
  Statement,
  UpdateStatement,
} from "../parser/types.js";
import { StatementType } from "../parser/types.js";
import type { ICatalog } from "../store/types.js";
import { createBindContext, type BindContext } from "./core/context.js";
import { BindError } from "./core/errors.js";
import { bindAlterTable } from "./statement/alter-table.js";
import { bindCreateIndex } from "./statement/create-index.js";
import { bindCreateTable } from "./statement/create-table.js";
import { bindDelete } from "./statement/delete.js";
import { bindDrop } from "./statement/drop.js";
import { bindInsert } from "./statement/insert.js";
import { bindQueryNode } from "./statement/query-node.js";
import { bindUpdate } from "./statement/update.js";
import type { LogicalOperator } from "./types.js";

export class Binder {
  private ctx: BindContext;

  constructor(catalog: ICatalog) {
    this.ctx = createBindContext(catalog);
  }

  bindStatement(stmt: Statement): LogicalOperator {
    this.ctx.resetTableIndex();

    switch (stmt.type) {
      case StatementType.SELECT_STATEMENT: {
        const scope = this.ctx.createScope();
        return bindQueryNode(this.ctx, stmt.node, scope);
      }
      case StatementType.INSERT_STATEMENT:
        return bindInsert(this.ctx, stmt as InsertStatement);
      case StatementType.UPDATE_STATEMENT:
        return bindUpdate(this.ctx, stmt as UpdateStatement);
      case StatementType.DELETE_STATEMENT:
        return bindDelete(this.ctx, stmt as DeleteStatement);
      case StatementType.CREATE_TABLE_STATEMENT:
        return bindCreateTable(this.ctx, stmt as CreateTableStatement);
      case StatementType.CREATE_INDEX_STATEMENT:
        return bindCreateIndex(this.ctx, stmt as CreateIndexStatement);
      case StatementType.ALTER_TABLE_STATEMENT:
        return bindAlterTable(this.ctx, stmt as AlterTableStatement);
      case StatementType.DROP_STATEMENT:
        return bindDrop(stmt as DropStatement);
      case StatementType.TRANSACTION_STATEMENT:
        throw new BindError(
          "Transaction statements are not handled by the binder",
        );
      default:
        throw new BindError("Unknown statement type");
    }
  }
}

// Re-exports
export { BindError } from "./core/errors.js";
export { BindScope, type BindingEntry } from "./core/scope.js";
export * from "./types.js";

// Interfaces
import type { TableSchema } from "../store/types.js";
import type { BindingEntry } from "./core/scope.js";
import type { BoundColumnRefExpression } from "./types.js";

export interface IBinder {
  bindStatement(stmt: Statement): LogicalOperator;
}

export interface IBindScope {
  addTable(tableName: string, alias: string, schema: TableSchema): BindingEntry;
  resolveColumn(
    columnName: string,
    tableAlias?: string,
  ): BoundColumnRefExpression;
  resolveColumnIn(
    columnName: string,
    entries: BindingEntry[],
  ): BoundColumnRefExpression;
  findByAlias(alias: string): BindingEntry | undefined;
  getAllBindings(): BindingEntry[];
  addCTE(name: string, plan: LogicalOperator, index: number): void;
  getCTE(name: string): { plan: LogicalOperator; index: number } | undefined;
  createChildScope(): IBindScope;
}
