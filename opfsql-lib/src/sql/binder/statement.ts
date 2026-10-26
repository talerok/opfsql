import type { Schema } from "../../schema/index.js";
import {
  StatementType,
  type ExplainStatement,
  type Statement,
  type TransactionStatement,
} from "../parser/index.js";
import type { logical, Statement as BoundStatement } from "../plan/index.js";
import { bindAlterTable } from "./alter-table.js";
import { bindCreateIndex } from "./create-index.js";
import { bindCreateTable } from "./create-table.js";
import { bindDelete, bindUpdate } from "./dml.js";
import { bindDrop } from "./drop.js";
import { bindInsert } from "./insert.js";
import { bindQuery } from "./query.js";
import { BindContext, Scope } from "./scope.js";

// ---------------------------------------------------------------------------
// The binder's entry: a statement's syntax tree and the schema it runs
// against to a bound statement (docs/planning.md). BEGIN, COMMIT and
// ROLLBACK never get here, and for EXPLAIN the engine binds the statement
// it explains.
// ---------------------------------------------------------------------------

/** The statements the binder takes. */
export type BindableStatement = Exclude<
  Statement,
  TransactionStatement | ExplainStatement
>;

export function bindStatement(
  statement: BindableStatement,
  schema: Schema,
): BoundStatement<logical.Plan> {
  const context = new BindContext(schema, bindQuery);
  switch (statement.type) {
    case StatementType.SELECT_STATEMENT: {
      const { plan, columns } = bindQuery(statement, Scope.root(context));
      const { parameterCount } = context;
      return { kind: "query", plan, columns, parameterCount };
    }
    case StatementType.INSERT_STATEMENT: {
      return bindInsert(statement, context);
    }
    case StatementType.UPDATE_STATEMENT: {
      return bindUpdate(statement, context);
    }
    case StatementType.DELETE_STATEMENT: {
      return bindDelete(statement, context);
    }
    case StatementType.CREATE_TABLE_STATEMENT: {
      return bindCreateTable(statement, schema);
    }
    case StatementType.CREATE_INDEX_STATEMENT: {
      return bindCreateIndex(statement, context);
    }
    case StatementType.ALTER_TABLE_STATEMENT: {
      return bindAlterTable(statement, context);
    }
    case StatementType.DROP_STATEMENT: {
      return bindDrop(statement, schema);
    }
  }
}
