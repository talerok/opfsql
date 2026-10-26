import type { Tuple } from "../expressions/index.js";
import type {
  Delete,
  Insert,
  physical,
  Query,
  Update,
} from "../sql/plan/index.js";
import type { Tables, WritableTables } from "../table/index.js";
import type { ParameterValues } from "../values/index.js";
import { compilePlan, statementContext } from "./compile.js";
import { drain } from "./operator.js";
import { runDelete } from "./writes/delete.js";
import { runInsert } from "./writes/insert.js";
import type { WriteOutcome } from "./writes/outcome.js";
import { runUpdate } from "./writes/update.js";

/** A statement that writes rows; DDL goes through the table layer's Catalog. */
export type WriteStatement =
  Insert<physical.Plan> | Update<physical.Plan> | Delete<physical.Plan>;

/**
 * Runs a planned query with `parameters`, compiling its plan for this run —
 * parameters are read while compiling, like constants — and returns its
 * rows. `checkpoint` is called once per batch the query reads.
 */
export function runQuery(
  statement: Query<physical.Plan>,
  tables: Tables,
  parameters: ParameterValues,
  checkpoint?: () => void,
): readonly Tuple[] {
  const context = statementContext(tables, parameters, checkpoint);
  return drain(compilePlan(statement.plan, context));
}

/** Runs a planned INSERT, UPDATE or DELETE, as runQuery runs a query. */
export function runWrite(
  statement: WriteStatement,
  tables: WritableTables,
  parameters: ParameterValues,
  checkpoint?: () => void,
): WriteOutcome {
  const context = statementContext(tables, parameters, checkpoint);
  switch (statement.kind) {
    case "insert": {
      return runInsert(statement, tables, context);
    }
    case "update": {
      return runUpdate(statement, tables, context);
    }
    case "delete": {
      return runDelete(statement, tables, context);
    }
  }
}
