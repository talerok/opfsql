import type {
  Delete,
  Insert,
  physical,
  Query,
  Update,
} from "../sql/plan/index.js";
import type { Value } from "../values/index.js";
import { compilePlan, statementContext } from "./compile.js";
import { drain } from "./operator.js";
import { type Outcome, type Target, writeTargetOf } from "./target.js";
import { runDelete } from "./writes/delete.js";
import { runInsert } from "./writes/insert.js";
import { runUpdate } from "./writes/update.js";

/** The statements the executor runs; DDL goes through the table layer's Catalog. */
export type ExecutableStatement =
  | Query<physical.Plan>
  | Insert<physical.Plan>
  | Update<physical.Plan>
  | Delete<physical.Plan>;

/**
 * Runs a planned statement with `parameters`, compiling its plan for this
 * run: parameters are read while compiling, like constants.
 */
export function execute(
  statement: ExecutableStatement,
  target: Target,
  parameters: readonly Value[],
): Outcome {
  switch (statement.kind) {
    case "query": {
      const context = statementContext(target.pages, parameters);
      const rows = drain(compilePlan(statement.plan, context));
      return { rows, changes: 0, lastInsertRowid: null };
    }
    case "insert": {
      return runInsert(statement, writeTargetOf(target), parameters);
    }
    case "update": {
      return runUpdate(statement, writeTargetOf(target), parameters);
    }
    case "delete": {
      return runDelete(statement, writeTargetOf(target), parameters);
    }
  }
}
