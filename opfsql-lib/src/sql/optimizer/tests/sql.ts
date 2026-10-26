import { InternalError } from "../../../errors.js";
import { bindIndexKey, bindStatement } from "../../binder/index.js";
import { parse, StatementType } from "../../parser/index.js";
import {
  formatPhysicalPlan,
  type logical,
  type Statement,
} from "../../plan/index.js";
import { optimize } from "../index.js";
import { schema } from "./builders.js";

// A query written as SQL, bound by the real binder against the test schema
// (builders.ts), for the tests that read better as the statement than as
// its plan built node by node. Expression keys bind as the engine's will,
// through the binder's bindIndexKey.

/** The bound statement of one SELECT. */
export function boundQuery(sql: string): Statement<logical.Plan> {
  const [statement, ...others] = parse(sql);
  const isQuery = statement?.type === StatementType.SELECT_STATEMENT;
  if (statement === undefined || !isQuery || others.length > 0) {
    throw new InternalError(`not one SELECT: ${sql}`);
  }
  return bindStatement(statement, schema);
}

/** The logical plan of one SELECT. */
export function boundPlan(sql: string): logical.Plan {
  const statement = boundQuery(sql);
  if (statement.kind !== "query") {
    throw new InternalError(`not a query: ${sql}`);
  }
  return statement.plan;
}

/** The physical plan the optimizer makes of one SELECT, printed. */
export function sqlPlanOf(sql: string): string {
  const statement = optimize(boundQuery(sql), schema, bindIndexKey);
  if (statement.kind !== "query") {
    throw new InternalError(`not a query: ${sql}`);
  }
  return formatPhysicalPlan(statement.plan);
}
