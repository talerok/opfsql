import type { DropStatement } from "../../../sql/parser/types.js";
import type * as BT from "../types.js";
import { LogicalOperatorType } from "../types.js";

export function bindDrop(stmt: DropStatement): BT.LogicalDrop {
  return {
    type: LogicalOperatorType.LOGICAL_DROP,
    dropType: stmt.dropType,
    name: stmt.name,
    ifExists: stmt.ifExists,
    children: [],
    expressions: [],
    types: [],
    estimatedCardinality: 0,
    columnBindings: [],
  };
}
