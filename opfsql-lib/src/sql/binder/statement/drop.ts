import type { DropStatement } from "../../parser/types.js";
import type * as BT from "../types.js";
import { LogicalOperatorType } from "../types.js";

export function bindDrop(stmt: DropStatement): BT.LogicalDrop {
  return {
    type: LogicalOperatorType.LOGICAL_DROP,
    dropType: stmt.drop_type,
    name: stmt.name,
    ifExists: stmt.if_exists,
    children: [],
    expressions: [],
    types: [],
    estimatedCardinality: 0,
    columnBindings: [],
  };
}
