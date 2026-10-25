import type { OrderByNode, SetOperationNode } from "../../parser/types.js";
import { ResultModifierType } from "../../parser/types.js";
import type { BindContext } from "../core/context.js";
import { BindError } from "../core/errors.js";
import { makeLimit, makeOrderBy } from "../core/operators.js";
import type { BindScope } from "../core/scope.js";
import { checkTypeCompatibility } from "../core/type-check.js";
import { evalConstantInt } from "../core/utils/eval-constant.js";
import { bindExpression } from "../expression/index.js";
import type * as BT from "../types.js";
import { LogicalOperatorType } from "../types.js";
import { collectCTEs } from "./cte.js";
import { bindQueryNode } from "./query-node.js";
import { bindSelect } from "./select.js";

export function bindSetOperation(
  ctx: BindContext,
  node: SetOperationNode,
  scope: BindScope,
): BT.LogicalOperator {
  const cteEntries = collectCTEs(ctx, node.cte_map, scope);

  const left = bindQueryNode(ctx, node.left, scope);

  const rightScope = scope.createIsolatedScope();
  const right = bindSelect(ctx, node.right, rightScope);

  if (left.types.length !== right.types.length) {
    throw new BindError(
      `UNION requires the same number of columns: left has ${left.types.length}, right has ${right.types.length}`,
    );
  }
  for (let i = 0; i < left.types.length; i++) {
    checkTypeCompatibility(left.types[i], right.types[i]);
  }

  let plan: BT.LogicalOperator = {
    type: LogicalOperatorType.LOGICAL_UNION,
    children: [left, right],
    all: node.set_op_type === "UNION_ALL",
    expressions: [],
    types: left.types,
    estimatedCardinality: 0,
    columnBindings: left.columnBindings,
  } satisfies BT.LogicalUnion;

  for (const mod of node.modifiers) {
    switch (mod.type) {
      case ResultModifierType.ORDER_MODIFIER:
        plan = makeOrderBy(plan, bindOrders(ctx, mod.orders, scope));
        break;
      case ResultModifierType.LIMIT_MODIFIER: {
        const limitVal = mod.limit !== null ? evalConstantInt(mod.limit) : null;
        const offsetVal = mod.offset !== null ? evalConstantInt(mod.offset) : 0;
        plan = makeLimit(plan, limitVal, offsetVal);
        break;
      }
    }
  }

  for (let i = cteEntries.length - 1; i >= 0; i--) {
    const cte = cteEntries[i];
    const innerPlan: BT.LogicalOperator = plan;
    plan = {
      type: LogicalOperatorType.LOGICAL_MATERIALIZED_CTE,
      cteName: cte.name,
      cteIndex: cte.index,
      children: [cte.plan, innerPlan],
      expressions: [],
      types: innerPlan.types,
      estimatedCardinality: 0,
      columnBindings: innerPlan.columnBindings,
    } satisfies BT.LogicalMaterializedCTE;
  }

  return plan;
}

function bindOrders(
  ctx: BindContext,
  orders: OrderByNode[],
  scope: BindScope,
): BT.BoundOrderByNode[] {
  return orders.map((o) => ({
    expression: bindExpression(ctx, o.expression, scope),
    orderType: o.type,
    nullOrder: o.null_order,
  }));
}
