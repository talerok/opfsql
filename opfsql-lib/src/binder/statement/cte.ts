import type { SelectStatement } from '../../parser/types.js';
import type { LogicalOperator } from '../types.js';
import type { BindContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { BindError } from '../core/errors.js';
import { bindQueryNode } from './query-node.js';

export interface CTECollected {
  name: string;
  plan: LogicalOperator;
  index: number;
}

export function collectCTEs(
  ctx: BindContext,
  cteMap: { map: Record<string, { query: SelectStatement; aliases: string[] }> },
  scope: BindScope,
): CTECollected[] {
  const entries: CTECollected[] = [];
  for (const [name, cteNode] of Object.entries(cteMap.map)) {
    const cteScope = scope.createChildScope();
    const ctePlan = bindQueryNode(ctx, cteNode.query.node, cteScope);
    const cteIndex = ctx.nextTableIndex();

    const aliases = cteNode.aliases;
    if (aliases.length > 0 && aliases.length !== ctePlan.types.length) {
      throw new BindError(
        `CTE "${name}" has ${ctePlan.types.length} columns but ${aliases.length} column aliases were provided`,
      );
    }

    scope.addCTE(name, ctePlan, cteIndex, aliases);
    entries.push({ name, plan: ctePlan, index: cteIndex });
  }
  return entries;
}
