import type { SelectNode, SetOperationNode } from '../../parser/types.js';
import type { LogicalOperator } from '../types.js';
import type { BindContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { bindSelect } from './select.js';
import { bindSetOperation } from './set-operation.js';

export function bindQueryNode(
  ctx: BindContext,
  node: SelectNode | SetOperationNode,
  scope: BindScope,
): LogicalOperator {
  if (node.type === 'SELECT_NODE') {
    return bindSelect(ctx, node, scope);
  }
  return bindSetOperation(ctx, node, scope);
}
