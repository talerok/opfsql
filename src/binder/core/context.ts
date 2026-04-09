import type { ICatalog } from '../../store/types.js';
import type { BoundAggregateExpression, BoundExpression } from '../types.js';
import { BindScope } from './scope.js';

export interface BindContext {
  catalog: ICatalog;
  nextTableIndex(): number;
  resetTableIndex(): void;
  createScope(): BindScope;
}

export interface AggregateContext {
  aggregates: BoundAggregateExpression[];
  groups: BoundExpression[];
}

export function createBindContext(catalog: ICatalog): BindContext {
  let seq = 0;
  const nextTableIndex = () => seq++;
  return {
    catalog,
    nextTableIndex,
    resetTableIndex: () => { seq = 0; },
    createScope: () => new BindScope(nextTableIndex),
  };
}
