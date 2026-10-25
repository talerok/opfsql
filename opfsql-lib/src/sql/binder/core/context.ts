import type { ICatalog } from "../../../store/types.js";
import type { BoundAggregateExpression, BoundExpression } from "../types.js";
import { BindScope } from "./scope.js";

export interface BindContext {
  catalog: ICatalog;
  nextTableIndex(): number;
  resetTableIndex(): void;
  createScope(): BindScope;
  /** Records a bound parameter: $n with 0-based index n - 1. */
  noteParameter(index: number): void;
  /** The highest parameter number bound since the reset ($3 → 3). */
  parameterCount(): number;
  resetParameters(): void;
}

export interface AggregateContext {
  aggregates: BoundAggregateExpression[];
  groups: BoundExpression[];
  groupIndex: number;
}

export function createBindContext(catalog: ICatalog): BindContext {
  let seq = 0;
  let parameters = 0;
  const nextTableIndex = () => seq++;
  return {
    catalog,
    nextTableIndex,
    resetTableIndex: () => {
      seq = 0;
    },
    createScope: () => new BindScope(nextTableIndex),
    noteParameter: (index) => {
      parameters = Math.max(parameters, index + 1);
    },
    parameterCount: () => parameters,
    resetParameters: () => {
      parameters = 0;
    },
  };
}
