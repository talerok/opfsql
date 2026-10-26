import { SemanticError } from "../../errors.js";
import type { CTENode } from "../parser/index.js";
import type { logical, OutputColumn } from "../plan/index.js";
import type { CteDefinition } from "./scope.js";
import { itemAt } from "./one.js";

/** A WITH query bound: how names see it, and the plan it materializes. */
export interface BoundCte {
  readonly definition: CteDefinition;
  readonly plan: logical.Plan;
}

/** The columns a WITH query offers: renamed by `name(a, b)` if it says so. */
export function cteColumns(
  cte: CTENode,
  columns: readonly OutputColumn[],
): OutputColumn[] {
  const names = cte.columnNames;
  if (names.length === 0) return [...columns];
  if (names.length !== columns.length) {
    throw new SemanticError(
      `WITH query ${cte.name} has ${String(columns.length)} columns but ${String(names.length)} names`,
    );
  }
  return columns.map((column, position) => ({
    name: itemAt(names, position, "the WITH query's column names"),
    type: column.type,
  }));
}
