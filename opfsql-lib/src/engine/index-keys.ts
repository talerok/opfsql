import { InternalError } from "../errors.js";
import {
  compileExpression,
  createRun,
  type Scope,
} from "../expressions/index.js";
import type { Schema } from "../schema/index.js";
import { bindIndexKey } from "../sql/binder/index.js";
import type { KeyBinder } from "../sql/optimizer/index.js";
import { type Expression, nodesOf, positions } from "../sql/plan/index.js";
import { type CompileExpression, IndexKeys } from "../table/index.js";
import { NO_PARAMETERS } from "../values/index.js";

// ---------------------------------------------------------------------------
// Index keys that are expressions are stored as SQL text (docs/planning.md,
// Schema text). The layers that use them cannot parse: the table layer gets
// a compiler, the optimizer a binder, and both come from here — the binder's
// bindIndexKey, and the expression compiler over the table's columns.
// ---------------------------------------------------------------------------

/** The relation a key's columns are bound to: a key reads one table. */
const KEY_TABLE = 0;

/** Binds expression keys for the optimizer, which matches conditions to them. */
export const bindKey: KeyBinder = bindIndexKey;

/** Compiles an expression key into a function of a table row (column order). */
export const compileIndexKey: CompileExpression = (sql, table) => {
  const expression = bindIndexKey(sql, table, KEY_TABLE);
  if (!holdsNoSubquery(expression)) {
    throw new InternalError(`the index key ${sql} holds a subquery`);
  }
  const scope: Scope = {
    layout: positions(KEY_TABLE, table.columns.length),
    outerLayouts: [],
    run: createRun(NO_PARAMETERS, { compile: refuseSubquery }),
  };
  return compileExpression(expression, scope);
};

/** The index keys of `schema`, each compiled when first needed. */
export function indexKeysOf(schema: Schema): IndexKeys {
  return new IndexKeys(schema, compileIndexKey);
}

/**
 * True if no node of `expression` is a subquery, so it holds no plan,
 * logical or physical. The binder refuses subqueries in index keys.
 */
function holdsNoSubquery<Plan>(
  expression: Expression<Plan>,
): expression is Expression<never> {
  for (const node of nodesOf(expression)) {
    if (isSubquery(node)) return false;
  }
  return true;
}

const SUBQUERY_KINDS: ReadonlySet<string> = new Set([
  "scalar-subquery",
  "exists",
  "quantified-comparison",
]);

function isSubquery<Plan>(node: Expression<Plan>): boolean {
  return SUBQUERY_KINDS.has(node.kind);
}

function refuseSubquery(): never {
  throw new InternalError("an index key compiled a subquery");
}
