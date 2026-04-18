import type { IndexKey } from "../../../store/index-btree/types.js";
import type {
  ICatalog,
  IndexDef,
  Row,
  RowId,
  SyncIIndexManager,
  TableSchema,
} from "../../../store/types.js";
import { bindIndexExpression } from "../../../store/index-expression.js";
import { compileExpression } from "../../evaluate/compile.js";
import { buildResolver } from "../../resolve.js";
import type { Tuple, Value } from "../../types.js";

// ---------------------------------------------------------------------------
// PreparedIndex — pre-compiled closures for index key evaluation.
// Created once per IndexDef, reused across all rows.
// ---------------------------------------------------------------------------

type Evaluators = Array<(tuple: Tuple) => Value>;

const prepared = new WeakMap<IndexDef, Evaluators>();

function prepare(schema: TableSchema, idx: IndexDef): Evaluators {
  let ev = prepared.get(idx);
  if (ev) return ev;

  const boundExprs = idx.expressions.map((e) =>
    bindIndexExpression(e, schema, 0),
  );
  const layout = schema.columns.map((_c, i) => ({
    tableIndex: 0,
    columnIndex: i,
  }));
  const resolver = buildResolver(layout);
  const ctx = {
    executeSubplan: () => [] as Value[][],
    params: [] as Value[],
  };
  ev = boundExprs.map((expr) => compileExpression(expr, resolver, ctx));

  prepared.set(idx, ev);
  return ev;
}

function rowToTuple(row: Row, schema: TableSchema): Tuple {
  return schema.columns.map((c) => (row[c.name] ?? null) as Value);
}

function evalKey(tuple: Tuple, evaluators: Evaluators): IndexKey {
  return evaluators.map((fn) => fn(tuple) as IndexKey[number]);
}

export function buildIndexKey(
  row: Row,
  schema: TableSchema,
  idx: IndexDef,
): IndexKey {
  return evalKey(rowToTuple(row, schema), prepare(schema, idx));
}

export function maintainIndexesInsert(
  tableName: string,
  row: Row,
  rowId: RowId,
  catalog?: ICatalog,
  indexManager?: SyncIIndexManager,
): void {
  if (!catalog || !indexManager) return;
  const schema = catalog.getTable(tableName);
  if (!schema) return;
  const tuple = rowToTuple(row, schema);
  for (const idx of catalog.getTableIndexes(tableName)) {
    indexManager.insert(idx.name, evalKey(tuple, prepare(schema, idx)), rowId);
  }
}

export function maintainIndexesDelete(
  tableName: string,
  row: Row,
  rowId: RowId,
  catalog?: ICatalog,
  indexManager?: SyncIIndexManager,
): void {
  if (!catalog || !indexManager) return;
  const schema = catalog.getTable(tableName);
  if (!schema) return;
  const tuple = rowToTuple(row, schema);
  for (const idx of catalog.getTableIndexes(tableName)) {
    indexManager.delete(idx.name, evalKey(tuple, prepare(schema, idx)), rowId);
  }
}
