import {
  OrderByNullType,
  OrderType,
  type OrderByNode,
} from "../../../../sql/parser/types.js";

/**
 * Where an ORDER BY item puts NULLs: as written, or by default last when
 * ascending and first when descending. docs/semantics.md specifies SQLite's
 * opposite default; the binder rebuild settles which one holds.
 */
export function nullOrderOf(order: OrderByNode): OrderByNullType {
  if (order.nullOrder !== null) return order.nullOrder;
  return order.type === OrderType.ASCENDING
    ? OrderByNullType.NULLS_LAST
    : OrderByNullType.NULLS_FIRST;
}
