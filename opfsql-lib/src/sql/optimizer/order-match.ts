import type { OrderDirection, physical } from "../plan/index.js";
import type { KeyPart } from "./key-parts.js";
import { sameExpression } from "./same-expression.js";

// ---------------------------------------------------------------------------
// Whether an index scan delivers an ORDER BY (docs/planning.md, rule 3).
// Index entries are ordered by their key parts, NULL first (docs/file-
// format.md, Index keys), so a forward scan gives each part ASC NULLS
// FIRST and a backward scan DESC NULLS LAST. The keys must be consecutive
// parts, starting no later than the first part not fixed by `=` (parts
// fixed by `=` hold one value, so any order of them is kept), all in one
// direction, each with that direction's NULL placement — unless its part
// holds no NULL, where placement means nothing.
// ---------------------------------------------------------------------------

export interface WantedOrder {
  /** The ORDER BY keys, over the get's columns. */
  readonly keys: readonly physical.OrderKey[];
  /** A LIMIT above: a read that delivers the order may stop early. */
  readonly isLimited: boolean;
}

/**
 * The direction an index whose first `equalParts` parts are fixed reads in
 * to deliver `order`, or null if no direction does.
 */
export function deliveredDirection(
  order: WantedOrder | null,
  parts: readonly KeyPart[],
  equalParts: number,
): OrderDirection | null {
  const direction = order?.keys[0]?.direction;
  if (order === null || direction === undefined) return null;
  for (let start = 0; start <= equalParts; start += 1) {
    if (keysMatchFrom(order.keys, parts, start, direction)) return direction;
  }
  return null;
}

function keysMatchFrom(
  keys: readonly physical.OrderKey[],
  parts: readonly KeyPart[],
  start: number,
  direction: OrderDirection,
): boolean {
  const nulls = direction === "ascending" ? "first" : "last";
  return keys.every((key, position) => {
    const part = parts[start + position];
    if (part === undefined || key.direction !== direction) return false;
    const isPlaced = key.nulls === nulls || part.isNeverNull;
    return isPlaced && sameExpression(key.expression, part.expression);
  });
}
