import type { PhysicalOperator, Tuple } from '../types.js';

/** Drain all tuples from an operator into a flat array. */
export async function drainOperator(op: PhysicalOperator): Promise<Tuple[]> {
  const result: Tuple[] = [];
  while (true) {
    const batch = await op.next();
    if (!batch) break;
    for (const tuple of batch) result.push(tuple);
  }
  return result;
}
