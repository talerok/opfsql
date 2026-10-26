import type { physical } from "../../sql/plan/index.js";
import { decodeKey, IndexReader } from "../../storage/index.js";
import type { Value } from "../../values/index.js";
import type { Context } from "../context.js";
import { type Operator, WalkOperator } from "../operator.js";
import { compileNonNullRange, keysIn } from "./key-range.js";

/** The first non-NULL value of the key part after `equal`, from one end of the index. */
export function indexExtreme(
  plan: physical.IndexExtreme,
  context: Context,
): Operator {
  const entries = new IndexReader(context.pages, plan.index.rootPage);
  const bounds = compileNonNullRange(plan.equal, context);
  const direction = plan.extreme === "MIN" ? "ascending" : "descending";
  const position = plan.equal.length;
  const extreme = (): Value => {
    const range = bounds();
    if (range === null) return null;
    const first = keysIn(entries, range, direction).next();
    if (first.done === true) return null;
    return decodeKey(first.value)[position] ?? null;
  };
  return new WalkOperator(() => [[extreme()]].values(), []);
}
