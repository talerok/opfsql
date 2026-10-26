import type { Value } from "../../src/values/index.js";
import {
  multisetDifference,
  type Rows,
  sequenceDifference,
  type Sides,
} from "./compare.js";

const COPIES: Sides = ["the plain copy", "the indexed copy"];

/** One statement of a check. */
export interface Form {
  readonly sql: string;
  readonly parameters: readonly Value[];
  /** Rows in a defined order, compared as a sequence across the copies. */
  readonly isOrdered: boolean;
}

/** Forms of one question, whose results must agree. */
export interface Check {
  readonly oracle: string;
  readonly forms: readonly Form[];
  /** What tells the forms' results apart, or null when they agree. */
  readonly verdict: (results: readonly Rows[]) => string | null;
}

export function unordered(
  sql: string,
  parameters: readonly Value[] = [],
): Form {
  return { sql, parameters, isOrdered: false };
}

export function ordered(sql: string): Form {
  return { sql, parameters: [], isOrdered: true };
}

/** p, NOT p and p IS NULL as WHERE clauses: the three partitions. */
export function partitions(condition: string): string[] {
  return [
    ` WHERE ${condition}`,
    ` WHERE NOT ${condition}`,
    ` WHERE ${condition} IS NULL`,
  ];
}

/** What tells one form's results on the two copies apart. */
export function copyDifference(
  form: Form,
  plain: Rows,
  indexed: Rows,
): string | null {
  if (form.isOrdered) return sequenceDifference(plain, indexed, COPIES);
  return multisetDifference(plain, indexed, COPIES);
}
