import { InternalError } from "../errors.js";
import {
  numberOperand,
  textOperand,
  typeFromName,
  type Value,
} from "../values/index.js";
import type { ArgumentKind, Implementation, StaticType } from "./types.js";

// ---------------------------------------------------------------------------
// Helpers shared by the function definitions: what each argument kind
// accepts at bind time, how arguments convert per row, and the common
// shapes of implementations.
// ---------------------------------------------------------------------------

const TEXT_TYPES: readonly StaticType[] = [
  "TEXT",
  "INTEGER",
  "REAL",
  "BOOLEAN",
  "ANY",
  "NULL",
];

const ACCEPTED: Record<ArgumentKind, ReadonlySet<StaticType>> = {
  text: new Set(TEXT_TYPES),
  "text-or-blob": new Set([...TEXT_TYPES, "BLOB"]),
  number: new Set(["INTEGER", "REAL", "ANY", "NULL"]),
  json: new Set(["JSON", "TEXT", "ANY", "NULL"]),
  boolean: new Set(["BOOLEAN", "JSON", "ANY", "NULL"]),
  any: new Set([...TEXT_TYPES, "BLOB", "JSON"]),
};

/**
 * The static type a type name stands for, as plans write them: `BIGINT` is
 * INTEGER, `NULL` the type of the NULL literal, an unknown name ANY.
 */
export function staticType(name: string): StaticType {
  return name === "NULL" ? "NULL" : (typeFromName(name) ?? "ANY");
}

/** True if an argument of `kind` may have the static type `type`. */
export function accepts(kind: ArgumentKind, type: StaticType): boolean {
  return ACCEPTED[kind].has(type);
}

/** The text form of a non-NULL argument (see `textOperand`). */
export function text(value: Value | undefined): string {
  return textOperand(present(value));
}

/** The number a non-NULL argument stands for (see `numberOperand`). */
export function number(value: Value | undefined): number {
  return numberOperand(present(value));
}

/** A non-NULL numeric argument truncated to an integer, as SQLite does. */
export function integer(value: Value | undefined): number {
  const truncated = Math.trunc(number(value));
  return truncated === 0 ? 0 : truncated;
}

/** Characters of `value` — code points, not UTF-16 units. */
export function characters(value: string): string[] {
  return value.match(/./gsu) ?? [];
}

/** An implementation that takes argument values as they are. */
export function eager(
  call: (values: readonly Value[]) => Value,
): Implementation {
  return { kind: "eager", call };
}

/** An implementation that is NULL whenever an argument is NULL. */
export function strict(
  call: (values: readonly Value[]) => Value,
): Implementation {
  return eager((values) => (values.includes(null) ? null : call(values)));
}

/** A return-type rule that does not depend on the arguments. */
export function returns(type: StaticType): () => StaticType {
  return () => type;
}

/** The return-type rule "same type as the first argument". */
export function firstArgumentType(types: readonly StaticType[]): StaticType {
  return types[0] ?? "ANY";
}

/**
 * The type of a value that is one of several expressions (COALESCE, CASE):
 * their type if they all share it, REAL for INTEGERs and REALs (a REAL
 * holding 3.0 must stay REAL: JavaScript cannot tell it from 3), NULL if
 * all are NULL, else ANY.
 */
export function commonType(types: readonly StaticType[]): StaticType {
  const typed = new Set(types.filter((type) => type !== "NULL"));
  if (typed.size === 0) return "NULL";
  const [only] = typed;
  if (only !== undefined && typed.size === 1) return only;
  return typed.size === 2 && typed.has("INTEGER") && typed.has("REAL")
    ? "REAL"
    : "ANY";
}

/** -0 is 0: SQL has one zero. */
export function positiveZero(value: number): number {
  return value === 0 ? 0 : value;
}

function present(value: Value | undefined): Exclude<Value, null> {
  // Callers are strict implementations, which never see NULL, reading
  // arguments the signature guarantees.
  if (value === undefined || value === null) {
    throw new InternalError("a function read an argument it does not have");
  }
  return value;
}
