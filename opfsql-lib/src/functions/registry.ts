import { SemanticError } from "../errors.js";
import { aggregateFunctions } from "./aggregate.js";
import { accepts } from "./arguments.js";
import { conditionalFunctions } from "./conditional.js";
import { jsonFunctions } from "./json.js";
import { likeFunctions } from "./like.js";
import { hashFunctions } from "./md5.js";
import { numericFunctions } from "./numeric.js";
import { textFunctions } from "./text.js";
import type {
  AggregateFunction,
  ArgumentKind,
  ScalarFunction,
  Signature,
  SqlFunction,
  StaticType,
} from "./types.js";

// ---------------------------------------------------------------------------
// The registry: every function opfsql has, by name. The binder resolves
// calls here; the executor runs what the binder resolved.
// ---------------------------------------------------------------------------

const FUNCTIONS: ReadonlyMap<string, SqlFunction> = new Map(
  [
    ...textFunctions,
    ...numericFunctions,
    ...conditionalFunctions,
    ...likeFunctions,
    ...jsonFunctions,
    ...hashFunctions,
    ...aggregateFunctions,
    // Frozen: definitions are shared by every plan that resolves to them.
  ].map((definition) => [definition.name, Object.freeze(definition)]),
);

/** The function called `name` (any case), or undefined if there is none. */
export function lookupFunction(name: string): SqlFunction | undefined {
  return FUNCTIONS.get(name.toLowerCase());
}

/** The function called `name`; a SemanticError if there is none. */
export function sqlFunction(name: string): SqlFunction {
  const definition = lookupFunction(name);
  if (definition === undefined) throw noSuchFunction(name);
  return definition;
}

/** True if `name` is an aggregate function. */
export function isAggregateFunction(name: string): boolean {
  return lookupFunction(name)?.kind === "aggregate";
}

/** The scalar function `name`; a SemanticError if there is none. */
export function scalarFunction(name: string): ScalarFunction {
  const definition = lookupFunction(name);
  if (definition?.kind !== "scalar") throw noSuchFunction(name);
  return definition;
}

/** The aggregate function `name`; a SemanticError if there is none. */
export function aggregateFunction(name: string): AggregateFunction {
  const definition = lookupFunction(name);
  if (definition?.kind !== "aggregate") throw noSuchFunction(name);
  return definition;
}

/**
 * Checks a call's argument types against the function's signature and
 * returns the type of its result. Fails with a SemanticError for a wrong
 * number of arguments or an argument of a type the function does not take.
 */
export function resolveCall(
  definition: SqlFunction,
  argumentTypes: readonly StaticType[],
): StaticType {
  const { signature } = definition;
  if (!fitsArity(signature, argumentTypes.length)) {
    throw new SemanticError(
      `wrong number of arguments to function ${definition.name}()`,
    );
  }
  for (const [index, type] of argumentTypes.entries()) {
    if (!accepts(argumentKind(signature, index), type)) {
      throw new SemanticError(
        `${definition.name}() does not accept ${type} as argument ${String(index + 1)}`,
      );
    }
  }
  return definition.returnType(argumentTypes);
}

function fitsArity(signature: Signature, count: number): boolean {
  const fixed = signature.required.length + (signature.optional?.length ?? 0);
  const maximum = signature.rest === undefined ? fixed : Infinity;
  return count >= signature.required.length && count <= maximum;
}

function argumentKind(signature: Signature, index: number): ArgumentKind {
  const fixed = [...signature.required, ...(signature.optional ?? [])];
  return fixed[index] ?? signature.rest ?? "any";
}

function noSuchFunction(name: string): SemanticError {
  return new SemanticError(`no such function: ${name}`);
}
