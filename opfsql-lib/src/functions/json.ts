import { DatatypeMismatchError, SemanticError } from "../errors.js";
import {
  describeValue,
  type JsonValue,
  normalizeValue,
  type Value,
} from "../values/index.js";
import { returns, strict, text } from "./arguments.js";
import type { CallSite, ScalarFunction, StaticType } from "./types.js";

// ---------------------------------------------------------------------------
// JSON functions with SQLite's path syntax: `$`, then `.key`, `."key"`,
// `[n]` and `[#-n]` (n-th from the end). The document is a JSON value when
// its static type is JSON, and JSON text otherwise.
// ---------------------------------------------------------------------------

type PathStep =
  | { readonly type: "key"; readonly key: string }
  | {
      readonly type: "index";
      readonly index: number;
      readonly fromEnd: boolean;
    };

type Path = readonly PathStep[];

const STEP = /^(?:\.(?:"((?:[^"\\]|\\.)*)"|([^.[]+))|\[(#-)?(\d+)\])/u;

/** Parses a JSON path such as `$.items[0]."a key"`. */
export function parsePath(path: string): Path {
  if (!path.startsWith("$")) throw badPath(path);
  const steps: PathStep[] = [];
  let rest = path.slice(1);
  while (rest !== "") {
    const match = STEP.exec(rest);
    if (match === null) throw badPath(path);
    steps.push(pathStep(match));
    rest = rest.slice(match[0].length);
  }
  return steps;
}

function pathStep(match: RegExpExecArray): PathStep {
  const [, quoted, bare, fromEnd, index] = match;
  if (index !== undefined) {
    return {
      type: "index",
      index: Number(index),
      fromEnd: fromEnd !== undefined,
    };
  }
  const key =
    quoted === undefined ? bare : (JSON.parse(`"${quoted}"`) as string);
  return { type: "key", key: key ?? "" };
}

function badPath(path: string): SemanticError {
  return new SemanticError(`bad JSON path: ${JSON.stringify(path)}`);
}

/** The value at `path` in `document`, or undefined if there is none. */
export function followPath(
  document: JsonValue,
  path: Path,
): JsonValue | undefined {
  let current: JsonValue | undefined = document;
  for (const step of path) {
    if (current === undefined) return undefined;
    current = followStep(current, step);
  }
  return current;
}

function followStep(value: JsonValue, step: PathStep): JsonValue | undefined {
  if (step.type === "key") {
    return isObject(value) && Object.hasOwn(value, step.key)
      ? value[step.key]
      : undefined;
  }
  if (!Array.isArray(value)) return undefined;
  return value[step.fromEnd ? value.length - step.index : step.index];
}

function isObject(value: JsonValue): value is Record<string, JsonValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** The JSON value an argument holds: parsed if it is JSON text. */
function documentOf(value: Value, type: StaticType): JsonValue {
  if (value instanceof Uint8Array) {
    throw new DatatypeMismatchError(`${describeValue(value)} is not JSON`);
  }
  if (typeof value !== "string" || type === "JSON") return value;
  try {
    return normalizeValue(JSON.parse(value) as JsonValue) as JsonValue;
  } catch {
    throw new DatatypeMismatchError(`malformed JSON: ${describeValue(value)}`);
  }
}

/**
 * Builds the per-row function of (document, path) for a JSON function,
 * parsing a constant path once.
 */
function withPath(
  call: CallSite,
  apply: (document: JsonValue, path: Path) => Value,
): ReturnType<typeof strict> {
  const documentType = call.argumentTypes[0] ?? "ANY";
  const constant = call.constants[1];
  const fixed = typeof constant === "string" ? parsePath(constant) : null;
  return strict(([document = null, path]) =>
    apply(
      documentOf(document, documentType),
      fixed ?? (path === undefined ? [] : parsePath(text(path))),
    ),
  );
}

const jsonExtract: ScalarFunction = {
  kind: "scalar",
  name: "json_extract",
  signature: { required: ["json", "text"], rest: "text" },
  returnType: returns("JSON"),
  implement: (call) => {
    if (call.constants.length > 2) {
      // Several paths: a JSON array of the values found, null where none.
      const documentType = call.argumentTypes[0] ?? "ANY";
      return strict(([document = null, ...paths]) => {
        const json = documentOf(document, documentType);
        return paths.map(
          (path) => followPath(json, parsePath(text(path))) ?? null,
        );
      });
    }
    // JSON null is SQL NULL.
    return withPath(call, (json, path) => followPath(json, path) ?? null);
  },
};

const jsonType: ScalarFunction = {
  kind: "scalar",
  name: "json_type",
  signature: { required: ["json"], optional: ["text"] },
  returnType: returns("TEXT"),
  implement: (call) =>
    withPath(call, (json, path) => {
      const found = followPath(json, path);
      return found === undefined ? null : jsonTypeName(found);
    }),
};

function jsonTypeName(value: JsonValue): string {
  if (value === null) return "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") {
    return Number.isSafeInteger(value) ? "integer" : "real";
  }
  if (typeof value === "string") return "text";
  return Array.isArray(value) ? "array" : "object";
}

const jsonArrayLength: ScalarFunction = {
  kind: "scalar",
  name: "json_array_length",
  signature: { required: ["json"], optional: ["text"] },
  returnType: returns("INTEGER"),
  implement: (call) =>
    withPath(call, (json, path) => {
      const found = followPath(json, path);
      if (found === undefined) return null;
      return Array.isArray(found) ? found.length : 0;
    }),
};

const jsonValid: ScalarFunction = {
  kind: "scalar",
  name: "json_valid",
  signature: { required: ["any"] },
  returnType: returns("BOOLEAN"),
  implement: ({ argumentTypes }) =>
    strict(([value = null]) => isValidJson(value, argumentTypes[0] ?? "ANY")),
};

function isValidJson(value: Value, type: StaticType): boolean {
  if (value instanceof Uint8Array) return false;
  try {
    documentOf(value, type);
    return true;
  } catch {
    return false;
  }
}

export const jsonFunctions: readonly ScalarFunction[] = [
  jsonExtract,
  jsonType,
  jsonArrayLength,
  jsonValid,
];
