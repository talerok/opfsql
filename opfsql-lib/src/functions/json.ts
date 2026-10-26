import { DatatypeMismatchError, SemanticError } from "../errors.js";
import {
  describeValue,
  type JsonValue,
  parseJsonText,
  type Value,
} from "../values/index.js";
import { firstArgumentType, returns, strict, text } from "./arguments.js";
import type {
  CallSite,
  Implementation,
  ScalarFunction,
  StaticType,
} from "./types.js";

// ---------------------------------------------------------------------------
// JSON functions with SQLite's path syntax: `$`, then `.key`, `."key"`,
// `[n]` and `[#-n]` (n-th from the end). The document is a JSON value when
// its static type is JSON, and JSON text otherwise.
// ---------------------------------------------------------------------------

export type PathStep =
  | { readonly type: "key"; readonly key: string }
  | {
      readonly type: "index";
      readonly index: number;
      readonly fromEnd: boolean;
    };

/** A parsed JSON path: the steps after `$`. */
export type Path = readonly PathStep[];

/** `$`: the whole document. */
const ROOT: Path = [];

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
    const hasKey = isObject(value) && Object.hasOwn(value, step.key);
    return hasKey ? value[step.key] : undefined;
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
  return parseJsonText(value);
}

/**
 * The paths of a call — its arguments after the document — for a row's
 * argument values; a path that is the same for every row is parsed once.
 */
function pathArguments(call: CallSite): (values: readonly Value[]) => Path[] {
  const fixed = call.constants
    .slice(1)
    .map((constant) =>
      typeof constant === "string" ? parsePath(constant) : undefined,
    );
  return (values) =>
    fixed.map((path, index) => path ?? parsePath(text(values[index + 1])));
}

/**
 * The implementation of a JSON function of (document [, path]): `apply`
 * gets the document as a JSON value and the path, `$` when there is none.
 */
function withPath(
  call: CallSite,
  apply: (document: JsonValue, path: Path) => Value,
): Implementation {
  const documentType = firstArgumentType(call.argumentTypes);
  const pathsOf = pathArguments(call);
  return strict((values) => {
    const [document = null] = values;
    const [path = ROOT] = pathsOf(values);
    return apply(documentOf(document, documentType), path);
  });
}

/** Several paths: a JSON array of the values found, null where none. */
function extractEach(call: CallSite): Implementation {
  const documentType = firstArgumentType(call.argumentTypes);
  const pathsOf = pathArguments(call);
  return strict((values) => {
    const [document = null] = values;
    const json = documentOf(document, documentType);
    return pathsOf(values).map((path) => followPath(json, path) ?? null);
  });
}

const jsonExtract: ScalarFunction = {
  kind: "scalar",
  name: "json_extract",
  signature: { required: ["json", "text"], rest: "text" },
  returnType: returns("JSON"),
  implement: (call) => {
    const hasSeveralPaths = call.argumentTypes.length > 2;
    if (hasSeveralPaths) return extractEach(call);
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
  implement: ({ argumentTypes }) => {
    const type = firstArgumentType(argumentTypes);
    return strict(([value = null]) => isValidJson(value, type));
  },
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
