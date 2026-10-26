import * as taxonomy from "../errors.js";
import { type ErrorCode, InternalError, OpfsqlError } from "../errors.js";

// Errors cross the worker boundary as plain data and come back as the same
// class: an OpfsqlError subclass keeps its code, message, own fields (offset,
// timeout, statementIndex, a constraint's columns, …) and its cause; a
// DOMException stays one.

type WireValue = string | number | boolean | null;

/** A field that crosses: a plain value, or a list of them. */
export type WireField = WireValue | readonly WireValue[];

export interface WireError {
  readonly kind: "opfsql" | "dom" | "error";
  readonly name: string;
  readonly code: ErrorCode | null;
  readonly message: string;
  readonly fields: Readonly<Record<string, WireField>>;
  readonly cause?: WireError;
}

type ErrorClass = new (...parameters: never[]) => OpfsqlError;

/** Every OpfsqlError class errors.ts exports, by name. */
const classes = new Map<string, ErrorClass>(
  Object.entries(taxonomy as Record<string, unknown>).filter(
    (entry): entry is [string, ErrorClass] => isErrorClass(entry[1]),
  ),
);

/** How deep a chain of causes travels; deeper ones are cut. */
const CAUSE_DEPTH = 8;

/** Not fields: carried separately, or meaningless in another thread. */
const OMITTED = new Set(["name", "code", "message", "stack", "cause"]);

function isErrorClass(value: unknown): value is ErrorClass {
  return (
    value === OpfsqlError ||
    (typeof value === "function" &&
      Object.prototype.isPrototypeOf.call(OpfsqlError, value))
  );
}

export function toWireError(error: unknown, depth = 0): WireError {
  if (!(error instanceof Error)) {
    return {
      kind: "error",
      name: "Error",
      code: null,
      message: String(error),
      fields: {},
    };
  }
  const cause: unknown = error.cause;
  const wire: WireError = {
    kind: kindOf(error),
    name: error.name,
    code: error instanceof OpfsqlError ? error.code : null,
    message: error.message,
    fields: fieldsOf(error),
  };
  const isChainEnd = cause === undefined || depth >= CAUSE_DEPTH;
  if (isChainEnd) return wire;
  return { ...wire, cause: toWireError(cause, depth + 1) };
}

/** The error a request failed with, as an OpfsqlError of its own class. */
export function fromWireError(wire: WireError): OpfsqlError {
  const error = revive(wire);
  if (error instanceof OpfsqlError) return error;
  return new InternalError(error.message, { cause: error });
}

function kindOf(error: Error): WireError["kind"] {
  if (error instanceof OpfsqlError) return "opfsql";
  const isDomException =
    typeof DOMException === "function" && error instanceof DOMException;
  return isDomException ? "dom" : "error";
}

function fieldsOf(error: Error): Record<string, WireField> {
  const fields: Record<string, WireField> = {};
  for (const [key, value] of Object.entries(error)) {
    if (OMITTED.has(key)) continue;
    if (isField(value)) fields[key] = value;
  }
  return fields;
}

function isField(value: unknown): value is WireField {
  if (!Array.isArray(value)) return isWireValue(value);
  return value.every((item: unknown) => isWireValue(item));
}

function isWireValue(value: unknown): value is WireValue {
  return (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  );
}

function revive(wire: WireError): Error {
  const cause = wire.cause === undefined ? undefined : revive(wire.cause);
  const options = cause === undefined ? undefined : { cause };
  switch (wire.kind) {
    case "opfsql": {
      const errorClass = classes.get(wire.name) ?? OpfsqlError;
      // An instance of the class without running its constructor, whose
      // parameters differ from class to class; the fields come from the wire.
      const error = Reflect.construct(
        Error,
        [wire.message, options],
        errorClass,
      ) as OpfsqlError;
      define(error, { ...wire.fields, name: wire.name, code: wire.code });
      return error;
    }
    case "dom": {
      return new DOMException(wire.message, wire.name);
    }
    case "error": {
      const error = new Error(wire.message, options);
      define(error, { ...wire.fields, name: wire.name });
      return error;
    }
  }
}

/** Sets fields the way class fields are: own, enumerable, writable. */
function define(target: Error, fields: Record<string, WireField>): void {
  for (const [key, value] of Object.entries(fields)) {
    Object.defineProperty(target, key, {
      value,
      enumerable: true,
      writable: true,
      configurable: true,
    });
  }
}
