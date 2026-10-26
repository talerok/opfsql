import { describe, expect, it } from "vitest";
import * as errors from "../../errors.js";
import { fromWireError, toWireError } from "../wire-error.js";

/** An error's own fields, as a plain object. */
function fieldsOf(error: Error): Record<string, unknown> {
  return Object.fromEntries(Object.entries(error));
}

type ErrorClass = abstract new (...parameters: never[]) => errors.OpfsqlError;

/** Every OpfsqlError class errors.ts exports. */
function exportedErrorClasses(): ErrorClass[] {
  return Object.values(errors as Record<string, unknown>).filter(
    (value): value is ErrorClass => isErrorClass(value),
  );
}

function isErrorClass(value: unknown): value is ErrorClass {
  if (typeof value !== "function") return false;
  const isSubclass = value.prototype instanceof errors.OpfsqlError;
  return value === errors.OpfsqlError || isSubclass;
}

/**
 * One error of each class, with fields: a class errors.ts exports without
 * one here fails the test, so no class goes untried.
 */
const SAMPLES: readonly errors.OpfsqlError[] = [
  new errors.OpfsqlError("CORRUPT", "damaged"),
  new errors.SqlSyntaxError("bad token", 7, 3),
  new errors.SemanticError("no such table"),
  new errors.NotSupportedError("WINDOW"),
  new errors.MisuseError("closed"),
  new errors.DatatypeMismatchError("not an integer"),
  new errors.IntegerOverflowError(),
  new errors.ConstraintError("unique", "users", ["email"], "users_email"),
  new errors.RowTooLargeError("t", 5000, 2025),
  new errors.IndexKeyTooLargeError("t_body", 3000, 2000),
  new errors.IoError("could not write", { cause: new Error("disk") }),
  new errors.StorageFullError("no room"),
  new errors.CorruptError("page 3 is damaged"),
  new errors.UnsupportedFormatError("app.opfsql", 4, 5),
  new errors.WorkerError("the worker crashed"),
  new errors.InternalError("bug"),
  new errors.BusyError(250),
  new errors.SerializationError(),
  new errors.DatabaseInUseError("db", "one-tab-only", {
    cause: new Error("held"),
  }),
  new errors.AbortError("stop"),
];

/** What the page gets for an error the worker throws. */
function acrossTheBoundary(error: unknown): errors.OpfsqlError {
  return fromWireError(structuredClone(toWireError(error)));
}

describe("errors across the worker boundary", () => {
  it("come back as the same class, with code, message and fields", () => {
    const received = acrossTheBoundary(
      new errors.SqlSyntaxError("bad token", 7, 3),
    );
    expect(received).toBeInstanceOf(errors.SqlSyntaxError);
    expect(received).toBeInstanceOf(errors.OpfsqlError);
    expect(received).toBeInstanceOf(Error);
    expect(received).toMatchObject({
      name: "SqlSyntaxError",
      code: "SYNTAX_ERROR",
      message: "bad token",
      offset: 7,
      length: 3,
    });
    expect(String(received)).toBe("SqlSyntaxError: bad token");
  });

  it("keep every class errors.ts exports", () => {
    for (const errorClass of exportedErrorClasses()) {
      const original = SAMPLES.find(
        (sample) => sample.constructor === errorClass,
      );
      if (original === undefined) {
        throw new Error(`no sample of ${errorClass.name} to send across`);
      }
      const received = acrossTheBoundary(original);
      expect(received.constructor).toBe(errorClass);
      expect(fieldsOf(received)).toEqual(fieldsOf(original));
      expect(received.message).toBe(original.message);
    }
  });

  it("keep the cause chain, foreign errors included", () => {
    class DiskError extends Error {
      override readonly name = "DiskError";
      constructor(readonly sector: number) {
        super(`sector ${String(sector)} is unreadable`);
      }
    }
    const disk = new DiskError(42);
    const error = new errors.IoError("could not read", { cause: disk });
    const received = acrossTheBoundary(error);
    expect(received).toBeInstanceOf(errors.IoError);
    expect(received.cause).toBeInstanceOf(Error);
    expect(received.cause).toMatchObject({
      name: "DiskError",
      message: disk.message,
      sector: 42,
    });
  });

  it("keep a DOMException a DOMException", () => {
    const error = new errors.DatabaseInUseError("db", "one-tab-only", {
      cause: new DOMException("held elsewhere", "InvalidStateError"),
    });
    const received = acrossTheBoundary(error);
    expect(received).toBeInstanceOf(errors.DatabaseInUseError);
    expect(received.cause).toBeInstanceOf(DOMException);
    expect(received.cause).toMatchObject({ name: "InvalidStateError" });
  });

  it("keep where in a script the error happened", () => {
    const error = new errors.SemanticError("no such table");
    Object.defineProperty(error, "statementIndex", {
      value: 2,
      enumerable: true,
    });
    expect(acrossTheBoundary(error).statementIndex).toBe(2);
  });

  it("turn anything else into INTERNAL", () => {
    const received = acrossTheBoundary(new TypeError("boom"));
    expect(received).toMatchObject({ code: "INTERNAL", message: "boom" });
    expect(received).toHaveProperty("cause.name", "TypeError");
    expect(acrossTheBoundary("oops")).toMatchObject({
      code: "INTERNAL",
      message: "oops",
    });
  });
});
