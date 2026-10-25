import { describe, expect, it } from "vitest";
import {
  AbortError,
  BusyError,
  DatabaseInUseError,
  DatatypeMismatchError,
  IntegerOverflowError,
  InternalError,
  MisuseError,
  NotSupportedError,
  OpfsqlError,
  SemanticError,
  SerializationError,
  SqlSyntaxError,
  TransactionAbortedError,
} from "../../errors.js";
import { RowTooLargeError } from "../../store/errors.js";
import { fromWireError, toWireError } from "../wire-error.js";

/** An error's own fields, as a plain object. */
function fieldsOf(error: Error): Record<string, unknown> {
  return Object.fromEntries(Object.entries(error));
}

/** What the page gets for an error the worker throws. */
function acrossTheBoundary(error: unknown): OpfsqlError {
  return fromWireError(structuredClone(toWireError(error)));
}

describe("errors across the worker boundary", () => {
  it("come back as the same class, with code, message and fields", () => {
    const received = acrossTheBoundary(new SqlSyntaxError("bad token", 7, 3));
    expect(received).toBeInstanceOf(SqlSyntaxError);
    expect(received).toBeInstanceOf(OpfsqlError);
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
    const samples: OpfsqlError[] = [
      new OpfsqlError("CORRUPT", "damaged"),
      new BusyError(250),
      new DatabaseInUseError("db", { cause: new Error("held") }),
      new NotSupportedError("WINDOW"),
      new AbortError("stop"),
      new SerializationError(),
      new TransactionAbortedError(),
      new MisuseError("closed"),
      new SemanticError("no such table"),
      new DatatypeMismatchError("not an integer"),
      new IntegerOverflowError(),
      new InternalError("bug"),
    ];
    for (const original of samples) {
      const received = acrossTheBoundary(original);
      expect(received.constructor).toBe(original.constructor);
      expect(fieldsOf(received)).toEqual(fieldsOf(original));
      expect(received.message).toBe(original.message);
    }
  });

  it("keep the cause chain, foreign errors included", () => {
    const storage = new RowTooLargeError("t", 5000, 2025);
    const error = new OpfsqlError("TOO_LARGE", storage.message, {
      cause: storage,
    });
    const received = acrossTheBoundary(error);
    expect(received.code).toBe("TOO_LARGE");
    expect(received.cause).toBeInstanceOf(Error);
    expect(received.cause).toMatchObject({
      name: "RowTooLargeError",
      message: storage.message,
      table: "t",
      size: 5000,
      limit: 2025,
    });
  });

  it("keep a DOMException a DOMException", () => {
    const error = new DatabaseInUseError("db", {
      cause: new DOMException("held elsewhere", "InvalidStateError"),
    });
    const received = acrossTheBoundary(error);
    expect(received).toBeInstanceOf(DatabaseInUseError);
    expect(received.cause).toBeInstanceOf(DOMException);
    expect(received.cause).toMatchObject({ name: "InvalidStateError" });
  });

  it("keep where in a script the error happened", () => {
    const error = new SemanticError("no such table");
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
