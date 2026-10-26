import { describe, expect, it } from "vitest";
import {
  InternalError,
  IoError,
  MisuseError,
  StorageFullError,
  toOpfsqlError,
} from "../errors.js";

// One translation for every path: the engine and the worker host give a
// failure from outside opfsql the same code.

describe("toOpfsqlError", () => {
  it("passes an OpfsqlError through as it is", () => {
    const error = new MisuseError("closed");
    expect(toOpfsqlError(error)).toBe(error);
  });

  it("makes a quota DOMException STORAGE_FULL, and any other IO_ERROR", () => {
    const quota = new DOMException("full", "QuotaExceededError");
    const full = toOpfsqlError(quota);
    expect(full).toBeInstanceOf(StorageFullError);
    expect(full.cause).toBe(quota);
    const state = new DOMException("gone", "InvalidStateError");
    expect(toOpfsqlError(state)).toBeInstanceOf(IoError);
  });

  it("makes anything else INTERNAL: a bug", () => {
    const bug = new TypeError("x is undefined");
    const internal = toOpfsqlError(bug);
    expect(internal).toBeInstanceOf(InternalError);
    expect(internal).toMatchObject({ message: "x is undefined", cause: bug });
    expect(toOpfsqlError("oops")).toMatchObject({
      code: "INTERNAL",
      message: "oops",
    });
  });
});
