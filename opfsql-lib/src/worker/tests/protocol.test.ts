import { describe, expect, it } from "vitest";
import { SerializationError } from "../../engine/types.js";
import { DatabaseInUseError } from "../../store/errors.js";
import { fromWireError, toWireError } from "../protocol.js";

describe("errors across the worker boundary", () => {
  it("keep their name and message", () => {
    for (const original of [
      new DatabaseInUseError("db", null),
      new SerializationError(),
      new TypeError("boom"),
    ]) {
      const received = fromWireError(structuredClone(toWireError(original)));
      expect(received.name).toBe(original.name);
      expect(received.message).toBe(original.message);
    }
  });

  it("turn thrown non-errors into plain errors", () => {
    expect(fromWireError(toWireError("oops"))).toMatchObject({
      name: "Error",
      message: "oops",
    });
  });
});
