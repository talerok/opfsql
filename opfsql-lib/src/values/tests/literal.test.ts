import { describe, expect, it } from "vitest";
import { InternalError } from "../../errors.js";
import { realLiteral, sqlLiteral, textLiteral } from "../index.js";

describe("SQL literals", () => {
  it("write each value so that it reads back the same", () => {
    expect(
      [
        null,
        true,
        false,
        42,
        -1.5,
        Infinity,
        -Infinity,
        "it's",
        "",
        Uint8Array.from([0xde, 0xad]),
      ].map((value) => sqlLiteral(value)),
    ).toEqual([
      "NULL",
      "TRUE",
      "FALSE",
      "42",
      "-1.5",
      "9e999",
      "-9e999",
      "'it''s'",
      "''",
      "X'dead'",
    ]);
  });

  it("keep a REAL's decimal point, so it reads back as REAL", () => {
    expect(
      [2, -2, 0.5, 1e21, Infinity].map((value) => realLiteral(value)),
    ).toEqual(["2.0", "-2.0", "0.5", "1e+21", "9e999"]);
  });

  it("quote text, and have none for a JSON object or array", () => {
    expect(textLiteral("a'b")).toBe("'a''b'");
    expect(() => sqlLiteral({ a: 1 })).toThrow(InternalError);
  });
});
