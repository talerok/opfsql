import { describe, expect, it } from "vitest";
import { SemanticError } from "../../errors.js";
import {
  isAggregateFunction,
  lookupFunction,
  resolveCall,
  scalarFunction,
} from "../index.js";
import { call } from "./helpers.js";

function failing(): never {
  throw new Error("evaluated");
}

describe("the registry", () => {
  it("finds functions by name in any case", () => {
    expect(lookupFunction("UPPER")?.name).toBe("upper");
    expect(lookupFunction("Count")?.kind).toBe("aggregate");
    expect(lookupFunction("nosuchfn")).toBeUndefined();
  });

  it("tells aggregates from scalar functions", () => {
    expect(isAggregateFunction("sum")).toBe(true);
    expect(isAggregateFunction("abs")).toBe(false);
    expect(isAggregateFunction("nosuchfn")).toBe(false);
  });

  it("rejects unknown functions", () => {
    expect(() => scalarFunction("nosuchfn")).toThrow(
      new SemanticError("no such function: nosuchfn"),
    );
    expect(() => scalarFunction("sum")).toThrow(SemanticError);
  });

  it("rejects a wrong number of arguments", () => {
    const upper = scalarFunction("upper");
    expect(() => resolveCall(upper, [])).toThrow(
      new SemanticError("wrong number of arguments to function upper()"),
    );
    expect(() => resolveCall(scalarFunction("substr"), ["TEXT"])).toThrow(
      SemanticError,
    );
    expect(() =>
      resolveCall(scalarFunction("substr"), [
        "TEXT",
        "INTEGER",
        "INTEGER",
        "INTEGER",
      ]),
    ).toThrow(SemanticError);
    expect(() => resolveCall(scalarFunction("coalesce"), ["TEXT"])).toThrow(
      SemanticError,
    );
  });

  it("accepts any number of rest arguments", () => {
    const coalesce = scalarFunction("coalesce");
    expect(
      resolveCall(coalesce, ["NULL", "INTEGER", "INTEGER", "INTEGER"]),
    ).toBe("INTEGER");
  });

  it("rejects arguments of types the function does not take", () => {
    expect(() => resolveCall(scalarFunction("abs"), ["TEXT"])).toThrow(
      SemanticError,
    );
    expect(() => resolveCall(scalarFunction("upper"), ["BLOB"])).toThrow(
      SemanticError,
    );
    expect(() => resolveCall(lookupFunction("sum")!, ["TEXT"])).toThrow(
      SemanticError,
    );
    expect(() => resolveCall(lookupFunction("avg")!, ["BOOLEAN"])).toThrow(
      SemanticError,
    );
  });

  it("derives result types from argument types", () => {
    expect(resolveCall(scalarFunction("abs"), ["INTEGER"])).toBe("INTEGER");
    expect(resolveCall(scalarFunction("abs"), ["REAL"])).toBe("REAL");
    expect(resolveCall(scalarFunction("round"), ["INTEGER"])).toBe("REAL");
    expect(resolveCall(scalarFunction("floor"), ["INTEGER"])).toBe("INTEGER");
    expect(resolveCall(scalarFunction("floor"), ["REAL"])).toBe("REAL");
    expect(resolveCall(scalarFunction("substr"), ["BLOB", "INTEGER"])).toBe(
      "BLOB",
    );
    expect(resolveCall(scalarFunction("coalesce"), ["INTEGER", "TEXT"])).toBe(
      "ANY",
    );
    expect(resolveCall(lookupFunction("sum")!, ["INTEGER"])).toBe("INTEGER");
    expect(resolveCall(lookupFunction("sum")!, ["REAL"])).toBe("REAL");
    expect(resolveCall(lookupFunction("avg")!, ["INTEGER"])).toBe("REAL");
    expect(resolveCall(lookupFunction("count")!, [])).toBe("INTEGER");
  });

  it("evaluates lazy functions only as far as needed", () => {
    const coalesce = scalarFunction("coalesce").implement({
      argumentTypes: ["INTEGER", "INTEGER"],
      constants: [undefined, undefined],
    });
    expect(coalesce.kind).toBe("lazy");
    if (coalesce.kind !== "lazy") return;
    expect(coalesce.call([() => 1, failing], null)).toBe(1);
  });

  it("is consistent with the helper's direct calls", () => {
    expect(call("upper", ["abc"])).toBe("ABC");
  });
});
