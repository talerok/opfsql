import { describe, expect, it } from "vitest";
import { SemanticError } from "../../errors.js";
import {
  aggregateFunction,
  isAggregateFunction,
  lookupFunction,
  resolveCall,
  scalarFunction,
  sqlFunction,
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
    expect(() => resolveCall(aggregateFunction("sum"), ["TEXT"])).toThrow(
      SemanticError,
    );
    expect(() => resolveCall(aggregateFunction("avg"), ["BOOLEAN"])).toThrow(
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
    expect(resolveCall(scalarFunction("coalesce"), ["INTEGER", "REAL"])).toBe(
      "REAL",
    );
    expect(resolveCall(scalarFunction("coalesce"), ["INTEGER", "ANY"])).toBe(
      "ANY",
    );
    expect(resolveCall(aggregateFunction("sum"), ["INTEGER"])).toBe("INTEGER");
    expect(resolveCall(aggregateFunction("sum"), ["REAL"])).toBe("REAL");
    expect(resolveCall(aggregateFunction("avg"), ["INTEGER"])).toBe("REAL");
    expect(resolveCall(aggregateFunction("count"), [])).toBe("INTEGER");
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

  it("knows every name, scalar or aggregate, or fails as SQLite does", () => {
    expect(sqlFunction("upper").kind).toBe("scalar");
    expect(sqlFunction("SUM").kind).toBe("aggregate");
    expect(() => sqlFunction("nosuchfn")).toThrow(
      new SemanticError("no such function: nosuchfn"),
    );
  });

  it("marks random() volatile, so it is never folded", () => {
    expect(scalarFunction("random").isVolatile).toBe(true);
    expect(scalarFunction("abs").isVolatile).toBeUndefined();
  });

  it("gives random() a new safe integer, odd or even, each call", () => {
    const values = Array.from({ length: 200 }, () => call("random", []));
    expect(values.every((value) => Number.isSafeInteger(value))).toBe(true);
    const parities = new Set(
      values.map((value) => Math.abs(Number(value) % 2)),
    );
    expect(parities.size).toBe(2);
  });
});
