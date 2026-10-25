import { describe, expect, it } from "vitest";
import { DatatypeMismatchError, SemanticError } from "../../errors.js";
import { md5 } from "../md5.js";
import { roundHalfAwayFromZero } from "../numeric.js";
import { call } from "./helpers.js";

const bytes = (...values: number[]): Uint8Array => new Uint8Array(values);

describe("text functions", () => {
  it("are NULL for a NULL argument", () => {
    for (const name of ["upper", "lower", "length", "trim", "ltrim", "rtrim"]) {
      expect(call(name, [null])).toBeNull();
    }
    expect(call("substr", ["abc", null])).toBeNull();
    expect(call("replace", ["abc", "b", null])).toBeNull();
    expect(call("instr", [null, "a"])).toBeNull();
  });

  it("UPPER and LOWER convert case", () => {
    expect(call("upper", ["héllo"])).toBe("HÉLLO");
    expect(call("lower", ["ABC"])).toBe("abc");
    expect(call("upper", [12])).toBe("12");
  });

  it("LENGTH counts characters in text and bytes in BLOBs", () => {
    expect(call("length", ["héllo"])).toBe(5);
    expect(call("length", ["😀"])).toBe(1);
    expect(call("length", [bytes(1, 2, 3)])).toBe(3);
    expect(call("length", [1.5])).toBe(3);
  });

  it("TRIM removes spaces, or the characters given", () => {
    expect(call("trim", ["  a b  "])).toBe("a b");
    expect(call("trim", ["\ta\t"])).toBe("\ta\t");
    expect(call("ltrim", ["  a  "])).toBe("a  ");
    expect(call("rtrim", ["  a  "])).toBe("  a");
    expect(call("trim", ["xxaxyx", "xy"])).toBe("a");
    expect(call("rtrim", ["a😀😀", "😀"])).toBe("a");
  });

  it("SUBSTR follows SQLite for every kind of start and length", () => {
    expect(call("substr", ["hello", 2])).toBe("ello");
    expect(call("substr", ["hello", 2, 3])).toBe("ell");
    expect(call("substr", ["hello", 0, 2])).toBe("h");
    expect(call("substr", ["hello", 0])).toBe("hello");
    expect(call("substr", ["hello", -3])).toBe("llo");
    expect(call("substr", ["hello", -3, 2])).toBe("ll");
    expect(call("substr", ["hello", -7, 3])).toBe("h");
    expect(call("substr", ["hello", 3, -2])).toBe("he");
    expect(call("substr", ["hello", 10])).toBe("");
    expect(call("substr", ["h😀llo", 2, 1])).toBe("😀");
    expect(call("substr", ["hello", 2.9, 1])).toBe("e");
    expect(call("substring", ["hello", 1, 1])).toBe("h");
  });

  it("SUBSTR counts bytes in a BLOB", () => {
    expect(call("substr", [bytes(1, 2, 3, 4), 2, 2])).toEqual(bytes(2, 3));
  });

  it("REPLACE replaces every occurrence, literally", () => {
    expect(call("replace", ["a-b-c", "-", "+"])).toBe("a+b+c");
    expect(call("replace", ["abc", "", "x"])).toBe("abc");
    expect(call("replace", ["abc", "b", "$&$&"])).toBe("a$&$&c");
  });

  it("CONCAT skips NULLs; CONCAT_WS puts a separator between", () => {
    expect(call("concat", ["a", null, 1, true])).toBe("a1true");
    expect(call("concat", [null])).toBe("");
    expect(call("concat_ws", [", ", "a", null, "b"])).toBe("a, b");
    expect(call("concat_ws", [null, "a"])).toBeNull();
  });

  it("INSTR finds the 1-based position, or 0", () => {
    expect(call("instr", ["hello", "l"])).toBe(3);
    expect(call("instr", ["hello", "z"])).toBe(0);
    expect(call("instr", ["😀ab", "a"])).toBe(2);
    expect(call("instr", [bytes(1, 2, 3), bytes(2, 3)])).toBe(2);
  });

  it("HEX writes the bytes of a value in uppercase", () => {
    expect(call("hex", [bytes(0xca, 0xfe)])).toBe("CAFE");
    expect(call("hex", ["a"])).toBe("61");
    expect(call("hex", [12])).toBe("3132");
    expect(call("hex", [null])).toBe("");
  });

  it("refuse values that have no text", () => {
    expect(() => call("upper", [bytes(1)], { types: ["ANY"] })).toThrow(
      DatatypeMismatchError,
    );
  });
});

describe("numeric functions", () => {
  it("ABS keeps the type and fails on what is not a number", () => {
    expect(call("abs", [-3])).toBe(3);
    expect(call("abs", [-2.5])).toBe(2.5);
    expect(call("abs", [null])).toBeNull();
    expect(call("abs", ["-4"], { types: ["ANY"] })).toBe(4);
    expect(() => call("abs", ["x"], { types: ["ANY"] })).toThrow(
      DatatypeMismatchError,
    );
  });

  it("ROUND rounds halves away from zero", () => {
    expect(call("round", [2.5])).toBe(3);
    expect(call("round", [-2.5])).toBe(-3);
    // toBe compares with Object.is: this is +0, not -0.
    expect(call("round", [-0.4])).toBe(0);
    expect(call("round", [1.2345, 2])).toBeCloseTo(1.23, 10);
    expect(call("round", [-1.235, 2])).toBeCloseTo(-1.24, 10);
    expect(call("round", [7])).toBe(7);
    expect(call("round", [1.5, null])).toBeNull();
  });

  it("ROUND clamps the number of digits and leaves huge numbers alone", () => {
    expect(roundHalfAwayFromZero(1.25, -1)).toBe(1);
    expect(roundHalfAwayFromZero(1e300, 2)).toBe(1e300);
    expect(roundHalfAwayFromZero(Infinity, 0)).toBe(Infinity);
  });

  it("FLOOR, CEIL and SIGN", () => {
    expect(call("floor", [-1.5])).toBe(-2);
    expect(call("ceil", [-1.5])).toBe(-1);
    expect(call("ceil", [-0.5])).toBe(0);
    expect(call("ceiling", [1.2])).toBe(2);
    expect(call("sign", [-7])).toBe(-1);
    expect(call("sign", [0])).toBe(0);
  });
});

describe("conditional functions", () => {
  it("COALESCE and IFNULL give the first non-NULL argument", () => {
    expect(call("coalesce", [null, null, 3, 4])).toBe(3);
    expect(call("coalesce", [null, null])).toBeNull();
    expect(call("ifnull", [null, "b"])).toBe("b");
    expect(call("ifnull", ["a", "b"])).toBe("a");
  });

  it("NULLIF is NULL when its arguments are equal", () => {
    expect(call("nullif", [1, 1])).toBeNull();
    expect(call("nullif", [1, 2])).toBe(1);
    expect(call("nullif", [null, 1])).toBeNull();
    expect(call("nullif", [1, null])).toBe(1);
    expect(call("nullif", ["a", "A"])).toBe("a");
  });

  it("IIF chooses by a BOOLEAN condition", () => {
    expect(call("iif", [true, "yes", "no"])).toBe("yes");
    expect(call("iif", [false, "yes", "no"])).toBe("no");
    expect(call("iif", [null, "yes", "no"])).toBe("no");
    expect(call("iif", [false, "yes"])).toBeNull();
    expect(() =>
      call("iif", [1, "yes", "no"], { types: ["ANY", "TEXT", "TEXT"] }),
    ).toThrow(DatatypeMismatchError);
  });

  it("TYPEOF names the kind of a value", () => {
    expect(call("typeof", [null])).toBe("null");
    expect(call("typeof", [1])).toBe("integer");
    expect(call("typeof", [1.5])).toBe("real");
    expect(call("typeof", [2], { types: ["REAL"] })).toBe("real");
    expect(call("typeof", ["a"])).toBe("text");
    expect(call("typeof", [bytes(1)])).toBe("blob");
    expect(call("typeof", [true])).toBe("boolean");
    expect(call("typeof", [{ a: 1 }])).toBe("json");
  });
});

function like(text: string, pattern: string, escape?: string) {
  return call(
    "like",
    escape === undefined ? [pattern, text] : [pattern, text, escape],
  );
}

describe("LIKE", () => {
  it("matches % and _", () => {
    expect(like("hello", "h%")).toBe(true);
    expect(like("hello", "h_llo")).toBe(true);
    expect(like("hello", "h_lo")).toBe(false);
    expect(like("", "%")).toBe(true);
    expect(like("a\nb", "a%b")).toBe(true);
    expect(like("😀", "_")).toBe(true);
  });

  it("ignores case for ASCII letters only", () => {
    expect(like("HELLO", "hello")).toBe(true);
    expect(like("É", "é")).toBe(false);
  });

  it("takes regular expression characters literally", () => {
    expect(like("a.c", "a.c")).toBe(true);
    expect(like("abc", "a.c")).toBe(false);
    expect(like("(x)", "(%)")).toBe(true);
  });

  it("supports an ESCAPE character", () => {
    expect(like("50%", String.raw`50\%`, "\\")).toBe(true);
    expect(like("500", String.raw`50\%`, "\\")).toBe(false);
    expect(like("a_b", "a!_b", "!")).toBe(true);
    expect(like("axb", "a!_b", "!")).toBe(false);
    expect(like("a", "a!", "!")).toBe(false);
    expect(() => like("a", "a", "!!")).toThrow(SemanticError);
  });

  it("is NULL for NULL operands", () => {
    expect(call("like", [null, "a"])).toBeNull();
    expect(call("like", ["a", null])).toBeNull();
  });

  it("compiles a constant pattern once and still matches", () => {
    expect(call("like", ["a%", "abc"], { constant: true })).toBe(true);
    expect(call("like", ["a%", "bc"], { constant: true })).toBe(false);
  });
});

describe("JSON functions", () => {
  const document = { a: { b: [10, 20, 30] }, "x y": true, n: null };

  it("JSON_EXTRACT follows a path", () => {
    expect(call("json_extract", [document, "$.a.b[1]"])).toBe(20);
    expect(call("json_extract", [document, "$.a.b[#-1]"])).toBe(30);
    expect(call("json_extract", [document, '$."x y"'])).toBe(true);
    expect(call("json_extract", [document, "$.a"])).toEqual({
      b: [10, 20, 30],
    });
    expect(call("json_extract", [document, "$.missing"])).toBeNull();
    expect(call("json_extract", [document, "$.n"])).toBeNull();
  });

  it("JSON_EXTRACT parses TEXT and returns an array for several paths", () => {
    expect(call("json_extract", ['{"a":1}', "$.a"])).toBe(1);
    expect(call("json_extract", ['{"a":1,"b":2}', "$.a", "$.c"])).toEqual([
      1,
      null,
    ]);
    expect(() => call("json_extract", ["{oops", "$.a"])).toThrow(
      DatatypeMismatchError,
    );
    expect(() => call("json_extract", ['{"a":1}', "a"])).toThrow(SemanticError);
  });

  it("treats a JSON-typed string as a value, not as JSON text", () => {
    expect(
      call("json_extract", ["{oops", "$"], { types: ["JSON", "TEXT"] }),
    ).toBe("{oops");
  });

  it("JSON_TYPE, JSON_ARRAY_LENGTH and JSON_VALID", () => {
    expect(call("json_type", [document])).toBe("object");
    expect(call("json_type", [document, "$.a.b"])).toBe("array");
    expect(call("json_type", [document, "$.a.b[0]"])).toBe("integer");
    expect(call("json_type", [document, "$.n"])).toBe("null");
    expect(call("json_type", [document, "$.nope"])).toBeNull();
    expect(call("json_array_length", [document, "$.a.b"])).toBe(3);
    expect(call("json_array_length", ["[1,2]"])).toBe(2);
    expect(call("json_array_length", [document])).toBe(0);
    expect(call("json_valid", ['{"a":1}'])).toBe(true);
    expect(call("json_valid", ["{oops"])).toBe(false);
    expect(call("json_valid", [null])).toBeNull();
  });
});

describe("MD5", () => {
  const encoder = new TextEncoder();

  it("matches the RFC 1321 test suite", () => {
    expect(md5(encoder.encode(""))).toBe("d41d8cd98f00b204e9800998ecf8427e");
    expect(md5(encoder.encode("abc"))).toBe("900150983cd24fb0d6963f7d28e17f72");
    expect(md5(encoder.encode("message digest"))).toBe(
      "f96b697d7cb7938d525a2f31aaf161d0",
    );
    expect(
      md5(
        encoder.encode(
          "12345678901234567890123456789012345678901234567890123456789012345678901234567890",
        ),
      ),
    ).toBe("57edf4a22be3c955ac49da2e2107b67a");
  });

  it("hashes text as UTF-8, JSON canonically and BLOBs as bytes", () => {
    expect(call("md5", ["hello world"])).toBe(
      "5eb63bbbe01eeed093cb22bb8f5acdc3",
    );
    expect(call("md5", [{ b: 1, a: 2 }])).toBe(call("md5", ['{"a":2,"b":1}']));
    expect(call("md5", [new Uint8Array([0x61, 0x62, 0x63])])).toBe(
      "900150983cd24fb0d6963f7d28e17f72",
    );
    expect(call("md5", [null])).toBeNull();
  });
});
