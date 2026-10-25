import { describe, expect, it } from "vitest";
import { NotSupportedError, SqlSyntaxError } from "../../../errors.js";
import { tokenize } from "../lexer.js";

/** Each token as `kind:value`, without the final end token. */
function kinds(sql: string): string[] {
  return tokenize(sql)
    .filter((token) => token.kind !== "end")
    .map((token) => `${token.kind}:${token.value}`);
}

function lexError(sql: string): SqlSyntaxError {
  try {
    tokenize(sql);
  } catch (error) {
    if (error instanceof SqlSyntaxError) return error;
    throw error;
  }
  throw new Error(`no error from ${sql}`);
}

describe("tokens", () => {
  it("keeps offsets and the text as written", () => {
    const tokens = tokenize("SELECT  a1,'x''y'");
    expect(
      tokens.map(({ kind, text, start, end }) => [kind, text, start, end]),
    ).toEqual([
      ["keyword", "SELECT", 0, 6],
      ["identifier", "a1", 8, 10],
      ["symbol", ",", 10, 11],
      ["string", "'x''y'", 11, 17],
      ["end", "", 17, 17],
    ]);
  });

  it("reads keywords in any case, upper-cased, and other words as identifiers", () => {
    expect(kinds("select From wHeRe foo")).toEqual([
      "keyword:SELECT",
      "keyword:FROM",
      "keyword:WHERE",
      "identifier:foo",
    ]);
  });

  it("does not take names of Object.prototype for keywords", () => {
    expect(kinds("constructor __proto__ toString hasOwnProperty")).toEqual([
      "identifier:constructor",
      "identifier:__proto__",
      "identifier:toString",
      "identifier:hasOwnProperty",
    ]);
  });

  it("reads words that used to be rejected as plain words", () => {
    expect(kinds("partition window full")).toEqual([
      "identifier:partition",
      "keyword:WINDOW",
      "keyword:FULL",
    ]);
  });

  it("reads non-ASCII characters and $ inside identifiers, as SQLite does", () => {
    expect(kinds("имя a$b _x 😀")).toEqual([
      "identifier:имя",
      "identifier:a$b",
      "identifier:_x",
      "identifier:😀",
    ]);
  });

  it("reads the longest symbol", () => {
    expect(kinds("<= >= <> != == || << >> -> ->> < > = |")).toEqual(
      [
        "<=",
        ">=",
        "<>",
        "!=",
        "==",
        "||",
        "<<",
        ">>",
        "->",
        "->>",
        "<",
        ">",
        "=",
        "|",
      ].map((symbol) => `symbol:${symbol}`),
    );
  });

  it("skips comments", () => {
    expect(kinds("a -- line\n/* block\n */ b")).toEqual([
      "identifier:a",
      "identifier:b",
    ]);
  });
});

describe("numbers", () => {
  it.each([
    ["42", "integer:42"],
    ["0x10", "integer:0x10"],
    ["0XfF", "integer:0XfF"],
    ["1.5", "real:1.5"],
    [".5", "real:.5"],
    ["5.", "real:5."],
    ["1e3", "real:1e3"],
    ["2.5E-1", "real:2.5E-1"],
    ["1e+2", "real:1e+2"],
  ])("reads %s whole", (sql, token) => {
    expect(kinds(sql)).toEqual([token]);
  });

  it.each(["1e3x", "0x1g", "1_000", "1e", "12abc", "0x"])(
    "rejects %s, a number running into a letter",
    (sql) => {
      const error = lexError(sql);
      expect(error.message).toMatch(/^invalid number '/);
      expect(error.offset).toBe(0);
    },
  );

  it("reads a dot after a name as a dot, not a number", () => {
    expect(kinds("t.a")).toEqual(["identifier:t", "symbol:.", "identifier:a"]);
  });
});

describe("strings, names and blobs", () => {
  it("unescapes '' in strings and \"\" in quoted names", () => {
    expect(kinds(`'it''s' "a""b" ''`)).toEqual([
      "string:it's",
      'quoted:a"b',
      "string:",
    ]);
  });

  it("reads blobs as hex digits", () => {
    expect(kinds("x'CAFE' X'00' x''")).toEqual([
      "blob:CAFE",
      "blob:00",
      "blob:",
    ]);
  });

  it("rejects malformed blobs", () => {
    expect(lexError("x'ABC'").message).toBe(
      "a blob literal needs an even number of hex digits",
    );
    const error = lexError("x'GG'");
    expect([error.offset, error.length]).toEqual([0, 5]);
  });

  it("rejects an empty quoted name", () => {
    expect(lexError('SELECT ""').message).toBe("a quoted name cannot be empty");
  });

  it.each([
    ["SELECT 'abc", "unterminated string", 7, 4],
    ['SELECT "abc', "unterminated quoted name", 7, 4],
    ["SELECT 1 /* never closed", "unterminated comment", 9, 15],
    ["SELECT #", "unexpected character '#'", 7, 1],
    ["SELECT !", "unexpected character '!'", 7, 1],
    ["SELECT ^", "unexpected character '^'", 7, 1],
  ])("locates the error in %s", (sql, message, offset, length) => {
    const error = lexError(sql);
    expect([error.message, error.offset, error.length]).toEqual([
      message,
      offset,
      length,
    ]);
  });
});

describe("parameters", () => {
  it("reads $n, ?n and ?", () => {
    expect(kinds("$1 ?2 ? $10")).toEqual([
      "parameter:1",
      "parameter:2",
      "parameter:",
      "parameter:10",
    ]);
  });

  it("does not support named parameters", () => {
    for (const sql of [":name", "@name", "$name"]) {
      expect(() => tokenize(sql)).toThrow(NotSupportedError);
    }
  });

  it("rejects a lone $", () => {
    expect(lexError("$").message).toBe("unexpected character '$'");
  });
});
