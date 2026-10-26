import { describe, expect, it } from "vitest";
import {
  and,
  between,
  call,
  column,
  compare,
  constant,
  filter,
  get,
  inList,
  NULL,
  or,
  parameter,
  planOf,
  users,
} from "./builders.js";

const u = { tableIndex: 1, table: users };
const id = column(u, "id");
const rowid = column(u, "rowid");
const age = column(u, "age");
const name = column(u, "name");
const email = column(u, "email");
const tag = column(u, "tag");

function scanOf(...conditions: Parameters<typeof filter>[1][]): string {
  return planOf(filter(get(u), ...conditions));
}

describe("rowid lookups", () => {
  it("looks up a rowid equal to a constant, and drops the condition it enforces", () => {
    expect(scanOf(compare(id, "=", constant(5)))).toBe(
      "rowid-lookup users#1 (rowid = 5)",
    );
  });

  it("treats ROWID_COLUMN as the INTEGER PRIMARY KEY", () => {
    expect(scanOf(compare(constant(5), "=", rowid))).toBe(
      "rowid-lookup users#1 (rowid = 5)",
    );
  });

  it("keeps a condition with a parameter, which may be NULL at run time", () => {
    expect(scanOf(compare(id, "=", parameter(0)))).toBe(
      "rowid-lookup users#1 (rowid = $1) residual: id = $1",
    );
  });

  it("looks up the rowids of an IN list", () => {
    expect(scanOf(inList(id, constant(1), constant(2), constant(3)))).toBe(
      "rowid-lookup users#1 (rowid IN (1, 2, 3))",
    );
  });

  it("reads a range of rowids", () => {
    expect(scanOf(between(id, constant(1), constant(9)))).toBe(
      "rowid-lookup users#1 (rowid >= 1 AND rowid <= 9)",
    );
  });

  it("never seeks a rowid with a constant that is no number", () => {
    const text = constant("five", "ANY");
    expect(scanOf(compare(id, "=", text))).toBe(
      "table-scan users#1 residual: id = 'five'",
    );
  });
});

describe("index scans", () => {
  it("prefers a unique index's whole key to an equality prefix", () => {
    const plan = scanOf(
      compare(age, "=", constant(30)),
      compare(email, "=", constant("a@b")),
    );
    expect(plan).toBe(
      "index-scan users#1 using users_email (email = 'a@b') residual: age = 30",
    );
  });

  it("prefers the longer equality prefix", () => {
    const plan = scanOf(
      compare(age, "=", constant(30)),
      compare(name, "=", constant("bob")),
    );
    expect(plan).toBe(
      "index-scan users#1 using users_name_age (name = 'bob', age = 30)",
    );
  });

  it("follows an equality prefix with a range on the next part", () => {
    const plan = scanOf(
      compare(name, "=", constant("bob")),
      compare(age, ">", constant(18)),
      compare(age, "<=", constant(65)),
    );
    expect(plan).toBe(
      "index-scan users#1 using users_name_age (name = 'bob', age > 18 AND age <= 65) residual: age <= 65",
    );
  });

  it("keeps an upper bound on a column that may hold NULL, which sorts first", () => {
    expect(scanOf(compare(age, "<", constant(18)))).toBe(
      "index-scan users#1 using users_age (age < 18) residual: age < 18",
    );
  });

  it("drops an upper bound on a NOT NULL column", () => {
    expect(scanOf(compare(email, "<", constant("m")))).toBe(
      "index-scan users#1 using users_email (email < 'm')",
    );
  });

  it("lets a parameter bound a range, keeping the condition", () => {
    expect(scanOf(compare(age, ">=", parameter(0)))).toBe(
      "index-scan users#1 using users_age (age >= $1) residual: age >= $1",
    );
  });

  it("matches an expression key to a condition written the same way", () => {
    const lowered = call("lower", name);
    expect(scanOf(compare(lowered, "=", constant("bob")))).toBe(
      "index-scan users#1 using users_lower_name (lower(name) = 'bob')",
    );
  });

  it("never seeks an ANY column with a typed value, which = converts per row", () => {
    // tag 5 = '5' is true by affinity; an index of 5 has no entry '5'.
    expect(scanOf(compare(tag, "=", constant("5")))).toBe(
      "table-scan users#1 residual: tag = '5'",
    );
    expect(scanOf(compare(tag, "=", parameter(0)))).toBe(
      "index-scan users#1 using users_tag (tag = $1) residual: tag = $1",
    );
  });

  it("does not seek with NULL, which = never matches", () => {
    expect(scanOf(compare(age, "=", NULL))).toBe(
      "table-scan users#1 residual: age = NULL",
    );
  });

  it("does not seek with <>", () => {
    expect(scanOf(compare(age, "<>", constant(3)))).toBe(
      "table-scan users#1 residual: age <> 3",
    );
  });
});

describe("index unions", () => {
  it("reads an OR of indexable branches through each, keeping the OR", () => {
    const plan = scanOf(
      or(
        compare(id, "=", constant(1)),
        compare(email, "=", constant("x")),
        and(
          compare(name, "=", constant("bob")),
          compare(age, ">", constant(3)),
        ),
      ),
    );
    expect(plan).toBe(
      "index-union users#1 [rowid = 1; using users_email (email = 'x'); using users_name_age (name = 'bob', age > 3)] residual: id = 1 OR email = 'x' OR name = 'bob' AND age > 3",
    );
  });

  it("reads an IN list on an indexed column as a union of its items", () => {
    expect(scanOf(inList(age, constant(1), parameter(0)))).toBe(
      "index-union users#1 [using users_age (age = 1); using users_age (age = $1)] residual: age IN (1, $1)",
    );
  });

  it("scans when one branch has no index", () => {
    const plan = scanOf(
      or(
        compare(id, "=", constant(1)),
        compare(column(u, "tag"), ">", constant("a")),
      ),
    );
    expect(plan).toBe("table-scan users#1 residual: id = 1 OR tag > 'a'");
  });
});

describe("parameters never choose the index (rule 5)", () => {
  it("chooses the same index for a parameter as for a constant", () => {
    const withConstant = scanOf(
      compare(age, "=", constant(30)),
      compare(name, ">", constant("a")),
    );
    const withParameter = scanOf(
      compare(age, "=", parameter(0)),
      compare(name, ">", parameter(1)),
    );
    expect(withConstant).toMatch(/using users_age \(age = 30\)/u);
    expect(withParameter).toMatch(/using users_age \(age = \$1\)/u);
  });
});
