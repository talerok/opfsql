import { describe, expect, it } from "vitest";
import {
  column,
  compare,
  constant,
  get,
  join,
  lines,
  orders,
  planOf,
  plus,
  random,
  users,
} from "./builders.js";

const u = { tableIndex: 1, table: users, alias: "u" };
const o = { tableIndex: 2, table: orders, alias: "o" };
const id = column(u, "id");
const userId = column(o, "user_id");

describe("join planning", () => {
  it("hashes on an equality with one side's columns on each side, either way round", () => {
    expect(
      planOf(join("inner", get(u), get(o), compare(userId, "=", id))),
    ).toBe(
      lines(
        "hash-join inner on u.id = o.user_id",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });

  it("keeps several keys, in the order written", () => {
    const plan = join(
      "inner",
      get(u),
      get(o),
      compare(id, "=", userId),
      compare(column(u, "age"), "=", column(o, "id")),
    );
    expect(planOf(plan)).toBe(
      lines(
        "hash-join inner on u.id = o.user_id, u.age = o.id",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });

  it("keeps everything that is not a key as the residual, never dropping it", () => {
    const plan = join(
      "inner",
      get(u),
      get(o),
      compare(id, "=", userId),
      compare(column(o, "total"), ">", column(u, "age")),
    );
    expect(planOf(plan)).toBe(
      lines(
        "hash-join inner on u.id = o.user_id residual: (o.total > u.age)",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });

  it("keeps a LEFT join's residual in the join, deciding matches", () => {
    const plan = join(
      "left",
      get(u),
      get(o),
      compare(id, "=", userId),
      compare(column(o, "total"), ">", column(u, "age")),
    );
    expect(planOf(plan)).toBe(
      lines(
        "hash-join left on u.id = o.user_id residual: (o.total > u.age)",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });

  it("loops when no condition is an equality key", () => {
    const plan = join("inner", get(u), get(o), compare(id, "<", userId));
    expect(planOf(plan)).toBe(
      lines(
        "nested-loop-join inner on (u.id < o.user_id)",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });

  it("does not take an equality reading both sides on one side as a key", () => {
    const plan = join(
      "inner",
      get(u),
      get(o),
      compare(plus(id, userId), "=", constant(3)),
    );
    expect(planOf(plan)).toBe(
      lines(
        "nested-loop-join inner on ((u.id + o.user_id) = 3)",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });

  it("does not take a volatile equality as a key", () => {
    const plan = join(
      "inner",
      get(u),
      get(o),
      compare(plus(id, random()), "=", userId),
    );
    expect(planOf(plan)).toBe(
      lines(
        "nested-loop-join inner on ((u.id + random()) = o.user_id)",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });

  it("pairs every row of a cross join", () => {
    expect(planOf(join("inner", get(u), get(o)))).toBe(
      lines(
        "nested-loop-join inner",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });

  it("keeps the FROM order, left-deep", () => {
    const p = { tableIndex: 3, table: users, alias: "p" };
    const plan = join(
      "inner",
      join("inner", get(u), get(o), compare(id, "=", userId)),
      get(p),
      compare(column(p, "id"), "=", userId),
    );
    expect(planOf(plan)).toBe(
      lines(
        "hash-join inner on o.user_id = p.id",
        "  hash-join inner on u.id = o.user_id",
        "    table-scan users#1",
        "    table-scan orders#2",
        "  table-scan users#3",
      ),
    );
  });
});
