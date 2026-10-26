import { beforeEach, describe, expect, it } from "vitest";
import { type physical, ROWID_COLUMN } from "../../sql/plan/index.js";
import type { Index } from "../../schema/index.js";
import type { Value } from "../../values/index.js";
import {
  age,
  amount,
  email,
  type Fixture,
  fixture,
  id,
  name,
  note,
  ORDERS,
  USERS,
} from "./fixtures.js";
import {
  aggregateCall,
  between,
  compare,
  constant,
  Counting,
  excluding,
  filter,
  hashAggregate,
  including,
  indexExtreme,
  indexScan,
  indexUnion,
  keys,
  limit,
  not,
  NULL,
  or,
  parameter,
  rowidList,
  rowidRange,
  type Source,
  source,
  tableScan,
} from "./plans.js";

/** Table indexes of an aggregate's groups and its aggregates. */
const GROUPS = 20;
const AGGREGATES = 21;

let data: Fixture;

beforeEach(async () => {
  data = await fixture();
});

/** Users read with these columns. */
function users(columns: number[] = [0]): Source {
  return source(data.users, USERS, columns);
}

/** The first column of each row a plan returns: the user's id, here. */
function ids(plan: physical.Plan, parameters: Value[] = []): Value[] {
  return data.database.query(plan, parameters).map(([first = null]) => first);
}

function idsInRange(lower: physical.Bound, upper: physical.Bound): Value[] {
  return ids(rowidRange(users(), between(lower, upper)));
}

function ageIndexScan(
  keyRange: physical.KeyRange,
  direction: physical.OrderKey["direction"] = "ascending",
): physical.IndexScan {
  return indexScan(users(), data.byAge, keyRange, { direction });
}

function agesInRange(
  lower: physical.Bound | null,
  upper: physical.Bound | null,
): Value[] {
  return ids(ageIndexScan(keys([], between(lower, upper))));
}

function extremeOf(extreme: "MIN" | "MAX", prefix: Value[] = []): Value[][] {
  const equal = prefix.map((value) => constant(value));
  const index = prefix.length === 0 ? data.byAge : data.byAgeName;
  return data.database.query(indexExtreme(data.users, index, extreme, equal));
}

/** Users aged 30, users 1 and 2, and users from 4 on. */
function branches(): physical.IndexBranch[] {
  const listed = {
    kind: "list",
    values: [constant(1), constant(2)],
  } as const;
  const from4 = between(including(constant(4)), null);
  return [
    { kind: "index", index: data.byAge, keys: keys([constant(30)]) },
    { kind: "rowids", rowids: listed },
    { kind: "rowids", rowids: { kind: "range", range: from4 } },
  ];
}

describe("table scan", () => {
  it("reads every row in rowid order, or from the largest rowid down", () => {
    expect(data.database.query(tableScan(users([0, 1])))).toEqual([
      [1, "Ann"],
      [2, "bob"],
      [3, "Cid"],
      [4, "dan"],
      [5, "Eve"],
    ]);
    const backward = tableScan(users(), { direction: "descending" });
    expect(ids(backward)).toEqual([5, 4, 3, 2, 1]);
  });

  it("outputs the rowid where its columns ask for it", () => {
    const plan = tableScan(users([2, ROWID_COLUMN]));
    expect(data.database.query(plan)).toEqual([
      [30, 1],
      [25, 2],
      [null, 3],
      [30, 4],
      [41, 5],
    ]);
  });

  it("applies its residual to every row, both ways: NULL is not TRUE", () => {
    const residual = [compare(age, ">", constant(26))];
    const forward = tableScan(users([0, 2]), { residual });
    expect(ids(forward)).toEqual([1, 4, 5]);
    const backward = tableScan(users([0, 2]), {
      residual,
      direction: "descending",
    });
    expect(ids(backward)).toEqual([5, 4, 1]);
  });
});

describe("rowid lookup", () => {
  it("returns each listed row once, in rowid order, missing rowids skipped", () => {
    const listed = [constant(3), constant(1), constant(3), constant(99)];
    expect(ids(rowidList(users(), listed))).toEqual([1, 3]);
  });

  it("seeks a listed value as it is, a parameter by its affinity; no other value matches", () => {
    const listed = [
      parameter(0, "numeric"),
      constant(2.5),
      NULL,
      constant("x"),
    ];
    expect(ids(rowidList(users(), listed), ["2"])).toEqual([2]);
  });

  it("reads a range with inclusive and exclusive ends", () => {
    const [two, four] = [constant(2), constant(4)];
    expect(idsInRange(including(two), including(four))).toEqual([2, 3, 4]);
    expect(idsInRange(excluding(two), excluding(four))).toEqual([3]);
    expect(idsInRange(including(two), excluding(four))).toEqual([2, 3]);
    expect(idsInRange(including(four), including(two))).toEqual([]);
    const [low, high] = [constant(1.5), constant(3.5)];
    expect(idsInRange(including(low), including(high))).toEqual([2, 3]);
    expect(idsInRange(excluding(low), excluding(high))).toEqual([2, 3]);
  });

  it("bounds a range by parameters converted by their affinity, by the total order", () => {
    const bounds = between(
      excluding(parameter(0, "numeric")),
      including(parameter(1, "numeric")),
    );
    const plan = rowidRange(users(), bounds);
    expect(ids(plan, ["1", "3"])).toEqual([2, 3]);
    // Every number sorts after booleans and before text.
    expect(ids(plan, [false, "z"])).toEqual([1, 2, 3, 4, 5]);
    expect(ids(plan, ["z", 9])).toEqual([]);
    expect(ids(plan, [null, 3])).toEqual([]);
    expect(ids(plan, [1, null])).toEqual([]);
  });

  it("reads backward from a bound, touching only the rows a LIMIT takes", () => {
    // WHERE id < $1 ORDER BY id DESC LIMIT 2: keyset pagination.
    const counting = new Counting();
    const residual = [compare(counting.of(id), ">", constant(0))];
    const below = between(null, excluding(parameter(0, "numeric")));
    const page = rowidRange(users(), below, {
      residual,
      direction: "descending",
    });
    expect(ids(limit(page, constant(2)), [5])).toEqual([4, 3]);
    expect(counting.calls).toBe(2);
  });

  it("reads a list or a range backward, each row once, by any kind of bound", () => {
    const listed = [constant(3), constant(1), constant(3), constant(99)];
    const descending = { direction: "descending" } as const;
    expect(ids(rowidList(users(), listed, descending))).toEqual([3, 1]);
    const range = (lower: physical.Bound, upper: physical.Bound) =>
      ids(rowidRange(users(), between(lower, upper), descending));
    expect(range(including(constant(1.5)), excluding(constant("z")))).toEqual([
      5, 4, 3, 2,
    ]);
    expect(range(excluding(constant(false)), including(constant(3.5)))).toEqual(
      [3, 2, 1],
    );
    expect(range(including(constant("a")), including(constant(9)))).toEqual([]);
  });

  it("reads an open-ended range and applies its residual", () => {
    const residual = [compare(name, "<>", constant("dan"))];
    const bounds = between(including(constant(3)), null);
    const plan = rowidRange(users([0, 1]), bounds, { residual });
    expect(ids(plan)).toEqual([3, 5]);
  });
});

describe("index scan", () => {
  it("reads the entries equal to a prefix, in key order", () => {
    expect(ids(ageIndexScan(keys([constant(30)])))).toEqual([1, 4]);
  });

  it("reads a range with inclusive and exclusive ends", () => {
    const [low, high] = [constant(25), constant(30)];
    expect(agesInRange(including(low), including(high))).toEqual([2, 1, 4]);
    expect(agesInRange(excluding(low), excluding(high))).toEqual([]);
    expect(agesInRange(excluding(low), including(high))).toEqual([1, 4]);
    expect(agesInRange(including(low), excluding(high))).toEqual([2]);
  });

  it("never finds Cid's NULL age in a range, read either way", () => {
    expect(agesInRange(null, excluding(constant(30)))).toEqual([2]);
    expect(agesInRange(excluding(constant(26)), null)).toEqual([1, 4, 5]);
    const below30 = keys([], between(null, excluding(constant(30))));
    expect(ids(ageIndexScan(below30, "descending"))).toEqual([2]);
    // Without a range, the equal prefix alone: every age, NULL first.
    expect(ids(ageIndexScan(keys([])))).toEqual([3, 2, 1, 4, 5]);
  });

  it("reads backward, from the largest key", () => {
    const from25 = keys([], between(including(constant(25)), null));
    expect(ids(ageIndexScan(from25, "descending"))).toEqual([5, 4, 1, 2]);
  });

  it("ranges over the part after an equal prefix", () => {
    const after = between(excluding(constant("Ann")), null);
    const plan = indexScan(
      users(),
      data.byAgeName,
      keys([constant(30)], after),
    );
    expect(ids(plan)).toEqual([4]);
  });

  it("seeks a parameter as its affinity converts it", () => {
    const equal = ageIndexScan(keys([parameter(0, "numeric")]));
    expect(ids(equal, ["30"])).toEqual([1, 4]);
    expect(ids(equal, ["thirty"])).toEqual([]);
    expect(ids(equal, [null])).toEqual([]);
    const from = keys([], between(including(parameter(0, "numeric")), null));
    expect(ids(ageIndexScan(from), ["30.5"])).toEqual([5]);
  });

  it("seeks an expression key with a text-affinity parameter", () => {
    data.database.insert(data.users, [[6, "5", 50, null]]);
    const plan = indexScan(
      users(),
      data.byLowerName,
      keys([parameter(0, "text")]),
    );
    expect(ids(plan, [5])).toEqual([6]);
    expect(ids(plan, ["ann"])).toEqual([1]);
  });

  it("applies its residual to every row", () => {
    const residual = [not(compare(name, "=", constant("dan")))];
    const plan = indexScan(users([0, 1]), data.byAge, keys([constant(30)]), {
      residual,
    });
    expect(ids(plan)).toEqual([1]);
  });
});

describe("index union", () => {
  it("returns each row its branches find once, in rowid order", () => {
    expect(ids(indexUnion(users(), branches()))).toEqual([1, 2, 4, 5]);
  });

  it("checks its residual, the OR the branches came from, on every row", () => {
    const residual = [
      or(compare(age, "=", constant(30)), compare(email, "=", constant("b@x"))),
    ];
    const plan = indexUnion(users([0, 2, 3]), branches(), residual);
    expect(ids(plan)).toEqual([1, 2, 4]);
  });
});

describe("index extreme", () => {
  it("reads MIN and MAX from either end of the index, skipping NULLs", () => {
    expect(extremeOf("MIN")).toEqual([[25]]);
    expect(extremeOf("MAX")).toEqual([[41]]);
  });

  it("reads the part after an equal prefix, and NULL when none is there", () => {
    data.database.insert(data.users, [[6, null, 70, null]]);
    expect(extremeOf("MIN", [30])).toEqual([["Ann"]]);
    expect(extremeOf("MAX", [30])).toEqual([["dan"]]);
    expect(extremeOf("MIN", [70])).toEqual([[null]]);
    expect(extremeOf("MAX", [70])).toEqual([[null]]);
    expect(extremeOf("MAX", [99])).toEqual([[null]]);
    expect(extremeOf("MIN", [null])).toEqual([[null]]);
  });
});

/** orders indexed on note (ANY) and amount: note holds the text '1', never the number 1. */
function notesIndexed(): Index {
  return data.database.createIndex(
    {
      name: "orders_note_amount",
      table: "orders",
      keys: [
        { kind: "column", column: 3 },
        { kind: "column", column: 2 },
      ],
      isUnique: false,
      origin: "statement",
    },
    data.orders,
  );
}

describe("seek values, matched as they are", () => {
  it("find in an ANY column what a scan with `=` finds: not '1' for 1", () => {
    const byNote = notesIndexed();
    const orders = source(data.orders, ORDERS, [0, 3]);
    const isOne = compare(note, "=", constant(1));
    expect(data.database.query(filter(tableScan(orders), isOne))).toEqual([]);
    const seekOne = indexScan(orders, byNote, keys([constant(1)]), {
      residual: [isOne],
    });
    expect(data.database.query(seekOne)).toEqual([]);
    const seekText = indexScan(orders, byNote, keys([constant("1")]));
    expect(data.database.query(seekText)).toEqual([[12, "1"]]);
  });

  it("give MIN and MAX from an index what an aggregate over a scan gives", () => {
    const byNote = notesIndexed();
    const largest = (value: Value) =>
      data.database.query(
        indexExtreme(data.orders, byNote, "MAX", [constant(value)]),
      );
    const orders = tableScan(source(data.orders, ORDERS, [2, 3]));
    const scanned = hashAggregate(
      [GROUPS, AGGREGATES],
      filter(orders, compare(note, "=", constant(1))),
      [],
      [aggregateCall("max", [amount])],
    );
    expect(data.database.query(scanned)).toEqual([[null]]);
    expect(largest(1)).toEqual([[null]]);
    expect(largest("1")).toEqual([[7]]);
  });
});
