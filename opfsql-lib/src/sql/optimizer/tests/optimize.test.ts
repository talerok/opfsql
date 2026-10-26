import { describe, expect, it } from "vitest";
import { InternalError } from "../../../errors.js";
import {
  formatPhysicalPlan,
  type logical,
  type physical,
  ROWID_COLUMN,
  type Statement,
} from "../../plan/index.js";
import { type KeyBinder, optimize } from "../index.js";
import {
  aggregate,
  aggregateCall,
  and,
  asc,
  bindKey,
  call,
  column,
  compare,
  constant,
  exists,
  filter,
  get,
  inSubquery,
  join,
  limit,
  lines,
  nullCheck,
  order,
  orders,
  parameter,
  planOf,
  plus,
  project,
  query,
  scalarSubquery,
  schema,
  TRUE,
  users,
  usersEmail,
  values,
} from "./builders.js";

const u = { tableIndex: 1, table: users, alias: "u" };
const o = { tableIndex: 2, table: orders, alias: "o" };
const age = column(u, "age");

function optimized(
  statement: Statement<logical.Plan>,
): Statement<physical.Plan> {
  return optimize(statement, schema, bindKey);
}

/** The physical plan of a DML statement's source. */
function sourceOf(statement: Statement<physical.Plan>): string {
  if (!("source" in statement)) {
    throw new InternalError(`${statement.kind} has no source`);
  }
  return formatPhysicalPlan(statement.source);
}

/** `value`, frozen to its leaves, so a pass that changed it would throw. */
function deepFreeze<T>(value: T): T {
  if (value instanceof Object && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
}

/** DELETE FROM users WHERE conditions: every column, then the rowid. */
function deleteWhere(
  ...conditions: logical.Expression[]
): Statement<physical.Plan> {
  const target = get(u, "id", "name", "age", "email", "tag", "rowid");
  const source = filter(target, ...conditions);
  return optimized({
    kind: "delete",
    table: users,
    tableIndex: 1,
    source,
    parameterCount: 1,
  });
}

/** The columns a DELETE's physical source reads. */
function columnsRead(statement: Statement<physical.Plan>): readonly number[] {
  if (statement.kind !== "delete" || !("columns" in statement.source)) {
    throw new InternalError("a delete whose source is a scan");
  }
  return statement.source.columns;
}

describe("DML", () => {
  it("gives an UPDATE's source an access path, keeping every column and the rowid", () => {
    const target = get(u, "id", "name", "age", "email", "tag", "rowid");
    const source = filter(target, compare(column(u, "id"), "=", constant(5)));
    const statement = optimized({
      kind: "update",
      table: users,
      tableIndex: 1,
      source,
      assignments: [{ column: 2, value: plus(age, constant(1)) }],
      parameterCount: 0,
    });
    expect(sourceOf(statement)).toBe("rowid-lookup users#1 (rowid = 5)");
    if (
      statement.kind !== "update" ||
      statement.source.kind !== "rowid-lookup"
    ) {
      throw new InternalError("an update with its rowid lookup");
    }
    expect(statement.source.columns).toEqual(target.columns);
    expect(statement.assignments).toEqual([
      { column: 2, value: plus(age, constant(1)) },
    ]);
  });

  it("gives a DELETE's source an access path, reading the rowid alone", () => {
    const statement = deleteWhere(
      compare(column(u, "email"), "=", parameter(0)),
    );
    expect(sourceOf(statement)).toBe(
      "index-scan users#1 using users_email (email = $1)",
    );
    expect(columnsRead(statement)).toEqual([ROWID_COLUMN]);
  });

  it("keeps the columns a DELETE's residual still reads", () => {
    const statement = deleteWhere(compare(age, "<", constant(18)));
    expect(sourceOf(statement)).toBe(
      "index-scan users#1 using users_age (age < 18) residual: u.age < 18",
    );
    expect(columnsRead(statement)).toEqual([2, ROWID_COLUMN]);
  });

  it("keeps every column for a correlated subquery, whose plan reads them", () => {
    const inner = filter(
      get(o),
      compare(column(o, "user_id"), "=", column(u, "id", 1)),
    );
    const statement = deleteWhere(exists(inner, true));
    expect(columnsRead(statement)).toEqual([0, 1, 2, 3, 4, ROWID_COLUMN]);
  });

  it("optimizes an INSERT's source like a query, and plans its ON CONFLICT", () => {
    const source = project(
      5,
      filter(get(o), compare(column(o, "user_id"), "=", constant(7))),
      column(o, "user_id"),
    );
    const max = aggregate(
      { groupIndex: 6, aggregateIndex: 7 },
      get(o),
      [],
      aggregateCall("max", column(o, "id")),
    );
    const statement = optimized({
      kind: "insert",
      table: orders,
      columns: [1],
      source,
      onConflict: {
        target: "rowid",
        action: {
          kind: "update",
          targetIndex: 8,
          excludedIndex: 9,
          assignments: [{ column: 1, value: scalarSubquery(max) }],
          condition: null,
        },
      },
      parameterCount: 0,
    });
    expect(sourceOf(statement)).toBe(
      lines(
        "project#5 o.user_id",
        "  index-scan orders#2 using orders_user (user_id = 7)",
      ),
    );
    if (
      statement.kind !== "insert" ||
      statement.onConflict?.action.kind !== "update"
    ) {
      throw new InternalError("an insert that updates on conflict");
    }
    const [assignment] = statement.onConflict.action.assignments;
    expect(
      assignment?.value.kind === "scalar-subquery" &&
        formatPhysicalPlan(assignment.value.plan),
    ).toBe(
      lines("hash-aggregate#6#7 compute max(o.id)", "  table-scan orders#2"),
    );
  });

  it("passes DDL through untouched", () => {
    const statement: Statement<logical.Plan> = {
      kind: "drop",
      object: "table",
      name: "users",
      isIfExists: false,
    };
    expect(optimized(statement)).toBe(statement);
    const index: Statement<logical.Plan> = {
      kind: "create-index",
      index: usersEmail,
      isIfNotExists: true,
    };
    expect(optimized(index)).toBe(index);
  });
});

describe("subqueries", () => {
  it("plans a subquery's plan with every pass", () => {
    const inner = filter(
      get(o),
      and(
        TRUE,
        compare(column(o, "user_id"), "=", plus(constant(3), constant(4))),
      ),
    );
    const total = project(5, inner, column(o, "total"));
    const plan = filter(get(u), compare(age, ">", scalarSubquery(total)));
    expect(planOf(plan)).toBe(
      lines(
        "table-scan users#1 residual: u.age > (subquery 1)",
        "subquery 1:",
        "  project#5 o.total",
        "    index-scan orders#2 using orders_user (user_id = 7)",
      ),
    );
  });

  it("plans correlated subqueries, checked after the conditions that can move", () => {
    const inner = filter(
      get(o),
      compare(column(o, "user_id"), "=", column(u, "id", 1)),
    );
    const plan = filter(
      get(u),
      exists(inner, true),
      inSubquery(age, project(6, get(o), column(o, "user_id"))),
    );
    expect(planOf(plan)).toBe(
      lines(
        "table-scan users#1 residual: u.age = ANY (subquery 1) AND EXISTS (correlated subquery 2)",
        "subquery 1:",
        "  project#6 o.user_id",
        "    table-scan orders#2",
        "subquery 2:",
        "  index-scan orders#2 using orders_user (user_id = u.id)",
      ),
    );
  });

  it("plans subqueries wherever expressions are: projects, joins, keys, values", () => {
    const one = values(7, [constant(1)]);
    const plan = order(
      join(
        "inner",
        project(5, get(u), scalarSubquery(one)),
        values(6, [scalarSubquery(one)]),
        exists(one),
      ),
      asc(scalarSubquery(one)),
    );
    const printed = planOf(plan);
    expect(printed.match(/^subquery \d+:$/gmu)).toHaveLength(4);
  });
});

describe("purity", () => {
  it("never changes its input, and gives the same plan for the same input", () => {
    const plan = limit(
      order(
        filter(
          join(
            "left",
            get(u),
            get(o),
            compare(column(o, "user_id"), "=", column(u, "id")),
          ),
          compare(column(u, "name"), "=", constant("bob")),
          nullCheck(column(o, "total")),
        ),
        asc(age),
      ),
      constant(3),
    );
    const statement = deepFreeze(query(plan));
    const first = optimized(statement);
    expect(optimized(statement)).toEqual(first);
    expect(statement).toEqual(query(plan));
  });

  it("matches nothing to an expression key without a binder", () => {
    const lowered = call("lower", column(u, "name"));
    const statement = query(
      filter(get(u), compare(lowered, "=", constant("bob"))),
    );
    const plan = optimize(statement, schema);
    expect(plan.kind === "query" && formatPhysicalPlan(plan.plan)).toBe(
      "table-scan users#1 residual: lower(u.name) = 'bob'",
    );
  });
});

describe("expression keys", () => {
  it("binds each expression key once per relation in one call", () => {
    const bound: string[] = [];
    const counting: KeyBinder = (sql, table, tableIndex) => {
      bound.push(`${sql} over #${String(tableIndex)}`);
      return bindKey(sql, table, tableIndex);
    };
    // The order asks the read for its order first, then plans it plainly:
    // two requests for the indexes of #1, one binding.
    const lowered = call("lower", column(u, "name"));
    const read = filter(get(u), compare(lowered, "=", constant("bob")));
    optimize(query(order(read, asc(column(u, "name")))), schema, counting);
    expect(bound).toEqual(["lower(name) over #1"]);
  });
});

describe("the rules every optimization keeps", () => {
  it("1: an access path only narrows what is read", () => {
    const plan = filter(
      get(u),
      compare(age, "<", parameter(0)),
      compare(column(u, "name"), "<>", constant("x")),
    );
    expect(planOf(plan)).toBe(
      "index-scan users#1 using users_age (age < $1) residual: u.age < $1 AND u.name <> 'x'",
    );
  });

  it("2: a join condition is never dropped", () => {
    const plan = join(
      "left",
      get(u),
      get(o),
      compare(column(o, "user_id"), "=", column(u, "id")),
      compare(column(o, "total"), ">", age),
      compare(age, ">", constant(1)),
    );
    expect(planOf(plan)).toMatch(
      /^hash-join left on u\.id = o\.user_id residual: o\.total > u\.age AND u\.age > 1\n/u,
    );
  });

  it("3: order comes from one place, and top-n never keeps its limit", () => {
    const sorted = planOf(
      limit(order(get(o), asc(column(o, "total"))), constant(2)),
    );
    expect(sorted).toBe(
      lines("top-n 2 by o.total ASC", "  table-scan orders#2"),
    );
    const delivered = planOf(
      limit(order(get(u), asc(column(u, "email"))), constant(2)),
    );
    expect(delivered).toBe(
      lines("limit 2", "  index-scan users#1 using users_email"),
    );
  });

  it("4: rewrites follow values, NULLs included", () => {
    const same = compare(age, "=", age);
    expect(planOf(filter(get(u), same))).toBe(
      "table-scan users#1 residual: u.age = u.age",
    );
  });

  it("5: parameters bound a range, they never choose the index", () => {
    const withParameter = planOf(
      filter(get(u), compare(age, ">", parameter(0))),
    );
    const withConstant = planOf(filter(get(u), compare(age, ">", constant(1))));
    expect(withParameter).toBe("index-scan users#1 using users_age (age > $1)");
    expect(withConstant).toBe("index-scan users#1 using users_age (age > 1)");
  });
});
