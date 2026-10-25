import { describe, expect, it } from "vitest";
import { Parser, parse } from "../index.js";
import {
  AlterType,
  DropType,
  JoinType,
  OrderByNullType,
  OrderType,
  ResultModifierType,
  SetOperationType,
  StatementType,
  TableReferenceType,
  TransactionType,
  type BeginMode,
  type CreateTableStatement,
  type TransactionStatement,
  type Statement,
  type TableReference,
} from "../types.js";
import {
  notSupported,
  only,
  query,
  render,
  renderOptional,
  select,
  syntaxError,
} from "./helpers.js";

function createTable(sql: string): CreateTableStatement {
  const statement = only(sql);
  if (statement.type !== StatementType.CREATE_TABLE_STATEMENT) {
    throw new Error("not CREATE TABLE");
  }
  return statement;
}

/** A FROM clause as a tree: `(CROSS a b)`, `(LEFT a b ON (= x y))`. */
function from(sql: string): string {
  const table = select(sql).fromTable;
  if (table === null) throw new Error("no FROM");
  return renderTable(table);
}

function renderTable(table: TableReference): string {
  switch (table.type) {
    case TableReferenceType.BASE_TABLE: {
      return table.alias === null
        ? table.tableName
        : `${table.tableName} AS ${table.alias}`;
    }
    case TableReferenceType.SUBQUERY: {
      return `(<query>) AS ${table.alias ?? "-"}`;
    }
    case TableReferenceType.JOIN: {
      const sides = `${renderTable(table.left)} ${renderTable(table.right)}`;
      const on =
        table.condition === null ? "" : ` ON ${render(table.condition)}`;
      const using =
        table.usingColumns.length === 0
          ? ""
          : ` USING ${table.usingColumns.join(",")}`;
      return `(${table.joinType} ${sides}${on}${using})`;
    }
  }
}

describe("statement boundaries", () => {
  it("separates statements with semicolons and skips empty ones", () => {
    expect(parse(";SELECT 1;; SELECT 2;")).toHaveLength(2);
    expect(parse("")).toEqual([]);
    expect(parse(" ; -- nothing\n ;")).toEqual([]);
  });

  it.each([
    ["SELECT 1 SELECT 2", "SELECT", 9],
    ["SELECT * FROM t1 t2 t3", "t3", 20],
    ["SELECT 1 2", "2", 9],
    ["DELETE FROM t WHERE a = 1 b", "b", 26],
    ["COMMIT now", "now", 7],
  ])("rejects leftovers after a statement: %s", (sql, token, offset) => {
    const error = syntaxError(sql);
    expect([error.message, error.offset, error.length]).toEqual([
      `expected ';' or end of input, got '${token}'`,
      offset,
      token.length,
    ]);
  });

  it("does not take EXCEPT for an alias", () => {
    const { node } = query("SELECT * FROM t1 EXCEPT SELECT * FROM t2");
    expect(node.type === "SET_OPERATION_NODE" && node.setOperationType).toBe(
      SetOperationType.EXCEPT,
    );
  });

  it("keeps the old Parser class working", () => {
    expect(new Parser().parse("SELECT 1; SELECT 2")).toHaveLength(2);
  });
});

describe("errors", () => {
  it("names what was expected in SQL words, at the offending token", () => {
    const error = syntaxError("SELECT (1 FROM t");
    expect(error.code).toBe("SYNTAX_ERROR");
    expect(error.message).toBe("expected ')', got 'FROM'");
    expect([error.offset, error.length]).toEqual([10, 4]);
  });

  it("reports the end of input as such, and an empty string as a string", () => {
    expect(syntaxError("SELECT a FROM").message).toBe(
      "expected a table name, got end of input",
    );
    expect(syntaxError("SELECT a FROM").offset).toBe(13);
    expect(syntaxError("SELECT a FROM t WHERE a = '' b").message).toBe(
      "expected ';' or end of input, got 'b'",
    );
    expect(syntaxError("SELECT 1 ''").message).toBe(
      "expected ';' or end of input, got ''''",
    );
  });

  it("reports lexer errors at their place", () => {
    const error = syntaxError("SELECT 'abc");
    expect([error.message, error.offset, error.length]).toEqual([
      "unterminated string",
      7,
      4,
    ]);
  });

  it("shortens long tokens in messages", () => {
    const long = "x".repeat(100);
    expect(syntaxError(`SELECT 1 a ${long}`).message).toBe(
      `expected ';' or end of input, got '${"x".repeat(32)}…'`,
    );
  });

  it("rejects an unknown statement", () => {
    expect(syntaxError("FROM a, b").message).toBe(
      "expected a statement, got 'FROM'",
    );
    expect(syntaxError("hello").message).toBe(
      "expected a statement, got 'hello'",
    );
  });
});

describe("SELECT", () => {
  it("reads every clause", () => {
    const node = select(
      "SELECT DISTINCT a, count(*) AS n FROM t WHERE a > 1 GROUP BY a HAVING count(*) > 1 ORDER BY n DESC NULLS LAST, a LIMIT 10 OFFSET 5",
    );
    expect(node.selectList.map((item) => render(item))).toEqual([
      "a",
      "(count *) AS n",
    ]);
    expect(renderOptional(node.whereClause)).toBe("(> a 1)");
    expect(node.groups.groupExpressions.map((group) => render(group))).toEqual([
      "a",
    ]);
    expect(renderOptional(node.having)).toBe("(> (count *) 1)");
    expect(node.modifiers.map((modifier) => modifier.type)).toEqual([
      ResultModifierType.DISTINCT_MODIFIER,
      ResultModifierType.ORDER_MODIFIER,
      ResultModifierType.LIMIT_MODIFIER,
    ]);
  });

  it("leaves the NULL order open unless the query says", () => {
    const order = select(
      "SELECT a FROM t ORDER BY a, b DESC, c NULLS FIRST, d ASC NULLS LAST",
    ).modifiers.find(
      (modifier) => modifier.type === ResultModifierType.ORDER_MODIFIER,
    );
    expect(
      order?.type === ResultModifierType.ORDER_MODIFIER &&
        order.orders.map((item) => [item.type, item.nullOrder]),
    ).toEqual([
      [OrderType.ASCENDING, null],
      [OrderType.DESCENDING, null],
      [OrderType.ASCENDING, OrderByNullType.NULLS_FIRST],
      [OrderType.ASCENDING, OrderByNullType.NULLS_LAST],
    ]);
  });

  it.each([
    ["LIMIT 10", "10", null],
    ["LIMIT 10 OFFSET 5", "10", "5"],
    ["LIMIT 5, 10", "10", "5"],
    ["OFFSET 5", null, "5"],
    ["OFFSET 5 LIMIT 10", "10", "5"],
    ["LIMIT -1", "-1", null],
  ])("reads %s", (clause, limit, offset) => {
    const modifier = select(`SELECT a FROM t ${clause}`).modifiers.at(-1);
    if (modifier?.type !== ResultModifierType.LIMIT_MODIFIER) {
      throw new Error("no LIMIT");
    }
    expect([
      renderOptional(modifier.limit),
      renderOptional(modifier.offset),
    ]).toEqual([limit, offset]);
  });

  it("reads * and t.* only as whole items", () => {
    expect(
      select('SELECT *, t.*, "order".* FROM t').selectList.map((item) =>
        render(item),
      ),
    ).toEqual(["*", "t.*", "order.*"]);
  });

  it("takes only plain identifiers as bare aliases", () => {
    expect(
      select('SELECT a b, c "d e", f AS key FROM t').selectList.map((item) =>
        render(item),
      ),
    ).toEqual(["a AS b", "c AS d e", "f AS key"]);
    expect(syntaxError("SELECT a key FROM t").message).toBe(
      "expected ';' or end of input, got 'key'",
    );
  });
});

describe("FROM", () => {
  it.each([
    ["SELECT * FROM a, b", "(CROSS a b)"],
    ["SELECT * FROM a, b, c", "(CROSS (CROSS a b) c)"],
    [
      "SELECT * FROM a x, b JOIN c ON x.i = c.i",
      "(INNER (CROSS a AS x b) c ON (= x.i c.i))",
    ],
    ["SELECT * FROM a CROSS JOIN b", "(CROSS a b)"],
    // As in SQLite, a CROSS join or a comma with ON is an inner join.
    ["SELECT * FROM a CROSS JOIN b ON a.i = b.i", "(INNER a b ON (= a.i b.i))"],
    ["SELECT * FROM a, b USING (i)", "(INNER a b USING i)"],
    ["SELECT * FROM a JOIN b", "(INNER a b)"],
    ["SELECT * FROM a INNER JOIN b USING (i, j)", "(INNER a b USING i,j)"],
    [
      "SELECT * FROM a LEFT OUTER JOIN b ON a.i = b.i",
      "(LEFT a b ON (= a.i b.i))",
    ],
    ["SELECT * FROM a RIGHT JOIN b ON TRUE", "(RIGHT a b ON TRUE)"],
    [
      "SELECT * FROM left JOIN right ON left.i = right.i",
      "(INNER left right ON (= left.i right.i))",
    ],
    ["SELECT * FROM (SELECT 1) AS s", "(<query>) AS s"],
    ["SELECT * FROM (SELECT 1) s", "(<query>) AS s"],
    ['SELECT * FROM "order" o', "order AS o"],
    ["SELECT * FROM t AS index", "t AS index"],
  ])("%s", (sql, tree) => {
    expect(from(sql)).toBe(tree);
  });

  it("does not take a join keyword for an alias", () => {
    expect(notSupported("SELECT * FROM t1 NATURAL JOIN t2").feature).toBe(
      "NATURAL JOIN",
    );
    expect(notSupported("SELECT * FROM t1 FULL JOIN t2 ON TRUE").feature).toBe(
      "FULL JOIN",
    );
    expect(syntaxError("SELECT * FROM t1 LEFT t2").message).toBe(
      "expected JOIN, got 't2'",
    );
  });

  it.each([
    ["SELECT * FROM main.t", "a schema name"],
    ["SELECT * FROM json_each(x)", "a table-valued function"],
    ["SELECT * FROM (a JOIN b)", "parentheses around a FROM item"],
    ["SELECT * FROM ((SELECT 1))", "parentheses around a FROM item"],
    ["SELECT * FROM (VALUES (1))", "VALUES as a query"],
    ["SELECT * FROM t INDEXED BY i", "INDEXED BY"],
  ])("does not support %s", (sql, feature) => {
    expect(notSupported(sql).feature).toBe(feature);
  });
});

describe("compound queries and WITH", () => {
  it("chains compound operators left to right", () => {
    const { node } = query(
      "SELECT 1 UNION ALL SELECT 2 INTERSECT SELECT 3 EXCEPT SELECT 4 ORDER BY 1 LIMIT 2",
    );
    const types: string[] = [];
    let current = node;
    while (current.type === "SET_OPERATION_NODE") {
      types.push(current.setOperationType);
      current = current.left;
    }
    expect(types).toEqual([
      SetOperationType.EXCEPT,
      SetOperationType.INTERSECT,
      SetOperationType.UNION_ALL,
    ]);
    expect(node.modifiers.map((modifier) => modifier.type)).toEqual([
      ResultModifierType.ORDER_MODIFIER,
      ResultModifierType.LIMIT_MODIFIER,
    ]);
  });

  it("does not support INTERSECT ALL and EXCEPT ALL", () => {
    expect(notSupported("SELECT 1 INTERSECT ALL SELECT 1").feature).toBe(
      "INTERSECT ALL",
    );
    expect(notSupported("SELECT 1 EXCEPT ALL SELECT 1").feature).toBe(
      "EXCEPT ALL",
    );
  });

  it("attaches WITH to the outermost query, in the order written", () => {
    const { node } = query(
      "WITH RECURSIVE a(x) AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a UNION SELECT * FROM b",
    );
    expect(node.cteMap.recursive).toBe(true);
    expect(node.cteMap.ctes.map((cte) => [cte.name, cte.aliases])).toEqual([
      ["a", ["x"]],
      ["b", []],
    ]);
    expect(node.type === "SET_OPERATION_NODE" && node.left.cteMap.ctes).toEqual(
      [],
    );
  });

  it("keeps a CTE named like a keyword", () => {
    const { node } = query(
      "WITH recursive AS (SELECT 1), __proto__ AS (SELECT 2) SELECT * FROM recursive",
    );
    expect(node.cteMap.recursive).toBe(false);
    expect(node.cteMap.ctes.map((cte) => cte.name)).toEqual([
      "recursive",
      "__proto__",
    ]);
  });

  it("does not support MATERIALIZED hints", () => {
    for (const hint of ["MATERIALIZED", "NOT MATERIALIZED"]) {
      expect(
        notSupported(`WITH a AS ${hint} (SELECT 1) SELECT * FROM a`).feature,
      ).toBe("[NOT] MATERIALIZED");
    }
  });

  it("counts each CTE toward the nesting limit", () => {
    const ctes = Array.from(
      { length: 300 },
      (_, index) => `c${String(index)} AS (SELECT 1)`,
    );
    expect(syntaxError(`WITH ${ctes.join(", ")} SELECT 1`).message).toMatch(
      /nests more than/,
    );
  });

  it("does not support WITH before DML", () => {
    expect(notSupported("WITH a AS (SELECT 1) DELETE FROM t").feature).toBe(
      "WITH before DELETE",
    );
  });
});

describe("INSERT, UPDATE, DELETE", () => {
  it("reads INSERT … VALUES with ON CONFLICT", () => {
    const statement = only(
      "INSERT INTO t (id, key) VALUES (1, 'a'), (2, -3) ON CONFLICT (id) DO UPDATE SET key = excluded.key WHERE t.id > 0",
    );
    if (statement.type !== StatementType.INSERT_STATEMENT) {
      throw new Error("not an INSERT");
    }
    expect(statement.columns).toEqual(["id", "key"]);
    expect(
      statement.values.map((row) => row.map((value) => render(value))),
    ).toEqual([
      ["1", "'a'"],
      ["2", "-3"],
    ]);
    const conflict = statement.onConflict;
    expect(conflict?.conflictTarget).toEqual(["id"]);
    expect(
      conflict?.action !== "NOTHING" &&
        conflict?.action.setClauses.map((clause) => [
          clause.column,
          render(clause.value),
        ]),
    ).toEqual([["key", "excluded.key"]]);
  });

  it("reads INSERT … SELECT and DO NOTHING", () => {
    const statement = only(
      "INSERT INTO t SELECT * FROM s ON CONFLICT DO NOTHING",
    );
    if (statement.type !== StatementType.INSERT_STATEMENT) {
      throw new Error("not an INSERT");
    }
    expect(statement.selectStatement).not.toBeNull();
    expect(statement.onConflict).toEqual({
      conflictTarget: null,
      action: "NOTHING",
    });
  });

  it("reads UPDATE and DELETE", () => {
    const update = only("UPDATE t SET a = a + 1, end = 'x' WHERE id = ?");
    if (update.type !== StatementType.UPDATE_STATEMENT) {
      throw new Error("not an UPDATE");
    }
    expect(update.setClauses.map((clause) => clause.column)).toEqual([
      "a",
      "end",
    ]);
    expect(renderOptional(update.whereClause)).toBe("(= id $1)");
    const deletion = only("DELETE FROM t");
    expect(deletion).toEqual({
      type: StatementType.DELETE_STATEMENT,
      table: "t",
      whereClause: null,
    });
  });

  it.each([
    ["INSERT OR REPLACE INTO t VALUES (1)", "INSERT OR …"],
    ["INSERT INTO t DEFAULT VALUES", "DEFAULT VALUES"],
    ["INSERT INTO t VALUES (1) RETURNING *", "RETURNING"],
    ["UPDATE t SET a = 1 FROM s", "UPDATE … FROM"],
    ["UPDATE t SET (a, b) = (1, 2)", "assigning to a list of columns"],
    ["DELETE FROM t RETURNING a", "RETURNING"],
    ["INSERT INTO t AS x VALUES (1)", "a table alias in INSERT"],
    ["UPDATE t AS x SET a = 1", "a table alias in UPDATE"],
    ["DELETE FROM t AS x", "a table alias in DELETE"],
    [
      "INSERT INTO t VALUES (1) ON CONFLICT (a) WHERE a > 0 DO NOTHING",
      "ON CONFLICT … WHERE",
    ],
    ["REPLACE INTO t VALUES (1)", "the REPLACE statement"],
  ])("does not support %s", (sql, feature) => {
    expect(notSupported(sql).feature).toBe(feature);
  });
});

describe("CREATE TABLE", () => {
  it("keeps declared types as written, and allows none", () => {
    const table = createTable(
      "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(255) NOT NULL UNIQUE, price DECIMAL(10, 2), anything, big UNSIGNED BIG INT)",
    );
    expect(table.ifNotExists).toBe(true);
    expect(table.columns.map((column) => [column.name, column.type])).toEqual([
      ["id", "INTEGER"],
      ["name", "varchar(255)"],
      ["price", "DECIMAL(10, 2)"],
      ["anything", null],
      ["big", "UNSIGNED BIG INT"],
    ]);
    expect(table.columns[0]?.isPrimaryKey).toBe(true);
    expect(table.columns[0]?.isAutoincrement).toBe(true);
    expect(table.columns[1]?.isNotNull).toBe(true);
    expect(table.columns[1]?.isUnique).toBe(true);
  });

  it("allows non-reserved keywords and quoted names as column names", () => {
    const table = createTable(
      'CREATE TABLE t (key TEXT, first TEXT, last TEXT, text TEXT, json JSON, index INT, "default" INT, end INT, "order" INT)',
    );
    expect(table.columns.map((column) => column.name)).toEqual([
      "key",
      "first",
      "last",
      "text",
      "json",
      "index",
      "default",
      "end",
      "order",
    ]);
    expect(createTable('CREATE TABLE "order" ("key" TEXT)').table).toBe(
      "order",
    );
  });

  it("reads DEFAULT as an expression, with the sign folded", () => {
    const table = createTable(
      "CREATE TABLE t (a INT DEFAULT -1 NOT NULL, b TEXT DEFAULT 'x' || 'y', c INT DEFAULT (1 = 1), d REAL DEFAULT +2.5, e INT DEFAULT NULL)",
    );
    expect(
      table.columns.map((column) =>
        column.defaultValue === null ? "-" : render(column.defaultValue),
      ),
    ).toEqual(["-1", "(|| 'x' 'y')", "(= 1 1)", "real(2.5)", "NULL"]);
    expect(table.columns[0]?.isNotNull).toBe(true);
  });

  it("does not let DEFAULT take a NOT NULL constraint as its value", () => {
    expect(syntaxError("CREATE TABLE t (a INT DEFAULT NOT NULL)").message).toBe(
      "expected a default value, got 'NOT'",
    );
  });

  it("does not read non-ASCII look-alikes as keywords", () => {
    expect(createTable("CREATE TABLE ıs (ſelect INT)").columns[0]?.name).toBe(
      "ſelect",
    );
    expect(syntaxError("ſelect 1").message).toBe(
      "expected a statement, got 'ſelect'",
    );
  });

  it("reads a table PRIMARY KEY, once", () => {
    expect(
      createTable("CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b))")
        .primaryKey,
    ).toEqual(["a", "b"]);
    expect(() =>
      parse("CREATE TABLE t (a INT, PRIMARY KEY (a), PRIMARY KEY (a))"),
    ).toThrow("more than one PRIMARY KEY");
  });

  it.each([
    ["CREATE TABLE t (a INT REFERENCES u(id))", "REFERENCES"],
    ["CREATE TABLE t (a INT, FOREIGN KEY (a) REFERENCES u(id))", "FOREIGN KEY"],
    ["CREATE TABLE t (a INT CHECK (a > 0))", "CHECK"],
    ["CREATE TABLE t (a INT, UNIQUE (a))", "a table UNIQUE constraint"],
    ["CREATE TABLE t (a INT CONSTRAINT pk PRIMARY KEY)", "CONSTRAINT"],
    ["CREATE TABLE t (a TEXT COLLATE nocase)", "COLLATE"],
    [
      "CREATE TABLE t (a INTEGER PRIMARY KEY DESC)",
      "ASC or DESC on a PRIMARY KEY",
    ],
    [
      "CREATE TABLE t (a INT NOT NULL ON CONFLICT IGNORE)",
      "ON CONFLICT in a constraint",
    ],
    [
      "CREATE TABLE t (a INT, PRIMARY KEY (a) ON CONFLICT FAIL)",
      "ON CONFLICT in a constraint",
    ],
    ["CREATE TABLE t (a INT DEFAULT CURRENT_TIMESTAMP)", "CURRENT_TIMESTAMP"],
    ["CREATE TABLE t (a INT GENERATED ALWAYS AS (1))", "a generated column"],
    ["CREATE TABLE t (a INT) WITHOUT ROWID", "WITHOUT ROWID"],
    ["CREATE TABLE t (a INT) STRICT", "STRICT"],
    ['CREATE TABLE t (a "INTEGER")', "a quoted type name"],
    ["CREATE TABLE t AS SELECT 1", "CREATE TABLE … AS"],
    ["CREATE VIEW v AS SELECT 1", "CREATE VIEW"],
    ["CREATE TEMP TABLE t (a INT)", "CREATE TEMP"],
  ])("does not support %s", (sql, feature) => {
    expect(notSupported(sql).feature).toBe(feature);
  });
});

describe("other DDL", () => {
  it("reads CREATE INDEX", () => {
    expect(
      only("CREATE UNIQUE INDEX IF NOT EXISTS i ON t (a, lower(b))"),
    ).toMatchObject({
      type: StatementType.CREATE_INDEX_STATEMENT,
      indexName: "i",
      tableName: "t",
      isUnique: true,
      ifNotExists: true,
    });
    expect(notSupported("CREATE INDEX i ON t (a) WHERE a > 0").feature).toBe(
      "a partial index",
    );
    expect(notSupported("CREATE INDEX i ON t (a DESC)").feature).toBe(
      "ASC or DESC in an index",
    );
  });

  it("reads ALTER TABLE", () => {
    expect(only("ALTER TABLE t ADD COLUMN key TEXT DEFAULT ''")).toMatchObject({
      alterType: AlterType.ADD_COLUMN,
      columnDefinition: { name: "key", type: "TEXT" },
      columnName: null,
    });
    expect(only("ALTER TABLE t DROP first")).toMatchObject({
      alterType: AlterType.DROP_COLUMN,
      columnDefinition: null,
      columnName: "first",
    });
    expect(notSupported("ALTER TABLE t RENAME TO u").feature).toBe(
      "ALTER TABLE … RENAME",
    );
  });

  it("always reads COLUMN after ADD and DROP as the keyword", () => {
    expect(only("ALTER TABLE t ADD column INT")).toMatchObject({
      columnDefinition: { name: "INT", type: null },
    });
    expect(syntaxError("ALTER TABLE t ADD COLUMN").message).toBe(
      "expected a column name, got end of input",
    );
    expect(syntaxError("ALTER TABLE t ADD COLUMN NOT NULL").message).toBe(
      "expected a column name, got 'NOT'",
    );
    expect(syntaxError("ALTER TABLE t DROP COLUMN").message).toBe(
      "expected a column name, got end of input",
    );
  });

  it("reads DROP", () => {
    expect(only("DROP TABLE IF EXISTS t")).toEqual({
      type: StatementType.DROP_STATEMENT,
      dropType: DropType.TABLE,
      name: "t",
      ifExists: true,
    });
    expect(only("DROP INDEX if")).toMatchObject({
      name: "if",
      ifExists: false,
    });
    expect(notSupported("DROP VIEW v").feature).toBe("DROP VIEW");
  });
});

describe("transactions and EXPLAIN", () => {
  const begin = (mode: BeginMode): TransactionStatement => ({
    type: StatementType.TRANSACTION_STATEMENT,
    transactionType: TransactionType.BEGIN,
    mode,
  });
  const end = (
    transactionType: TransactionType.COMMIT | TransactionType.ROLLBACK,
  ): TransactionStatement => ({
    type: StatementType.TRANSACTION_STATEMENT,
    transactionType,
  });

  it.each<[string, TransactionStatement]>([
    ["BEGIN", begin("DEFERRED")],
    ["BEGIN TRANSACTION", begin("DEFERRED")],
    ["BEGIN DEFERRED", begin("DEFERRED")],
    ["BEGIN IMMEDIATE TRANSACTION", begin("IMMEDIATE")],
    ["COMMIT", end(TransactionType.COMMIT)],
    ["END TRANSACTION", end(TransactionType.COMMIT)],
    ["ROLLBACK", end(TransactionType.ROLLBACK)],
  ])("%s", (sql, expected) => {
    expect(only(sql)).toEqual(expected);
  });

  it("wraps a statement in EXPLAIN", () => {
    const statement: Statement = only("EXPLAIN SELECT 1");
    expect(statement.type).toBe(StatementType.EXPLAIN_STATEMENT);
    expect(syntaxError("EXPLAIN EXPLAIN SELECT 1").message).toBe(
      "expected a statement to explain, got 'EXPLAIN'",
    );
  });

  it.each([
    ["BEGIN EXCLUSIVE", "BEGIN EXCLUSIVE"],
    ["BEGIN TRANSACTION t1", "a transaction name"],
    ["COMMIT TRANSACTION t1", "a transaction name"],
    ["ROLLBACK TRANSACTION TO s", "ROLLBACK TO"],
    ["ROLLBACK TO s", "ROLLBACK TO"],
    ["SAVEPOINT s", "the SAVEPOINT statement"],
    ["PRAGMA table_info(t)", "the PRAGMA statement"],
    ["VACUUM", "the VACUUM statement"],
    ["VALUES (1)", "the VALUES statement"],
    ["EXPLAIN QUERY PLAN SELECT 1", "EXPLAIN QUERY PLAN"],
  ])("does not support %s", (sql, feature) => {
    expect(notSupported(sql).feature).toBe(feature);
  });
});

describe("join types", () => {
  it("reads every supported join into its JoinType", () => {
    const types = [
      "a JOIN b",
      "a INNER JOIN b",
      "a LEFT JOIN b",
      "a RIGHT OUTER JOIN b",
      "a CROSS JOIN b",
      "a, b",
    ].map((clause) => {
      const table = select(`SELECT * FROM ${clause}`).fromTable;
      return table?.type === TableReferenceType.JOIN ? table.joinType : null;
    });
    expect(types).toEqual([
      JoinType.INNER,
      JoinType.INNER,
      JoinType.LEFT,
      JoinType.RIGHT,
      JoinType.CROSS,
      JoinType.CROSS,
    ]);
  });
});
