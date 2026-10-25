import { describe, expect, it } from "vitest";
import { Parser, parse } from "../index.js";
import {
  AlterType,
  DropType,
  StatementType,
  TransactionType,
  type BeginMode,
  type ColumnDefinition,
  type TransactionStatement,
} from "../types.js";
import {
  notSupported,
  query,
  render,
  renderOptional,
  renderQuery,
  renderTable,
  select,
  statementOf,
  syntaxError,
} from "./helpers.js";

/** A query printed whole: see renderQuery. */
function printed(sql: string): string {
  return renderQuery(query(sql));
}

/** The FROM clause of a query, printed. */
function from(sql: string): string {
  const table = select(sql).from;
  if (table === null) throw new Error("no FROM");
  return renderTable(table);
}

/** A column definition: `name type [constraint, …]`. */
function renderColumn(column: ColumnDefinition): string {
  const constraints = column.constraints.map((constraint) =>
    constraint.type === "DEFAULT"
      ? `DEFAULT ${render(constraint.value)}`
      : constraint.type,
  );
  return [column.name, column.type ?? "-", ...constraints].join(" ");
}

function begin(mode: BeginMode): TransactionStatement {
  return {
    type: StatementType.TRANSACTION_STATEMENT,
    transactionType: TransactionType.BEGIN,
    mode,
  };
}

function transactionEnd(
  transactionType: TransactionType.COMMIT | TransactionType.ROLLBACK,
): TransactionStatement {
  return { type: StatementType.TRANSACTION_STATEMENT, transactionType };
}

function columns(sql: string): string[] {
  const statement = statementOf(sql, StatementType.CREATE_TABLE_STATEMENT);
  return statement.columns.map((column) => renderColumn(column));
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
    expect(printed("SELECT * FROM t1 EXCEPT SELECT * FROM t2")).toBe(
      "(EXCEPT (SELECT * FROM t1) (SELECT * FROM t2))",
    );
  });

  it("keeps the Parser class for callers that hold one", () => {
    expect(new Parser().parse("SELECT 1; SELECT 2")).toHaveLength(2);
  });
});

describe("errors", () => {
  it("names what was expected in SQL words, at the offending token", () => {
    const error = syntaxError("SELECT (1 FROM t");
    expect(error.code).toBe("SYNTAX_ERROR");
    expect([error.message, error.offset, error.length]).toEqual([
      "expected ')', got 'FROM'",
      10,
      4,
    ]);
  });

  it("reports the end of input as such, and an empty string as a string", () => {
    const end = syntaxError("SELECT a FROM");
    expect([end.message, end.offset]).toEqual([
      "expected a table name, got end of input",
      13,
    ]);
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
    expect(
      printed(
        "SELECT DISTINCT a, count(*) AS n FROM t WHERE a > 1 GROUP BY a HAVING count(*) > 1 ORDER BY n DESC NULLS LAST, a LIMIT 10 OFFSET 5",
      ),
    ).toBe(
      "(SELECT DISTINCT a, (count *) AS n FROM t WHERE (> a 1) GROUP BY a HAVING (> (count *) 1)) ORDER BY n DESC NULLS_LAST, a LIMIT 10 OFFSET 5",
    );
  });

  it("leaves the NULL order open unless the query says", () => {
    expect(
      printed(
        "SELECT a FROM t ORDER BY a, b DESC, c NULLS FIRST, d ASC NULLS LAST",
      ),
    ).toBe("(SELECT a FROM t) ORDER BY a, b DESC, c NULLS_FIRST, d NULLS_LAST");
  });

  it.each([
    ["LIMIT 10", "LIMIT 10"],
    ["LIMIT 10 OFFSET 5", "LIMIT 10 OFFSET 5"],
    ["LIMIT 5, 10", "LIMIT 10 OFFSET 5"],
    ["OFFSET 5", "OFFSET 5"],
    ["OFFSET 5 LIMIT 10", "LIMIT 10 OFFSET 5"],
    ["LIMIT -1", "LIMIT -1"],
  ])("reads %s", (clause, limit) => {
    expect(printed(`SELECT a FROM t ${clause}`)).toBe(
      `(SELECT a FROM t) ${limit}`,
    );
  });

  it("reads * and t.* only as whole items", () => {
    expect(printed('SELECT *, t.*, "order".* FROM t')).toBe(
      "(SELECT *, t.*, order.* FROM t)",
    );
  });

  it("takes only plain identifiers as bare aliases", () => {
    expect(printed('SELECT a b, c "d e", f AS key FROM t')).toBe(
      "(SELECT a AS b, c AS d e, f AS key FROM t)",
    );
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
    ["SELECT * FROM a LEFT JOIN b", "(LEFT a b)"],
    [
      "SELECT * FROM a LEFT OUTER JOIN b ON a.i = b.i",
      "(LEFT a b ON (= a.i b.i))",
    ],
    ["SELECT * FROM a RIGHT OUTER JOIN b ON TRUE", "(RIGHT a b ON TRUE)"],
    [
      "SELECT * FROM left JOIN right ON left.i = right.i",
      "(INNER left right ON (= left.i right.i))",
    ],
    ["SELECT * FROM (SELECT 1) AS s", "{(SELECT 1)} AS s"],
    ["SELECT * FROM (SELECT 1) s", "{(SELECT 1)} AS s"],
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
    ["SELECT * FROM t NOT INDEXED", "INDEXED BY"],
  ])("does not support %s", (sql, feature) => {
    expect(notSupported(sql).feature).toBe(feature);
  });
});

describe("compound queries and WITH", () => {
  it("chains compound operators left to right; ORDER BY and LIMIT end it", () => {
    expect(
      printed(
        "SELECT 1 UNION ALL SELECT 2 INTERSECT SELECT 3 EXCEPT SELECT DISTINCT 4 ORDER BY 1 LIMIT 2",
      ),
    ).toBe(
      "(EXCEPT (INTERSECT (UNION_ALL (SELECT 1) (SELECT 2)) (SELECT 3)) (SELECT DISTINCT 4)) ORDER BY 1 LIMIT 2",
    );
  });

  it("does not support INTERSECT ALL and EXCEPT ALL", () => {
    expect(notSupported("SELECT 1 INTERSECT ALL SELECT 1").feature).toBe(
      "INTERSECT ALL",
    );
    expect(notSupported("SELECT 1 EXCEPT ALL SELECT 1").feature).toBe(
      "EXCEPT ALL",
    );
  });

  it("puts WITH on the whole query, its CTEs in the order written", () => {
    expect(
      printed(
        "WITH RECURSIVE a(x) AS (SELECT 1), b AS (SELECT 2 LIMIT 1) SELECT * FROM a UNION SELECT * FROM b",
      ),
    ).toBe(
      "WITH RECURSIVE a(x) AS {(SELECT 1)}, b AS {(SELECT 2) LIMIT 1} (UNION (SELECT * FROM a) (SELECT * FROM b))",
    );
  });

  it("keeps a CTE named like a keyword", () => {
    expect(
      printed(
        "WITH recursive AS (SELECT 1), __proto__ AS (SELECT 2) SELECT * FROM recursive",
      ),
    ).toBe(
      "WITH recursive AS {(SELECT 1)}, __proto__ AS {(SELECT 2)} (SELECT * FROM recursive)",
    );
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
    const statement = statementOf(
      "INSERT INTO t (id, key) VALUES (1, 'a'), (2, -3) ON CONFLICT (id) DO UPDATE SET key = excluded.key WHERE t.id > 0",
      StatementType.INSERT_STATEMENT,
    );
    expect(statement.columns).toEqual(["id", "key"]);
    if (statement.source.type !== "VALUES") throw new Error("not VALUES");
    expect(
      statement.source.rows.map((row) => row.map((value) => render(value))),
    ).toEqual([
      ["1", "'a'"],
      ["2", "-3"],
    ]);
    const conflict = statement.onConflict;
    expect(conflict?.target).toEqual(["id"]);
    if (conflict?.action.type !== "UPDATE") throw new Error("not DO UPDATE");
    expect(
      conflict.action.assignments.map((assignment) => [
        assignment.column,
        render(assignment.value),
      ]),
    ).toEqual([["key", "excluded.key"]]);
    expect(renderOptional(conflict.action.where)).toBe("(> t.id 0)");
  });

  it("reads INSERT … SELECT and DO NOTHING", () => {
    const statement = statementOf(
      "INSERT INTO t SELECT * FROM s ON CONFLICT DO NOTHING",
      StatementType.INSERT_STATEMENT,
    );
    if (statement.source.type !== "SELECT") throw new Error("not SELECT");
    expect(renderQuery(statement.source.query)).toBe("(SELECT * FROM s)");
    expect(statement.onConflict).toEqual({
      target: null,
      action: { type: "NOTHING" },
    });
  });

  it("reads UPDATE and DELETE", () => {
    const update = statementOf(
      "UPDATE t SET a = a + 1, end = 'x' WHERE id = ?",
      StatementType.UPDATE_STATEMENT,
    );
    expect(update.assignments.map((assignment) => assignment.column)).toEqual([
      "a",
      "end",
    ]);
    expect(renderOptional(update.where)).toBe("(= id $1)");
    expect(
      statementOf("DELETE FROM t", StatementType.DELETE_STATEMENT),
    ).toEqual({
      type: StatementType.DELETE_STATEMENT,
      table: "t",
      where: null,
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
    const table = statementOf(
      "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(255) NOT NULL UNIQUE, price DECIMAL(10, 2) NULL, anything, big UNSIGNED BIG INT)",
      StatementType.CREATE_TABLE_STATEMENT,
    );
    expect(table.ifNotExists).toBe(true);
    expect(table.columns.map((column) => renderColumn(column))).toEqual([
      "id INTEGER PRIMARY KEY AUTOINCREMENT",
      "name varchar(255) NOT NULL UNIQUE",
      "price DECIMAL(10, 2) NULL",
      "anything -",
      "big UNSIGNED BIG INT",
    ]);
  });

  it("allows non-reserved keywords and quoted names as column names", () => {
    expect(
      columns(
        'CREATE TABLE t (key TEXT, first TEXT, last TEXT, text TEXT, json JSON, index INT, "default" INT, end INT, "order" INT)',
      ),
    ).toEqual([
      "key TEXT",
      "first TEXT",
      "last TEXT",
      "text TEXT",
      "json JSON",
      "index INT",
      "default INT",
      "end INT",
      "order INT",
    ]);
    expect(
      statementOf(
        'CREATE TABLE "order" ("key" TEXT)',
        StatementType.CREATE_TABLE_STATEMENT,
      ).table,
    ).toBe("order");
  });

  it("reads DEFAULT as an expression, with the sign folded", () => {
    expect(
      columns(
        "CREATE TABLE t (a INT DEFAULT -1 NOT NULL, b TEXT DEFAULT 'x' || 'y', c INT DEFAULT (1 = 1), d REAL DEFAULT +2.5, e INT DEFAULT NULL)",
      ),
    ).toEqual([
      "a INT DEFAULT -1 NOT NULL",
      "b TEXT DEFAULT (|| 'x' 'y')",
      "c INT DEFAULT (= 1 1)",
      "d REAL DEFAULT real(2.5)",
      "e INT DEFAULT NULL",
    ]);
  });

  it("does not let DEFAULT take a NOT NULL constraint as its value", () => {
    expect(syntaxError("CREATE TABLE t (a INT DEFAULT NOT NULL)").message).toBe(
      "expected a default value, got 'NOT'",
    );
  });

  it("does not read non-ASCII look-alikes as keywords", () => {
    expect(columns("CREATE TABLE ıs (ſelect INT)")).toEqual(["ſelect INT"]);
    expect(syntaxError("ſelect 1").message).toBe(
      "expected a statement, got 'ſelect'",
    );
  });

  it("keeps every PRIMARY KEY as written, for the binder to count", () => {
    const table = statementOf(
      "CREATE TABLE t (a INT PRIMARY KEY PRIMARY KEY, b INT, PRIMARY KEY (a, b), PRIMARY KEY (b))",
      StatementType.CREATE_TABLE_STATEMENT,
    );
    expect(table.columns.map((column) => renderColumn(column))).toEqual([
      "a INT PRIMARY KEY PRIMARY KEY",
      "b INT",
    ]);
    expect(table.primaryKeys).toEqual([["a", "b"], ["b"]]);
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
    ["CREATE TABLE t (a INT AS (1))", "a generated column"],
    ["CREATE TABLE t (a INT) WITHOUT ROWID", "WITHOUT ROWID"],
    ["CREATE TABLE t (a INT) STRICT", "STRICT"],
    ['CREATE TABLE t (a "INTEGER")', "a quoted type name"],
    ["CREATE TABLE t AS SELECT 1", "CREATE TABLE … AS"],
    ["CREATE VIEW v AS SELECT 1", "CREATE VIEW"],
    ["CREATE TEMP TABLE t (a INT)", "CREATE TEMP"],
  ])("does not support %s", (sql, feature) => {
    expect(notSupported(sql).feature).toBe(feature);
  });

  it("reads PRIMARY KEY (…) AS as a syntax error, not a generated column", () => {
    expect(
      syntaxError("CREATE TABLE t (a INT, PRIMARY KEY (a) AS)").message,
    ).toBe("expected ')', got 'AS'");
  });
});

describe("other DDL", () => {
  it("reads CREATE INDEX", () => {
    const index = statementOf(
      "CREATE UNIQUE INDEX IF NOT EXISTS i ON t (a, lower(b))",
      StatementType.CREATE_INDEX_STATEMENT,
    );
    expect(index).toMatchObject({
      name: "i",
      table: "t",
      isUnique: true,
      ifNotExists: true,
    });
    expect(index.expressions.map((part) => render(part))).toEqual([
      "a",
      "(lower b)",
    ]);
    expect(notSupported("CREATE INDEX i ON t (a) WHERE a > 0").feature).toBe(
      "a partial index",
    );
    expect(notSupported("CREATE INDEX i ON t (a DESC)").feature).toBe(
      "ASC or DESC in an index",
    );
  });

  it("reads ALTER TABLE", () => {
    const add = statementOf(
      "ALTER TABLE t ADD COLUMN key TEXT DEFAULT ''",
      StatementType.ALTER_TABLE_STATEMENT,
    );
    if (add.alterType !== AlterType.ADD_COLUMN) throw new Error("not ADD");
    expect(renderColumn(add.column)).toBe("key TEXT DEFAULT ''");
    expect(
      statementOf(
        "ALTER TABLE t DROP first",
        StatementType.ALTER_TABLE_STATEMENT,
      ),
    ).toMatchObject({ alterType: AlterType.DROP_COLUMN, columnName: "first" });
    expect(notSupported("ALTER TABLE t RENAME TO u").feature).toBe(
      "ALTER TABLE … RENAME",
    );
  });

  it("always reads COLUMN after ADD and DROP as the keyword", () => {
    const add = statementOf(
      "ALTER TABLE t ADD column INT",
      StatementType.ALTER_TABLE_STATEMENT,
    );
    if (add.alterType !== AlterType.ADD_COLUMN) throw new Error("not ADD");
    expect(renderColumn(add.column)).toBe("INT -");
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
    expect(
      statementOf("DROP TABLE IF EXISTS t", StatementType.DROP_STATEMENT),
    ).toEqual({
      type: StatementType.DROP_STATEMENT,
      dropType: DropType.TABLE,
      name: "t",
      ifExists: true,
    });
    expect(
      statementOf("DROP INDEX if", StatementType.DROP_STATEMENT),
    ).toMatchObject({ dropType: DropType.INDEX, name: "if", ifExists: false });
    expect(notSupported("DROP VIEW v").feature).toBe("DROP VIEW");
  });
});

describe("transactions and EXPLAIN", () => {
  it.each<[string, TransactionStatement]>([
    ["BEGIN", begin("DEFERRED")],
    ["BEGIN TRANSACTION", begin("DEFERRED")],
    ["BEGIN DEFERRED", begin("DEFERRED")],
    ["BEGIN IMMEDIATE TRANSACTION", begin("IMMEDIATE")],
    ["COMMIT", transactionEnd(TransactionType.COMMIT)],
    ["END TRANSACTION", transactionEnd(TransactionType.COMMIT)],
    ["ROLLBACK", transactionEnd(TransactionType.ROLLBACK)],
    ["ROLLBACK TRANSACTION", transactionEnd(TransactionType.ROLLBACK)],
  ])("%s", (sql, expected) => {
    expect(statementOf(sql, StatementType.TRANSACTION_STATEMENT)).toEqual(
      expected,
    );
  });

  it("wraps a statement in EXPLAIN", () => {
    const explain = statementOf(
      "EXPLAIN SELECT 1",
      StatementType.EXPLAIN_STATEMENT,
    );
    expect(explain.statement.type).toBe(StatementType.SELECT_STATEMENT);
    expect(syntaxError("EXPLAIN EXPLAIN SELECT 1").message).toBe(
      "expected a statement to explain, got 'EXPLAIN'",
    );
  });

  it.each([
    ["BEGIN EXCLUSIVE", "BEGIN EXCLUSIVE"],
    ["BEGIN TRANSACTION t1", "a transaction name"],
    ["COMMIT TRANSACTION t1", "a transaction name"],
    ["ROLLBACK TRANSACTION t1", "a transaction name"],
    ["ROLLBACK TRANSACTION TO s", "ROLLBACK TO"],
    ["ROLLBACK TO s", "ROLLBACK TO"],
    ["SAVEPOINT s", "the SAVEPOINT statement"],
    ["PRAGMA table_info(t)", "the PRAGMA statement"],
    ["VACUUM", "the VACUUM statement"],
    ["VALUES (1)", "VALUES as a query"],
    ["EXPLAIN QUERY PLAN SELECT 1", "EXPLAIN QUERY PLAN"],
  ])("does not support %s", (sql, feature) => {
    expect(notSupported(sql).feature).toBe(feature);
  });
});
