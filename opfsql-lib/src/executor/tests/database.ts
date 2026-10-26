import type {
  Index,
  IndexShape,
  Table,
  TableShape,
} from "../../schema/index.js";
import { constraintIndexName } from "../../schema/index.js";
import type { physical, Query } from "../../sql/plan/index.js";
import {
  type CompileExpression,
  IndexKeys,
  Tables,
  WritableTables,
} from "../../table/index.js";
import { Database } from "../../table/tests/database.js";
import type { Value } from "../../values/index.js";
import {
  runQuery,
  runWrite,
  type WriteOutcome,
  type WriteStatement,
} from "../index.js";

// A database for executor tests: tables made through the catalog, rows
// through the table layer, and statements run by the executor in read or
// write transactions on a pager over memory files.

/** lower(name), the one key expression here, for a row whose column 1 is the name. */
export const compileLower: CompileExpression = (sql) => {
  if (sql !== "lower(name)") throw new Error(`no expression ${sql}`);
  return lowerName;
};

function lowerName(row: readonly Value[]): Value {
  const name = row[1];
  return typeof name === "string" ? name.toLowerCase() : null;
}

export class TestDatabase {
  private constructor(private readonly database: Database) {}

  static async open(): Promise<TestDatabase> {
    return new TestDatabase(await Database.open(compileLower));
  }

  /** Creates a table, its constraint indexes, and opfsql_sequence if it needs one. */
  createTable(shape: TableShape, constraints: IndexShape[] = []): Table {
    const { table, indexes } = this.database.change((catalog) =>
      catalog.createTable(shape, constraints),
    );
    this.database.tables.push(table);
    this.database.indexes.push(...indexes);
    if (shape.isAutoincrement) this.database.findSequences();
    return table;
  }

  createIndex(shape: IndexShape, table: Table): Index {
    const index = this.database.change((catalog) =>
      catalog.createIndex(shape, table),
    );
    this.database.indexes.push(index);
    return index;
  }

  /** The index the catalog made for `table`'s constraint number `ordinal`. */
  constraintIndex(table: Table, ordinal: number): Index {
    const name = constraintIndexName(table.name, ordinal);
    const index = this.database.schema.index(name);
    if (index === null) throw new Error(`no index ${name}`);
    return index;
  }

  insert(table: Table, rows: Value[][]): void {
    this.database.insert(table, rows);
  }

  rows(table: Table): Value[][] {
    return this.database.rows(table);
  }

  /** What is wrong with the pages, or with index entries against rows. */
  problems(): string[] {
    return [...this.database.problems(), ...this.database.tableProblems()];
  }

  /** Runs a query plan in a read transaction. */
  query(plan: physical.Plan, parameters: readonly Value[] = []): Value[][] {
    const statement: Query<physical.Plan> = {
      kind: "query",
      plan,
      columns: [],
      parameterCount: parameters.length,
    };
    return this.database.reading((pages) => {
      const tables = new Tables(pages, this.database.schema);
      return runQuery(statement, tables, parameters).map((row) => [...row]);
    });
  }

  /** Runs a write in a write transaction: committed if it succeeds, rolled back if not. */
  run(
    statement: WriteStatement,
    parameters: readonly Value[] = [],
  ): WriteOutcome {
    const keys = new IndexKeys(this.database.schema, compileLower);
    return this.database.writing((pages) =>
      runWrite(statement, new WritableTables(pages, keys), parameters),
    );
  }
}
