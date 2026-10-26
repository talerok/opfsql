import type {
  Index,
  IndexShape,
  Table,
  TableShape,
} from "../../schema/index.js";
import { constraintIndexName } from "../../schema/index.js";
import type {
  Delete,
  Insert,
  physical,
  Query,
  Update,
} from "../../sql/plan/index.js";
import type {
  PageReader,
  Pager,
  WriteTransaction,
} from "../../storage/index.js";
import { beginWrite, read } from "../../storage/tests/pager-support.js";
import type { CompileExpression } from "../../table/index.js";
import { Database } from "../../table/tests/database.js";
import type { Value } from "../../values/index.js";
import { execute, type Outcome, type Target } from "../index.js";

// A database for executor tests: tables made through the catalog, rows
// through the table layer, and statements run by the executor in read or
// write transactions on a pager over memory files.

export type Write =
  Insert<physical.Plan> | Update<physical.Plan> | Delete<physical.Plan>;

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
    return read(this.database.pager, (pages) =>
      this.rowsOf(plan, pages, parameters),
    );
  }

  /** Runs a write in a write transaction: committed if it succeeds, rolled back if not. */
  run(statement: Write, parameters: readonly Value[] = []): Outcome {
    return attempt(this.database.pager, (pages) =>
      execute(statement, this.target(pages), parameters),
    );
  }

  /** Runs a write in a read transaction, where it cannot write. */
  runReading(statement: Write): Outcome {
    return read(this.database.pager, (pages) =>
      execute(statement, this.target(pages), []),
    );
  }

  private rowsOf(
    plan: physical.Plan,
    pages: PageReader,
    parameters: readonly Value[],
  ): Value[][] {
    const statement: Query<physical.Plan> = {
      kind: "query",
      plan,
      columns: [],
      parameterCount: parameters.length,
    };
    const { rows } = execute(statement, this.target(pages), parameters);
    return rows.map((row) => [...row]);
  }

  private target(pages: PageReader): Target {
    const { schema } = this.database;
    return { pages, schema, compileExpression: compileLower };
  }
}

function attempt<T>(
  pager: Pager,
  change: (transaction: WriteTransaction) => T,
): T {
  const transaction = beginWrite(pager);
  try {
    const result = change(transaction);
    transaction.commit();
    return result;
  } catch (error) {
    transaction.rollback();
    throw error;
  }
}
