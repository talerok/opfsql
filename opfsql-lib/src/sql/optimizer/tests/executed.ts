import { InternalError } from "../../../errors.js";
import { runQuery } from "../../../executor/index.js";
import { compileLower } from "../../../executor/tests/database.js";
import type { Tuple } from "../../../expressions/index.js";
import type { Index, Table } from "../../../schema/index.js";
import {
  type IndexRows,
  Tables as TableAccess,
  type TableRows,
} from "../../../table/index.js";
import { Database } from "../../../table/tests/database.js";
import type { Value } from "../../../values/index.js";
import type { physical } from "../../plan/index.js";
import { schema } from "./builders.js";
import type { Tables } from "./scans.js";

// The test schema (builders.ts) made in a real database over memory files,
// its sample rows written through the table layer, and physical plans run
// by the real executor: the third leg of the differential test, beside the
// reference interpreter's two.

export class SampleDatabase {
  private constructor(private readonly database: Database) {}

  /** A database of the test schema holding `samples`' rows. */
  static async open(samples: Tables): Promise<SampleDatabase> {
    const database = await Database.open(compileLower);
    for (const shape of schema.tables) {
      const { table } = database.change((catalog) =>
        catalog.createTable(shape, []),
      );
      database.tables.push(table);
    }
    for (const shape of schema.indexes) {
      const table = database.schema.table(shape.table);
      if (table === null) throw new InternalError(`no table ${shape.table}`);
      const statement = { ...shape, origin: "statement" } as const;
      const index = database.change((catalog) =>
        catalog.createIndex(statement, table),
      );
      database.indexes.push(index);
    }
    for (const [name, rows] of samples) {
      const table = database.schema.table(name);
      if (table === null) throw new InternalError(`no table ${name}`);
      database.insert(
        table,
        rows.map((row) => [...row.values]),
      );
    }
    return new SampleDatabase(database);
  }

  /** The rows the executor gives for `plan`, run in a read transaction. */
  query(plan: physical.Plan, parameters: readonly Value[]): readonly Tuple[] {
    const statement = {
      kind: "query",
      plan,
      columns: [],
      parameterCount: 0,
    } as const;
    return this.database.reading((pages) => {
      const tables = new ByName(pages, this.database.schema);
      return runQuery(statement, tables, parameters);
    });
  }
}

/**
 * Tables found by name: the test schema's tables and indexes hold made-up
 * root pages, and the database's hold the real ones.
 */
class ByName extends TableAccess {
  override rows(table: Table): TableRows {
    const real = this.schema.table(table.name);
    if (real === null) throw new InternalError(`no table ${table.name}`);
    return super.rows(real);
  }

  override index(index: Index): IndexRows {
    const real = this.schema.index(index.name);
    if (real === null) throw new InternalError(`no index ${index.name}`);
    return super.index(real);
  }
}
