import type { Check } from "./check.js";
import { insertSql, type Row } from "./data.js";
import { createIndexSql, createTableSql, type DatabaseSpec } from "./schema.js";

/** Rows listed as INSERT statements in a failure, at most; above, only counted. */
const LISTED_ROWS = 40;

export interface Failure {
  readonly seed: number;
  readonly index: number;
  readonly spec: DatabaseSpec;
  readonly rows: ReadonlyMap<string, readonly Row[]>;
  readonly pageSize: number;
  readonly check: Check;
  readonly reason: string;
  /** Whether the query and rows were reduced from the generated ones. */
  readonly isReduced: boolean;
}

/** Everything needed to see a failure again: by hand, or from its seed. */
export function describeFailure(failure: Failure): string {
  const { seed, index, spec, check } = failure;
  const reduced = failure.isReduced ? ", reduced" : "";
  return [
    `seed ${String(seed)}, case ${String(index)}${reduced}: ${check.oracle} failed`,
    failure.reason,
    ...describeSchema(spec),
    ...describeRows(failure),
    "forms:",
    ...check.forms.map((form) => {
      const parameters =
        form.parameters.length > 0
          ? ` -- ${JSON.stringify(form.parameters)}`
          : "";
      return `  ${form.sql};${parameters}`;
    }),
  ].join("\n");
}

function describeRows(failure: Failure): string[] {
  const { spec, rows } = failure;
  const total = spec.tables.reduce(
    (sum, table) => sum + (rows.get(table.name)?.length ?? 0),
    0,
  );
  const sizes = spec.tables.map(
    (table) =>
      `${table.name}: ${String(rows.get(table.name)?.length ?? 0)} rows`,
  );
  const summary = `data: ${sizes.join(", ")}; page size ${String(failure.pageSize)}`;
  if (total > LISTED_ROWS) return [summary];
  const inserts = spec.tables.flatMap((table) =>
    (rows.get(table.name) ?? []).map((row) => `  ${insertSql(table, row)};`),
  );
  return [summary, ...inserts];
}

/** The tables, and the indexes only the indexed copy has. */
export function describeSchema(spec: DatabaseSpec): string[] {
  return [
    "schema:",
    ...spec.tables.map((table) => `  ${createTableSql(table)};`),
    "indexed copy only:",
    ...spec.indexes.map(
      (each) =>
        `  ${createIndexSql(each)};${each.isEarly ? " -- before the rows" : ""}`,
    ),
  ];
}
